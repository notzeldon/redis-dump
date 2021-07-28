import json
import redis
import sys
import csv


def redis_dump(host, port, output):
    r = redis.Redis(host, int(port))

    with open(output, 'w', encoding='utf-8') as out_file:
        columns = ['key', 'value', 'type']

        writer_out = csv.DictWriter(out_file, fieldnames=columns, delimiter='\t')
        writer_out.writeheader()

        keys = r.execute_command('KEYS *')
        for key in keys:
            tmp_key = key.decode().strip()
            rtype = r.type(tmp_key).decode()
            if rtype == 'list':
                value = []
                for x in range(r.llen(tmp_key)):
                    value.append(r.lindex(tmp_key, x).decode())
                writer_out.writerow({'key': tmp_key, 'value': json.dumps(value), 'type': rtype})
            elif rtype == 'hash':
                tmp_value = r.hgetall(tmp_key)
                value = dict()
                for k, v in tmp_value.items():
                    value[k.decode()] = v.decode()
                writer_out.writerow({'key': tmp_key, 'value': json.dumps(value), 'type': rtype})
            elif rtype == 'string':
                value = r.get(tmp_key).decode()
                writer_out.writerow({'key': tmp_key, 'value': value, 'type': rtype})
            else:
                writer_out.writerow({'key': tmp_key, 'value': '[error]', 'type': rtype})



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    args = sys.argv[1:]
    redis_dump(*args)
