#!/usr/bin/env bash
# Tags: no-fasttest
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# ## reading ArrowStream file from python
# import pyarrow as pa
# stream = pa.ipc.open_stream("test.arrows")
# x = stream.read_all()
# print(x)

## writing ArrowStream file from python
# import pyarrow as pa
#data = [
#    pa.array([1, 2, 3, 4, 5]),
#    pa.array(["onee", "twoo", "three", "four", "five"]).dictionary_encode(),
#    pa.array([1, 2, 3, 4, 5]).dictionary_encode(),
#    pa.array([True, False, True, True, True])
#]
# batch = pa.record_batch(data, names=['id', 'lc_nullable', 'lc_int_nullable', 'bool_nullable'])
# writer = pa.ipc.new_stream("test4.arrows", batch.schema)
# writer.write_batch(batch)
# writer.close()

# cat data.arrow | gzip | base64

cat <<EOF | base64 --decode | gunzip | $CLICKHOUSE_LOCAL --query='SELECT * FROM table FORMAT TSVWithNamesAndTypes' --input-format=ArrowStream
H4sIAAAAAAAAA5VTsU7DQAz1pZc2giBaFFAGkBgYMjIyMOQD+gHdCqWpiBQlqErhZzowMvIB/Ft4
d+drrqGFYsny+fzO9rN1TdM0L4JoSEqOKKQ++RTgBBGSJEwEjLL6DOwn7B37IWIA9tX7a75TcgKd
VVUxLVdF8TgrMvgpsGuD9yL4Y2jivDmFFk/TvKzbVwFFUAk1PQpqZaJzkVB153xONS4Gvk8DsBni
veEmfFVTxW+cmsemplMv0NGAMV9ODUlmHkPdk8mvPM7vKXvp5Pag+ZyADaEDndP2iLTNh5onY0Oc
zORDnZU8qWO3HDcbaeegdhUDKTky5nvfmU+P9kvcsedOTHTyWJG6D7PbEb+pyiyr36qqfl5m2aJa
LRf5a8b83g/gl2z4nW32HJO7522e9zt4er/wTJzzLl62js1hZ2Z3aPGKTyxcPhfbfHpS9/2wp+/1
jr6DA/pO9tzbPtJOPO3EJ5249d1/JOnnXP7rHzpHi/UYI/+4v2LbmH9I36C0faSwBAAA
EOF