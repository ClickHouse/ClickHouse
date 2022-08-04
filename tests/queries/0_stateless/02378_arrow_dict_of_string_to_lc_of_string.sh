#!/usr/bin/env bash
# Tags: no-fasttest
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

## writing ArrowStream file from python
# import pyarrow as pa
# data = [
#     pa.array(["one", "two", "three", "four", "five"]).dictionary_encode(),
# ]
# batch = pa.record_batch(data, names=['lc_string'])
# writer = pa.ipc.new_stream("test4.arrows", batch.schema)
# writer.write_batch(batch)
# writer.close()

# cat data.arrow | gzip | base64

cat <<EOF | base64 --decode | gunzip | $CLICKHOUSE_LOCAL --query='SELECT * FROM table FORMAT TSVWithNamesAndTypes' --input-format=ArrowStream
H4sIAAAAAAAAA31RWw6CMBDcQnmoJJJIDJ9+eAAP4AH88A5+GFASAwmiXsEjeAQP4P3q9CEWIi4Z
tu3sznZSIYR4EFFMMsYUkU8ehVghGCeO9EKNzAxIVG2KCp8CVMc40b3Mk9wGWAKcdIyA0353buqi
PGAd4pOs/EdqHwGB2kuRhWJ524/R4mnmkupJzA0j1U8OV9zK8PrmWv/jKUUlN8zWnHv0DZeGI+3l
mcWxno6t5wMTYGp6qjJrblVzrLMsry51Xlyz1t+942/e9edyNTtt1bt+1j/8OH/8JNZ66P7MaEgf
9jtI2TfNROSXMAIAAA==
EOF