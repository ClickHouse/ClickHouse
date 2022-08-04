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
# data = [
#     pa.array(["one", None, "three", "four", "five"]).dictionary_encode(),
# ]
# batch = pa.record_batch(data, names=['id', 'lc_nullable', 'lc_int_nullable', 'bool_nullable'])
# writer = pa.ipc.new_stream("test4.arrows", batch.schema)
# writer.write_batch(batch)
# writer.close()

# cat data.arrow | gzip | base64

# https://github.com/ClickHouse/ClickHouse/pull/24341
# set output_format_arrow_low_cardinality_as_dictionary=1;
# SELECT *
# FROM values('id UInt64, s LowCardinality(String)', (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'))
# INTO OUTFILE 'test5.arrows'
# FORMAT ArrowStream

cat <<EOF | base64 --decode | gunzip | $CLICKHOUSE_LOCAL --query='SELECT * FROM table FORMAT TSVWithNamesAndTypes' --input-format=ArrowStream
H4sIAAAAAAAAA3VQQQrCMBCc2FSL9FCkSC+CRw8+wSd48AeikqpQKtTWN/gMjx78Y9xNN9IWXNhu
kpmd3am11r4AJOCYIsYYISI6USgNTeVDHK6KMnXcjBhjTIid0Evbq0LGdpRrSo02ZpTFaV82RXE4
FmZ/r6treQb1R47F3xh8jykn7s5iS4fqnw6tYN8yH64nlU1j14+RdthK8NZBq++9ZcTUgmzl3etz
BPgffm4mNelgaqDT1fP+PP9WmvpSGZPfmiq/Pgz7evZ8zfu+Au1mLn+qfR8b0Q47c0eD3SB7dH14
LBzwFgMPSvT8v+F92eoXUa2r5jgCAAA=
EOF