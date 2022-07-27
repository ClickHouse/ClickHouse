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
#     pa.array([1, 2, 3, Null, 5]),
#     pa.array(["one", "two", "three", Null, "five"]).dictionary_encode(),
#     pa.array([1, 2, 3, 4, 5]).dictionary_encode(),
#     pa.array([True, False, True, True, True])
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
H4sIAAAAAAAAA31RQQ6CMBAcSkViauLBGA8ePPoMDz7DxGDkRkyMevMxPsAHePAh/qbOskVBwSXD
su3s7LR47/0TwAgSAzgk6CHlFyOysEw3ciQb4kKMS/6UrAR9doy4ov1RT/ZWxIKw0BgS+3NRZNsi
3+yyU8Y65SOMNPAenCECWutc0a1mSBVmGMmzmn5MHPMDuY7ol5pCXPJFWX8NniUcfesJXakKY7mC
qqcMPb16+9yLrsjOMsyO0R2Vt7RlL/rTJ37vDb+Tpt9Y/S7eHQPW7u1tHTzXvVl0x/QrV97mIdd1
DH7PHLfw6jqmY67X/40X3pg6JYACAAA=
EOF