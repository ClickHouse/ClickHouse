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
#     pa.array(["one", None, "three", "", None, "", "six"]).dictionary_encode(),
# ]
# batch = pa.record_batch(data, names=['id', 'lc_nullable', 'lc_int_nullable', 'bool_nullable'])
# writer = pa.ipc.new_stream("test4.arrows", batch.schema)
# writer.write_batch(batch)
# writer.close()

# cat data.arrow | gzip | base64

cat <<EOF | base64 --decode | gunzip | $CLICKHOUSE_LOCAL --query='SELECT * FROM table FORMAT TSVWithNamesAndTypes' --input-format=ArrowStream
H4sIAAAAAAAAA3VQQQ6CQAychRWIcjCGGA4ePHrwCT7Bgz8waoiSICaoiU/wGR49+Me1XboIJDYp
3d2ZTjsYY8wLwBgcQ8QIMEBEJwqloal8iMNVUSaWmxIjQEjsMb3UvWrA2IZySalRx4SyOGzLe1Hs
9kW2vd6qvDyC+iPL4m8MvseUob2z2NyiutGhFcxb5sP2JLJpbPvhaYstBK8d1PrOW0pMLcha3p0+
h4//4eamUkctTPV02nqRpONfyux2qrLsmj8aX8+Or2nXl6/tzEWj2vWxEh9ha67X2w2yA8esh7k+
13PueVAtzMPvH/HebPkLlbsntUACAAA=
EOF