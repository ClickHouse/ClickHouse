#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reproduce GZDATA:
#
# ```python
# import pyarrow as pa
# data = [pa.array([1]), pa.array([2]), pa.array([3])]
# batch = pa.record_batch(data, names=['x', 'y', 'x'])
# with pa.ipc.new_file('data.arrow', batch.schema) as writer:
#     writer.write_batch(batch)
# ```
#
# ```bash
# cat data.arrow | gzip | base64
# ```

GZDATA="H4sIAHTzuWEAA9VTuw3CMBB9+RCsyIULhFIwAC0SJQWZACkNi1CAxCCMwCCMQMEIKdkgPJ8PJbIIEiVPujuf73yfp6Rumt1+BXTEA4CDRwmLAhMYnogkpw96hjpXDWSUA2Wt/pU1mJz6GjO9k+eUI+UicSRbqvuX3BPlNsh1zDCcZypTOJ0xvF186GOYZ5ht9NrX8Pu12svDYq4bWqmKLEdFU+GNkmcr23oOzspNgh4FxmEiO3bvoriL4jJa1Bc/+OmghkcXeJU+lmwUwoALHHDbDfUSgVNfo9V3T7U9Pz3++bswDNbyD7wAxr434AoDAAA="

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t1"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t1 ( x Int64, y Int64, z Int64 ) ENGINE = Memory"

echo ${GZDATA} | base64 --decode | gunzip | ${CLICKHOUSE_CLIENT} -q "INSERT INTO t1 settings input_format_arrow_allow_missing_columns = true FORMAT Arrow" 2>&1 | grep -qF "DUPLICATE_COLUMN" && echo 'OK' || echo 'FAIL' ||:

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t1"
