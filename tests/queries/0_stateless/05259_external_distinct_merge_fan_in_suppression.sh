#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-merge-fan-in.XXXXXX")
export LOCAL_DIR
trap 'rm -rf "${LOCAL_DIR}"' EXIT

PYTHONPATH="$CUR_DIR/helpers" python3 - <<'PY'
import re

from external_merge_fan_in import check


# A larger threshold allows hashing to emit keys before spilling. Suppression files retain these keys
# through intermediate merges so the final merge does not emit them again.
_, log = check('distinct_suppression', '''
    SELECT count(), uniqExact(k) FROM
    (SELECT DISTINCT concat(repeat('x', 128), toString(number % 262144)) AS k FROM numbers(1048576))
    ''', '262144\t262144', extra=('--max_bytes_before_external_distinct=67108864',))
assert re.search(r'Extracting [1-9]\d* DISTINCT suppression keys', log), log
PY
