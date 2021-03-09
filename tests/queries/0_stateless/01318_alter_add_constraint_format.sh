#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_FORMAT --oneline <<<"ALTER TABLE replicated_constraints1 ADD CONSTRAINT IF NOT EXISTS b_constraint CHECK b > 10"
