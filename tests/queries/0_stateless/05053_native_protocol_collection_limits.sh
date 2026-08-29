#!/usr/bin/env bash
# Tags: no-fasttest

# NOTE: this sh wrapper is required because of shell_config

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 "$CUR_DIR"/05053_native_protocol_collection_limits.python
