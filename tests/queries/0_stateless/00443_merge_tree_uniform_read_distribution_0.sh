#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

bash "$CURDIR"/00443_optimize_final_vertical_merge.sh
