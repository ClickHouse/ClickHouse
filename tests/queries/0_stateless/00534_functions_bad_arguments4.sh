#!/usr/bin/env bash
# shellcheck disable=SC2016

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./00534_functions_bad_arguments.lib
. "$CURDIR"/00534_functions_bad_arguments.lib

test_variant 'SELECT $_([]);'
