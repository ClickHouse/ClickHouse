#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/00534_functions_bad_arguments.lib

test_variant 'SELECT $_([NULL], [NULL], [NULL]);'
