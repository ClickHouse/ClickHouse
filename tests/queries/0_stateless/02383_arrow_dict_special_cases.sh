#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UNIQ_DEST_PATH=$USER_FILES_PATH/test-02383-$RANDOM-$RANDOM
mkdir -p $UNIQ_DEST_PATH

cp $CURDIR/data_arrow/dictionary*.arrow $UNIQ_DEST_PATH/
cp $CURDIR/data_arrow/corrupted.arrow $UNIQ_DEST_PATH/
cp $CURDIR/data_arrow/dict_with_nulls.arrow $UNIQ_DEST_PATH/

$CLICKHOUSE_LOCAL -q "
    desc file('$UNIQ_DEST_PATH/dictionary1.arrow');
    select * from file('$UNIQ_DEST_PATH/dictionary1.arrow') settings max_threads=1;
    desc file('$UNIQ_DEST_PATH/dictionary2.arrow');
    select * from file('$UNIQ_DEST_PATH/dictionary2.arrow') settings max_threads=1;
    desc file('$UNIQ_DEST_PATH/dictionary3.arrow');
    select * from file('$UNIQ_DEST_PATH/dictionary3.arrow') settings max_threads=1;

    desc file('$UNIQ_DEST_PATH/corrupted.arrow');
    select * from file('$UNIQ_DEST_PATH/corrupted.arrow'); -- {serverError INCORRECT_DATA}

    desc file('$UNIQ_DEST_PATH/dict_with_nulls.arrow');
    select * from file('$UNIQ_DEST_PATH/dict_with_nulls.arrow') settings max_threads=1;
"

rm -rf $UNIQ_DEST_PATH
