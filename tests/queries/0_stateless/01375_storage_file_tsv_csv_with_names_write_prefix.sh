#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# zero rows
echo 'zero rows'
for format in TSVWithNames TSVWithNamesAndTypes CSVWithNames CSVWithNamesAndTypes JSONCompactEachRowWithNames JSONCompactEachRowWithNamesAndTypes JSONCompactStringsEachRow JSONCompactStringsEachRowWithNamesAndTypes; do
    echo $format
    ${CLICKHOUSE_LOCAL} --query="
        CREATE TABLE ${format}_01375 ENGINE File($format, '01375_$format') AS SELECT * FROM numbers(1) WHERE number < 0;
        SELECT * FROM ${format}_01375;
        DROP TABLE ${format}_01375;
    "
    rm 01375_$format
done

# run multiple times to the same file
echo 'multi clickhouse-local one file'
for format in TSVWithNames TSVWithNamesAndTypes CSVWithNames CSVWithNamesAndTypes JSONCompactEachRowWithNames JSONCompactEachRowWithNamesAndTypes JSONCompactStringsEachRow JSONCompactStringsEachRowWithNamesAndTypes; do
    echo $format
    for _ in {1..2}; do
        ${CLICKHOUSE_LOCAL} --query="
            CREATE TABLE ${format}_01375 ENGINE File($format, '01375_$format') AS SELECT * FROM numbers(1);
            SELECT * FROM ${format}_01375;
            DROP TABLE ${format}_01375;
        "
    done
    rm 01375_$format
done
