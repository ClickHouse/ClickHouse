#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="
    insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet', Parquet, 'point Point, linestring LineString, polygon Polygon, multilinestring MultiLineString, multipolygon MultiPolygon') values (
            (10, 20),
            [(0, 0), (10, 0), (10, 10), (0, 10)],
            [[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]],
            [[(0, 0), (10, 0), (10, 10), (0, 10)], [(1, 1), (2, 2), (3, 3)]],
            [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]
        ), (
            (30, 40),
            [(1, 1), (11, 1), (11, 11), (1, 11)],
            [[(21, 21), (51, 21), (51, 51), (21, 51)], [(31, 31), (51, 51), (51, 31)]],
            [[(1, 1), (11, 1), (11, 11), (1, 11)], [(2, 2), (3, 3), (4, 4)]],
            [[[(1, 1), (11, 1), (11, 11), (1, 11)]], [[(21, 21), (51, 21), (51, 51), (21, 51)],[(31, 31), (51, 51), (51, 31)]]]
        );
    select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet');"

rm "${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
