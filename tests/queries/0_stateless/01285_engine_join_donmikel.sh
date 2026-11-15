#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS NmSubj;
DROP TABLE IF EXISTS events;

create table NmSubj
(
    NmId      UInt32,
    SubjectId UInt32
)
    engine = Join(All, inner, NmId);

create table events
(
    EventDate       Date,
    EventDateTime   DateTime,
    EventId         String,
    SessionId       FixedString(36),
    PageViewId      FixedString(36),
    UserId          UInt64,
    UniqUserId      FixedString(36),
    UrlReferrer     String,
    Param1          String,
    Param2          String,
    Param3          String,
    Param4          String,
    Param5          String,
    Param6          String,
    Param7          String,
    Param8          String,
    Param9          String,
    Param10         String,
    ApplicationType UInt8,
    Locale          String,
    Lang            String,
    Version         String,
    Path            String,
    QueryString     String,
    UserHostAddress UInt32
)
    engine = MergeTree()
        PARTITION BY (toYYYYMM(EventDate), EventId)
        ORDER BY (EventId, EventDate, Locale, ApplicationType, intHash64(UserId))
        SAMPLE BY intHash64(UserId)
        SETTINGS index_granularity = 8192;

insert into NmSubj values (1, 1), (2, 2), (3, 3);
"

$CLICKHOUSE_CLIENT --query "INSERT INTO events FORMAT TSV" < "${CURDIR}"/01285_engine_join_donmikel.tsv

$CLICKHOUSE_CLIENT --query "
SELECT toInt32(count() / 24) as Count
FROM events as e INNER JOIN NmSubj as ns
ON ns.NmId = toUInt32(e.Param1)
WHERE e.EventDate = today() - 7 AND e.EventId = 'GCO' AND ns.SubjectId = 2073"

$CLICKHOUSE_CLIENT --query "
DROP TABLE NmSubj;
DROP TABLE events;
"
