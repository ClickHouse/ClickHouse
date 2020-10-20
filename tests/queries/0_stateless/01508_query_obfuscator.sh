#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --seed Hello --obfuscate <<< "SELECT 123, 'Test://2020-01-01hello1234 at 2000-01-01T01:02:03', 12e100, Gibberish_id_testCool, hello(World), avgIf(remote('127.0.0.1'))"
$CLICKHOUSE_FORMAT --seed Hello --obfuscate <<< "SELECT cost_first_screen between a and b, case when x >= 123 then y else null end"

$CLICKHOUSE_FORMAT --seed Hello --obfuscate <<< "
SELECT
    VisitID,
    Goals.ID, Goals.EventTime,
    WatchIDs,
    EAction.ProductName, EAction.ProductPrice, EAction.ProductCurrency, EAction.ProductQuantity, EAction.EventTime, EAction.Type
FROM merge.visits_v2
WHERE
    StartDate >= '2020-09-17' AND StartDate <= '2020-09-25'
    AND CounterID = 24226447
    AND intHash32(UserID) = 416638616 AND intHash64(UserID) = 13269091395366875299
    AND VisitID IN (5653048135597886819, 5556254872710352304, 5516214175671455313, 5476714937521999313, 5464051549483503043)
    AND Sign = 1
"
