-- Tags: no-ordinary-database
CREATE OR REPLACE TABLE alias10__fuzz_13 (`Id` Array(Array(UInt256)), `EventDate` Array(String), `field1` Array(Array(Nullable(Int8))), `field2` Array(Date), `field3` Array(Array(Array(UInt128)))) ENGINE = Distributed(test_shard_localhost, currentDatabase(), alias_local10);

set allow_deprecated_syntax_for_merge_tree=1;
CREATE OR REPLACE TABLE alias_local10 (
  Id Int8,
  EventDate Date DEFAULT '2000-01-01',
  field1 Int8,
  field2 String,
  field3 ALIAS CASE WHEN field1 = 1 THEN field2 ELSE '0' END
) ENGINE = MergeTree(EventDate, (Id, EventDate), 8192);

SET prefer_localhost_replica = 0;

SELECT field1 FROM alias10__fuzz_13 WHERE arrayEnumerateDense(NULL, tuple('0.2147483646'), NULL) GROUP BY field1, arrayEnumerateDense(('0.02', '0.1', '0'), NULL) WITH TOTALS; -- { serverError TYPE_MISMATCH }


CREATE OR REPLACE TABLE local (x Int8) ENGINE = Memory;
CREATE OR REPLACE TABLE distributed (x Array(Int8)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), local);
SET prefer_localhost_replica = 0;
SELECT x FROM distributed GROUP BY x WITH TOTALS; -- { serverError TYPE_MISMATCH }

DROP TABLE distributed;
DROP TABLE local;
DROP TABLE alias_local10;
DROP TABLE alias10__fuzz_13;
