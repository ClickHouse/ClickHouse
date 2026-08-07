-- Tags: no-fasttest, use-vectorscan

DROP TABLE IF EXISTS t;

-- test that the check which rejects hyperscan regexes with too big bounded repeats works

-- {n}
SELECT multiMatchAny('test', ['.{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{   51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51   }']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['prefix.{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51}.suffix']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{4,4}midfix{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

-- {n,}
SELECT multiMatchAny('test', ['.{51,}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{   51,}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51   ,}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51,   }']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['prefix.{51,}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51,}.suffix']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{4,4}midfix{51,}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

-- {n,m}
SELECT multiMatchAny('test', ['.{1,51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51,52}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{   51,52}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51   ,52}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51,   52}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{51,52   }']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['prefix.{1,51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{1,51}.suffix']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny('test', ['.{4,4}midfix{1,51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

-- test that the check is implemented in all functions which use vectorscan

CREATE TABLE t(c String) Engine=MergeTree() ORDER BY c;
INSERT INTO t VALUES('Hallo Welt');

SELECT multiMatchAny('Hallo Welt', ['.{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAny(c, ['.{51}']) FROM t; -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

SELECT multiMatchAnyIndex('Hallo Welt', ['.{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAnyIndex(c, ['.{51}']) FROM t; -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

SELECT multiMatchAllIndices('Hallo Welt', ['.{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiMatchAllIndices(c, ['.{51}']) FROM t; -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

SELECT multiFuzzyMatchAny('Hallo Welt', 1, ['.{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiFuzzyMatchAny(c, 1, ['.{51}']) FROM t; -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

SELECT multiFuzzyMatchAnyIndex('Hallo Welt', 1, ['.{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiFuzzyMatchAnyIndex(c, 1, ['.{51}']) FROM t; -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

SELECT multiFuzzyMatchAllIndices('Hallo Welt', 1, ['.{51}']); -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }
SELECT multiFuzzyMatchAllIndices(c, 1, ['.{51}']) FROM t; -- { serverError HYPERSCAN_CANNOT_SCAN_TEXT }

DROP TABLE t;
