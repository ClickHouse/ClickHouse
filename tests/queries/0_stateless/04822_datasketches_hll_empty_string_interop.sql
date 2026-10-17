-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- `serializedHLL` must follow the Apache DataSketches string contract:
-- `hll_sketch::update(const std::string &)` ignores empty strings, so a sketch built
-- over empty strings only must stay empty instead of counting one distinct element.

SELECT 'empty strings leave the sketch empty';
SELECT serializedHLL(x) = '', cardinalityFromHLL(serializedHLL(x)) FROM (SELECT '' AS x FROM numbers(5));

SELECT 'empty strings do not contribute to cardinality';
SELECT cardinalityFromHLL(serializedHLL(if(number % 2 = 0, '', concat('v', toString(number))))) FROM numbers(10);

SELECT 'sketch over mixed values equals sketch over the non-empty values only';
WITH
    (SELECT serializedHLL(arrayJoin(['', 'a', 'b', '']))) AS with_empty,
    (SELECT serializedHLL(arrayJoin(['a', 'b']))) AS without_empty
SELECT with_empty = without_empty;

SELECT 'uniqHLL follows the same contract';
SELECT uniqHLL(x) FROM (SELECT '' AS x FROM numbers(5));
