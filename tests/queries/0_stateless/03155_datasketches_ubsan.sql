-- Tags: no-fasttest
SELECT uniqTheta(toFixedString('uniqTheta distinct', 18)) FROM (SELECT number % 2 AS x FROM numbers(10) WHERE materialize(16));
