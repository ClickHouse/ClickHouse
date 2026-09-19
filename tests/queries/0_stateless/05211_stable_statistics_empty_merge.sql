-- Merging an empty state with a state containing large finite values must not evaluate Inf * 0.
WITH
    (SELECT varPopStableState(x) FROM (SELECT materialize(1e155) AS x FROM numbers(2))) AS var_pop_state,
    (SELECT varPopStableState(x) FROM (SELECT materialize(1e155) AS x FROM numbers(0))) AS var_pop_empty_state,
    (SELECT stddevPopStableState(x) FROM (SELECT materialize(1e155) AS x FROM numbers(2))) AS stddev_pop_state,
    (SELECT stddevPopStableState(x) FROM (SELECT materialize(1e155) AS x FROM numbers(0))) AS stddev_pop_empty_state,
    (SELECT varSampStableState(x) FROM (SELECT materialize(1e155) AS x FROM numbers(2))) AS var_samp_state,
    (SELECT varSampStableState(x) FROM (SELECT materialize(1e155) AS x FROM numbers(0))) AS var_samp_empty_state,
    (SELECT stddevSampStableState(x) FROM (SELECT materialize(1e155) AS x FROM numbers(2))) AS stddev_samp_state,
    (SELECT stddevSampStableState(x) FROM (SELECT materialize(1e155) AS x FROM numbers(0))) AS stddev_samp_empty_state,
    (SELECT covarPopStableState(x, x) FROM (SELECT materialize(1e155) AS x FROM numbers(2))) AS covar_state,
    (SELECT covarPopStableState(x, x) FROM (SELECT materialize(1e155) AS x FROM numbers(0))) AS covar_empty_state,
    (SELECT stddevPopStableForEachState([toNullable(x)]) FROM (SELECT materialize(1e155) AS x FROM numbers(2))) AS stddev_foreach_state,
    (SELECT stddevPopStableForEachState([toNullable(x)]) FROM (SELECT materialize(1e155) AS x FROM numbers(0))) AS stddev_foreach_empty_state,
    (SELECT corrStableState(x, y)
     FROM
     (
         SELECT
             1e155 * (1 + number * 1e-14) AS x,
             number + 1 AS y
         FROM numbers(2)
     )) AS corr_state,
    (SELECT corrStableState(x, y)
     FROM
     (
         SELECT
             1e155 * (1 + number * 1e-14) AS x,
             number + 1 AS y
         FROM numbers(0)
     )) AS corr_empty_state
SELECT
    arrayReduce('varPopStableMerge', [var_pop_state, var_pop_empty_state]),
    arrayReduce('varPopStableMerge', [var_pop_empty_state, var_pop_state]),
    arrayReduce('stddevPopStableMerge', [stddev_pop_state, stddev_pop_empty_state]),
    arrayReduce('stddevPopStableMerge', [stddev_pop_empty_state, stddev_pop_state]),
    arrayReduce('varSampStableMerge', [var_samp_state, var_samp_empty_state]),
    arrayReduce('varSampStableMerge', [var_samp_empty_state, var_samp_state]),
    arrayReduce('stddevSampStableMerge', [stddev_samp_state, stddev_samp_empty_state]),
    arrayReduce('stddevSampStableMerge', [stddev_samp_empty_state, stddev_samp_state]),
    arrayReduce('covarPopStableMerge', [covar_state, covar_empty_state]),
    arrayReduce('covarPopStableMerge', [covar_empty_state, covar_state]),
    arrayReduce('stddevPopStableForEachMerge', [stddev_foreach_state, stddev_foreach_empty_state]),
    arrayReduce('stddevPopStableForEachMerge', [stddev_foreach_empty_state, stddev_foreach_state]),
    round(arrayReduce('corrStableMerge', [corr_state, corr_empty_state]), 6),
    round(arrayReduce('corrStableMerge', [corr_empty_state, corr_state]), 6);
