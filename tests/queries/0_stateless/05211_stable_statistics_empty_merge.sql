-- Merging an empty state with a state containing large finite values must not evaluate Inf * 0.
SELECT
    varPopStableMerge(var_state),
    stddevPopStableMerge(stddev_state),
    varSampStableMerge(var_samp_state),
    stddevSampStableMerge(stddev_samp_state),
    covarPopStableMerge(covar_state),
    stddevPopStableForEachMerge(stddev_foreach_state)
FROM
(
    SELECT
        varPopStableState(x) AS var_state,
        stddevPopStableState(x) AS stddev_state,
        varSampStableState(x) AS var_samp_state,
        stddevSampStableState(x) AS stddev_samp_state,
        covarPopStableState(x, x) AS covar_state,
        stddevPopStableForEachState([toNullable(x)]) AS stddev_foreach_state
    FROM (SELECT materialize(1e155) AS x FROM numbers(2))
);
