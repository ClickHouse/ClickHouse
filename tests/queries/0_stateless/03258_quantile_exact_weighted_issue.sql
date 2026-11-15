SELECT toTypeName(quantilesExactWeightedState(0.2, 0.4, 0.6, 0.8)(number + 1, 1) AS x) FROM numbers(49999);
SELECT toTypeName(quantilesExactWeightedInterpolatedState(0.2, 0.4, 0.6, 0.8)(number + 1, 1) AS x) FROM numbers(49999);
