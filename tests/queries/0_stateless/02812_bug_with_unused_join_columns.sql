SELECT concat(func.name, comb.name) AS x FROM system.functions AS func JOIN system.aggregate_function_combinators AS comb using name WHERE is_aggregate settings allow_experimental_analyzer=1;
