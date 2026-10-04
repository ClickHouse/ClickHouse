KILL QUERY WHERE query_id = '05233_no_such_query' SETTINGS kill_throw_if_noop = 1; -- { serverError NOTHING_TO_KILL }
KILL QUERY WHERE query_id = '05233_no_such_query' SETTINGS kill_throw_if_noop = 0;
KILL MUTATION WHERE database = currentDatabase() AND mutation_id = 'mutation_05233_no_such.txt' SETTINGS kill_throw_if_noop = 1; -- { serverError NOTHING_TO_KILL }
KILL MUTATION WHERE database = currentDatabase() AND mutation_id = 'mutation_05233_no_such.txt' SETTINGS kill_throw_if_noop = 0;
SELECT 'OK';
