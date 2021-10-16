-- at least this query should be present
SELECT count() > 0 FROM system.stack_trace WHERE query_id != '';
