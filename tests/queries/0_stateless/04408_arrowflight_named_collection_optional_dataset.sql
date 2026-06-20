-- Tags: no-fasttest, no-replicated-database

-- A named collection that omits the optional `dataset` key must be accepted by ArrowFlight.
-- It used to throw BAD_ARGUMENTS ("No such key 'dataset'") because processNamedCollectionResult
-- overwrote the safe getOrDefault read with a bare get<String>("dataset"). With `dataset` omitted
-- (and basic auth disabled) the table function now reaches the connection stage and fails with
-- ARROWFLIGHT_FETCH_SCHEMA_ERROR (no server is listening), exactly as it would with `dataset` set —
-- proving the key is optional rather than failing earlier with a "No such key" error.
DROP NAMED COLLECTION IF EXISTS arrowflight_04408_no_dataset;
CREATE NAMED COLLECTION arrowflight_04408_no_dataset AS host = 'localhost', port = 56789, use_basic_authentication = false;
SELECT * FROM arrowFlight(arrowflight_04408_no_dataset); -- { serverError ARROWFLIGHT_FETCH_SCHEMA_ERROR }
DROP NAMED COLLECTION arrowflight_04408_no_dataset;
