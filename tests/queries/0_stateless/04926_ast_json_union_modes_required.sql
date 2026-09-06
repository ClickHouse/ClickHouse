-- A non-normalized union needs one mode for every separator. Omitting the field used to
-- bypass the cardinality check and silently discard every select but the first during
-- query normalization.
SELECT position(parseQueryToJSON($$SELECT 1 UNION ALL SELECT 2$$), '"list_of_modes":["UNION_ALL"]') > 0;

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$SELECT 1 UNION ALL SELECT 2$$),
    '"list_of_modes":["UNION_ALL"],',
    '')); -- { serverError BAD_ARGUMENTS }
