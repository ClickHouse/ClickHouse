SELECT formatQueryFromJSON(parseQueryToJSON(
    $$INSERT INTO t FROM INFILE 'input.csv' RETURNING (SELECT 1) FORMAT CSV$$))
    = formatQuerySingleLine(
        $$INSERT INTO t FROM INFILE 'input.csv' RETURNING (SELECT 1) FORMAT CSV$$);

SELECT position(
    parseQueryToJSON($$INSERT INTO t FROM INFILE 'input.csv' RETURNING (SELECT 1) FORMAT CSV$$),
    '"returning_select":{"type":"SelectWithUnionQuery"') > 0;

SELECT position(
    formatQueryFromJSON(parseQueryToJSON($$INSERT INTO t FROM INFILE 'input.csv' RETURNING (SELECT 1) FORMAT CSV$$)),
    'RETURNING (SELECT 1) FORMAT CSV') > 0;

SELECT formatQueryFromJSON(parseQueryToJSON(
    $$INSERT INTO t FROM INFILE 'input.csv' COMPRESSION 'gzip' SETTINGS max_threads = 1 RETURNING (SELECT number FROM numbers(1)) FORMAT CSV$$))
    = formatQuerySingleLine(
        $$INSERT INTO t FROM INFILE 'input.csv' COMPRESSION 'gzip' SETTINGS max_threads = 1 RETURNING (SELECT number FROM numbers(1)) FORMAT CSV$$);
