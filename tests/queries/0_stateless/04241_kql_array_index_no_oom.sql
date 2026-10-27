-- Regression test: parsing KQL array indexing with deeply nested square brackets used to allocate
-- exponentially with depth (3^N) because the converted SQL duplicated the inner expression three
-- times. With N ~ 20 the create_parser fuzzer OOMed at 6+ GB of RSS.
SET allow_experimental_kusto_dialect = 1;
SET max_memory_usage = '256Mi';

-- Parsing must complete with bounded memory. The input is intentionally unbalanced so the
-- inner expression is empty — we only care that the parse attempt does not balloon memory
-- while building the intermediate string for `getExpression`.
SELECT ignore(formatQuerySingleLine('SELECT * FROM kql($$Customers | where x' || repeat('[', 30) || '$$);')) FORMAT Null;
