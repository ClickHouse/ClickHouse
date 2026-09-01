-- Kusto `extract(regexp, 0, text)` wraps the pattern in one more capturing group, so the
-- 127-group limit of `extractGroups` leaves 126 there. A non-zero index does not wrap.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print strlen(extract(strrep('(\\w)', 126), 0, strrep('a', 126)));
print extract(strrep('(\\w)', 127), 0, strrep('a', 127)); -- { serverError BAD_ARGUMENTS }
print extract(strrep('(\\w)', 127), 1, strrep('a', 127));

SET dialect = 'clickhouse';
