-- Exercise KQL string comparison operators implemented in
-- Parsers/Kusto/ParserKQLOperators.cpp: contains/startswith/endswith/has
-- families, their case-sensitive _cs variants and negations, =~/!~/in~/!in~,
-- has_any, has_all.

DROP TABLE IF EXISTS kql_op_t;
CREATE TABLE kql_op_t (x String, y Int64, z Array(String)) ENGINE = Memory;
INSERT INTO kql_op_t VALUES
    ('hello', 1, ['a', 'b']),
    ('World', 2, ['c', 'd']),
    ('foo bar', 3, ['e']),
    ('HELLO', 4, ['f']);

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '--- contains (case-insensitive) ---';
kql_op_t | where x contains 'ell' | project x | sort by x asc;
print '--- contains_cs (case-sensitive) ---';
kql_op_t | where x contains_cs 'ell' | project x | sort by x asc;
print '--- !contains ---';
kql_op_t | where x !contains 'hello' | project x | sort by x asc;
print '--- !contains_cs ---';
kql_op_t | where x !contains_cs 'ELL' | project x | sort by x asc;

print '--- startswith ---';
kql_op_t | where x startswith 'he' | project x | sort by x asc;
print '--- startswith_cs ---';
kql_op_t | where x startswith_cs 'HE' | project x | sort by x asc;
print '--- !startswith ---';
kql_op_t | where x !startswith 'he' | project x | sort by x asc;
print '--- !startswith_cs ---';
kql_op_t | where x !startswith_cs 'he' | project x | sort by x asc;

print '--- endswith ---';
kql_op_t | where x endswith 'lo' | project x | sort by x asc;
print '--- endswith_cs ---';
kql_op_t | where x endswith_cs 'LO' | project x | sort by x asc;
print '--- !endswith ---';
kql_op_t | where x !endswith 'lo' | project x | sort by x asc;
print '--- !endswith_cs ---';
kql_op_t | where x !endswith_cs 'LO' | project x | sort by x asc;

print '--- has ---';
kql_op_t | where x has 'foo' | project x | sort by x asc;
print '--- has_cs ---';
kql_op_t | where x has_cs 'World' | project x | sort by x asc;
print '--- !has ---';
kql_op_t | where x !has 'foo' | project x | sort by x asc;
print '--- !has_cs ---';
kql_op_t | where x !has_cs 'world' | project x | sort by x asc;

print '--- has_any ---';
kql_op_t | where x has_any('hello', 'foo') | project x | sort by x asc;
print '--- has_any: one match ---';
kql_op_t | where x has_any('FOO', 'BAR') | project x | sort by x asc;
print '--- has_all ---';
kql_op_t | where x has_all('foo', 'bar') | project x | sort by x asc;

print '--- =~ (case-insensitive equals) ---';
kql_op_t | where x =~ 'HELLO' | project x | sort by x asc;
print '--- !~ (case-insensitive not equals) ---';
kql_op_t | where x !~ 'HELLO' | project x | sort by x asc;

print '--- in ---';
kql_op_t | where x in ('hello', 'foo bar') | project x | sort by x asc;
print '--- !in ---';
kql_op_t | where x !in ('hello', 'foo bar') | project x | sort by x asc;
print '--- in~ (case-insensitive) ---';
kql_op_t | where x in~ ('HELLO', 'FOO BAR') | project x | sort by x asc;
print '--- !in~ ---';
kql_op_t | where x !in~ ('HELLO') | project x | sort by x asc;

print '--- error: has_any with no open paren ---';
kql_op_t | where x has_any 'hello'; -- { clientError SYNTAX_ERROR }

print '--- error: has_all with no open paren ---';
kql_op_t | where x has_all 'hello'; -- { clientError SYNTAX_ERROR }
