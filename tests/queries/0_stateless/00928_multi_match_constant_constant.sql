-- Tags: no-fasttest

SELECT multiMatchAny('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multiFuzzyMatchAny('goodbye', 1, ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);

SELECT multiMatchAnyIndex('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multiFuzzyMatchAnyIndex('goodbye', 1, ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);

SELECT multiMatchAllIndices('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multiFuzzyMatchAllIndices('goodbye', 1, ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);

SELECT multiSearchAllPositions('hello, world', ['hello', 'world']);
