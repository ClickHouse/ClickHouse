-- Tags: no-fasttest
-- no-fasttest: Requires vectorscan
SELECT multiMatchAny('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multiMatchAnyIndex('goodbye', ['^hello[, ]+world$', 'go+d *bye', 'w(or)+ld']);
SELECT multiSearchAllPositions('hello, world', ['hello', 'world']);
