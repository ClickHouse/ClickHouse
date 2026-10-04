-- Pin the behavior documented for `cutURLParameter`: only the first `name=value` occurrence of each
-- requested parameter is removed, and value-less flag parameters are left intact.

-- Only the first occurrence of a repeated parameter is removed.
SELECT cutURLParameter('http://example.com/?a=1&a=2&b=3', 'a');
SELECT cutURLParameter('http://example.com/?a=1&a=2&b=3', ['a', 'b']);

-- Value-less flag parameters are not matched, because only `name=` is searched for.
SELECT cutURLParameter('http://bigmir.net/?a&c=d#e=f', 'a');
SELECT cutURLParameter('http://example.com/?a&a=1', 'a');

-- The same holds for a non-constant URL column.
SELECT cutURLParameter(materialize('http://example.com/?a=1&a=2&b=3'), 'a');
SELECT cutURLParameter(materialize('http://bigmir.net/?a&c=d#e=f'), 'a');
