-- Anything outside the supported subset must be rejected by name.
--
-- The point of these cases is that the failure is a *parse* error naming the construct, not
-- a mistranslation. The previous implementation registered 28 functions whose bodies did
-- nothing and fell through to "table name" for unknown operators, so `series_fir(...)`
-- failed as "Function series_fir does not exist" and `search 'x'` as "Unknown table
-- expression identifier 'search'".

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- Unsupported operators.
print a = 1 | search 'x';                             -- { clientError SYNTAX_ERROR }
print a = 1 | parse a with 'x' b;                     -- { clientError SYNTAX_ERROR }
print a = 1 | top-nested 2 of a by max(a);            -- { clientError SYNTAX_ERROR }
print a = 1 | mv-apply x = a to typeof(long) on (summarize sum(x));  -- { clientError SYNTAX_ERROR }
print a = 1 | lookup (print a = 1) on a;              -- { clientError SYNTAX_ERROR }
print a = 1 | evaluate bag_unpack(a);                 -- { clientError SYNTAX_ERROR }
print a = 1 | invoke foo();                           -- { clientError SYNTAX_ERROR }
print a = 1 | facet by a;                             -- { clientError SYNTAX_ERROR }

-- An unsupported operator written where a source belongs must still be named, not taken
-- for a table. `search 'x'` used to fail as "Unknown table expression identifier 'search'".
search 'x';                                           -- { clientError SYNTAX_ERROR }
parse a;                                              -- { clientError SYNTAX_ERROR }
find x;                                               -- { clientError SYNTAX_ERROR }
getschema;                                            -- { clientError SYNTAX_ERROR }
evaluate f();                                         -- { clientError SYNTAX_ERROR }
externaldata (a:long) [1];                            -- { clientError SYNTAX_ERROR }
make-series x;                                        -- { clientError SYNTAX_ERROR }
top-nested 2 of a;                                    -- { clientError SYNTAX_ERROR }

-- Unsupported functions.
print series_fir(dynamic([1, 2]), dynamic([1]));      -- { clientError SYNTAX_ERROR }
print series_stats(dynamic([1, 2, 3]));               -- { clientError SYNTAX_ERROR }
print pack_all();                                     -- { clientError SYNTAX_ERROR }
print bag_keys(dynamic([1]));                         -- { clientError SYNTAX_ERROR }
print parse_url('http://host/p');                     -- { clientError SYNTAX_ERROR }
print parse_csv('a,b');                               -- { clientError SYNTAX_ERROR }
print toscalar(1);                                    -- { clientError SYNTAX_ERROR }
print geo_point_in_polygon(1, 1, dynamic({}));        -- { clientError SYNTAX_ERROR } GeoJSON needs dynamic objects
print geo_s2cell_to_central_point('88d9b');           -- { clientError SYNTAX_ERROR } returns a GeoJSON object
print format_timespan(1d, 'hh:mm');                   -- { clientError SYNTAX_ERROR }

-- Wrong number of arguments is reported at parse time, not at execution.
print strlen();                                       -- { clientError SYNTAX_ERROR }
print strlen('a', 'b');                               -- { clientError SYNTAX_ERROR }
print substring('abc');                               -- { clientError SYNTAX_ERROR }
print case(1 > 2, 'a');                               -- { clientError SYNTAX_ERROR }

-- Dynamic objects and member access are outside the array mapping.
print dynamic({'a': 1});                              -- { clientError SYNTAX_ERROR }
print a = dynamic([1]) | project a.b;                 -- { clientError SYNTAX_ERROR }
print a = dynamic([1]) | project a['k'];              -- { clientError SYNTAX_ERROR }

-- Malformed input.
print a = 1 | where a = 1;                            -- { clientError SYNTAX_ERROR } '=' is not a comparison
print 1 +;                                            -- { clientError SYNTAX_ERROR }
print 1zz;                                            -- { clientError SYNTAX_ERROR }
print a = 1 | ;                                       -- { clientError SYNTAX_ERROR }
| where 1 == 1;                                       -- { clientError SYNTAX_ERROR }
where 1 == 1;                                         -- { clientError SYNTAX_ERROR }
print a = 1 | join (print b = 2);                     -- { clientError SYNTAX_ERROR } no 'on'
print a = 1 | join kind=nonesuch (print b = 2) on a;  -- { clientError SYNTAX_ERROR }
print a = 1 | join hint.strategy=shuffle (print b = 2) on a;  -- { clientError SYNTAX_ERROR }
datatable (a:long) [1, 2, 3;                          -- { clientError SYNTAX_ERROR }
datatable (a:nosuchtype) [1];                         -- { clientError SYNTAX_ERROR }
datatable (a:long, b:long) [1, 2, 3];                 -- { clientError SYNTAX_ERROR } not a whole number of rows
let x = 1;                                            -- { clientError SYNTAX_ERROR } a query needs a tabular expression
let x = 1; let x = 2; print x;                        -- { clientError SYNTAX_ERROR } duplicate binding

-- Malformed function definitions and calls.
let f = (x:long) { f(x) }; print f(1);                -- { clientError SYNTAX_ERROR } no recursion
let f = (x:long) { x }; print f();                    -- { clientError SYNTAX_ERROR } missing argument
let f = (x:long) { x }; print f(1, 2);                -- { clientError SYNTAX_ERROR } too many arguments
let f = (x:long) { x }; print f(y = 1);               -- { clientError SYNTAX_ERROR } no such parameter
let f = (x:long, x:long) { x }; print f(1, 2);        -- { clientError SYNTAX_ERROR } duplicate parameter
let f = (a:long = 1, b:long) { a }; print f(1, 2);    -- { clientError SYNTAX_ERROR } default before required
let f = (a:long, T:(*)) { a }; print f(1, 2);         -- { clientError SYNTAX_ERROR } tabular must come first
let f = (x:nosuchtype) { x }; print f(1);             -- { clientError SYNTAX_ERROR }
let f = (x:long) { }; print f(1);                     -- { clientError SYNTAX_ERROR } empty body
let f = (x:long) { x x }; print f(1);                 -- { clientError SYNTAX_ERROR } trailing text in body
let f = (x:long) { g(x) }; let g = (x:long) { f(x) }; print f(1);  -- { clientError SYNTAX_ERROR } mutual recursion

-- A name that is not a Kusto function at all is passed to ClickHouse, so an unknown one is
-- reported by the analyzer rather than by the parser. See 04674 for the point of that.
print totally_made_up_function(1);                    -- { serverError UNKNOWN_FUNCTION }

SET dialect = 'clickhouse';
