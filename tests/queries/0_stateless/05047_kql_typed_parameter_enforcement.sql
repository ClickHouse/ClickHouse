SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- The declared types of function parameters are enforced at the call boundary.

-- A well-typed argument passes through.
let F = (a:long) { a }; print F(5);
let F = (s:string) { strlen(s) }; print F('abc');
let F = (b:bool) { b }; print F(true);
let F = (d:datetime) { d }; print F(datetime(2026-08-01 12:34:56));
let F = (t:timespan) { t }; print F(1h);
let F = (g:guid) { g }; print F(guid(74be27de-1e4e-49d9-b579-fe0b331d3642));

-- A lossless conversion is applied: a long argument of a real parameter widens.
let F = (r:real) { r }; print F(3);

-- A null literal is a typed null of whatever the parameter declares.
let F = (a:long) { isnull(a) }; print F(long(null));
let F = (t:timespan) { isnull(t) }; print F(timespan(null));

-- A literal default is enforced the same way the argument would be.
let F = (a:long = 7) { a }; print F();

-- A named argument is enforced too.
let F = (a:long, b:string) { strcat(b, tostring(a)) }; print F(b = 'n = ', a = 12);

-- An argument whose type does not belong to the declared KQL type is rejected.
let F = (a:long) { a }; print F('x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
let F = (a:long) { a }; print F(1.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
let F = (s:string) { s }; print F(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
let F = (t:timespan) { t }; print F(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
let F = (d:datetime) { d }; print F('2026-08-01'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
let F = (b:bool) { b }; print F(5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
let F = (g:guid) { g }; print F('74be27de-1e4e-49d9-b579-fe0b331d3642'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A value that does not fit the declared type is an error, not a silent truncation.
let F = (i:int) { i }; print F(3000000000); -- { serverError CANNOT_CONVERT_TYPE }

-- The declared column types of a tabular parameter are enforced on the argument's columns.
let G = (T:(a:long)) { T | project a }; G(datatable (a:long) [5]);
let G = (T:(a:long)) { T | project a }; G(datatable (a:string) ['x']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An expression argument is enforced by its type, not by how it is spelled.
let F = (a:long) { a }; print F(2 + 3);
let F = (a:long) { a }; print F(strlen('abcd'));
let F = (a:long) { a }; print F(2.5 * 2.0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
