-- `let` can bind a function, not just a value.
--
--     let Name = ( [Parameters] ) { Body } ;
--
-- A function is a calculation, not a value: the body is re-parsed at every call with the
-- parameter names bound to that call's arguments. Whether the body is read as a scalar or as
-- a pipeline depends on where the name is used.
--
-- Note the shape of every case below: a `let` binds only for the statement that follows it,
-- because one KQL statement is one ClickHouse query. That is also what keeps the binding from
-- leaking between concurrent queries. A name needed by two statements is bound twice.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- scalar functions --';
let MultiplyByN = (val:long, n:long) { val * n };
print MultiplyByN(5, 3);

let Square = (x:long) { x * x };
print Square(7);

print '-- a function is usable more than once in a statement, and composes --';
let Sq = (x:long) { x * x };
print Sq(3) + Sq(4);

let Sq2 = (x:long) { x * x };
print Sq2(Sq2(2));

let Inc = (x:long) { x + 1 };
let Twice = (x:long) { Inc(Inc(x)) };
print Twice(10);

print '-- no parameters, with or without the parentheses --';
let One = () { 1 };
print One;

let Two = () { 2 };
print Two();

print '-- default values --';
let WithDefault = (a:long, b:long = 5) { a + b };
print taken_default = WithDefault(1), given = WithDefault(1, 2);

print '-- named arguments, in any order --';
let Three = (a:long, b:string = 'b.default', c:long = 0) { strcat(a, '-', b, '-', c) };
print positional_then_named = Three(12, c = 7), all_named = Three(c = 7, a = 12);

let Four = (a:long, b:string = 'x', c:long = 0) { strcat(a, '-', b, '-', c) };
print Four(a = 1, b = 'y', c = 2);

print '-- the body may declare its own lets, and sees the enclosing ones --';
let Outer = 10;
let UsesOuter = (x:long) { let y = x + Outer; y * 2 };
print UsesOuter(1);

print '-- a parameter shadows an outer binding of the same name --';
let shadowed = 100;
let Shadow = (shadowed:long) { shadowed };
print Shadow(1);

print '-- functions in pipeline operators --';
let Double = (x:long) { x * 2 };
datatable (a:long) [1, 2, 3] | extend d = Double(a);

let IsBig = (x:long) { x > 1 };
datatable (a:long) [1, 2, 3] | where IsBig(a);

let Half = (x:long) { x / 2 };
datatable (a:long) [2, 4, 6] | summarize s = sum(Half(a));

let Parity = (x:long) { x % 2 };
datatable (a:long) [1, 2, 3, 4] | summarize c = count() by k = Parity(a) | sort by k asc;

print '-- functions that return a table --';
let Numbers = () { datatable (a:long) [1, 2, 3] };
Numbers | where a > 1;

let Above = (n:long) { datatable (a:long) [1, 2, 3] | where a > n };
Above(1);

let Above2 = (n:long) { datatable (a:long) [1, 2, 3] | where a > n };
Above2(1) | count;

print '-- a tabular parameter --';
let Count = (T:(*)) { T | count };
Count(datatable (a:long) [1, 2, 3]);

let AboveIn = (T:(a:long), lim:long) { T | where a > lim };
AboveIn(datatable (a:long) [1, 5, 9], 4);

print '-- view marks a parameterless function; it is accepted and changes nothing here --';
let Seven = view () { datatable (a:long) [7] };
Seven;

print '-- a tabular function joins and unions like any other source --';
let Left = () { datatable (k:long, v:string) [1, 'x'] };
Left | join (datatable (k:long, w:string) [1, 'y']) on k;

let One1 = () { datatable (a:long) [1] };
union (One1), (One1) | sort by a asc;

SET dialect = 'clickhouse';
