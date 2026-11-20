-- Regressions for `let` binding source-only tabular right-hand sides (a parenthesized
-- source, `union`), for the tabular-subquery form of `in` (`x in (T | project key)`), and
-- for datetime `bin_at` returning null for a negative bin size like the numeric form does.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- let binds a parenthesized source --';
let T = (datatable (n:long) [1]);
T | count;

print '-- let binds a union --';
let U = union (datatable (n:long) [1]), (datatable (n:long) [2]);
U | count;

let V = union (datatable (n:long) [1]), (datatable (n:long) [2, 3]);
V | summarize s = sum(n);

print '-- a union bound in a function body still works --';
let F = () { union (datatable (n:long) [1]), (datatable (n:long) [2]) };
F | count;

print '-- a parenthesized scalar stays a scalar --';
let x = (1 + 2);
print x;

print '-- a parenthesized pipeline still binds a table --';
let P = (datatable (n:long) [1, 2, 3] | take 2);
P | count;

print '-- in takes a tabular expression --';
let S = datatable (s:string) ['x', 'y'];
print v = 'x' | where v in (S | project s);

let S = datatable (s:string) ['x', 'y'];
print v = 'z' | where v in (S | project s) | count;

let S = datatable (s:string) ['x', 'y'];
print v = 'z' | where v !in (S | project s);

let S = datatable (s:string) ['x', 'y'];
print v = 'x' | where v in (S);

print v = 'x' | where v in (datatable (s:string) ['x'] | project s);

let S = datatable (s:string) ['x', 'y'];
datatable (v:string) ['x', 'y', 'z'] | where v in (S) | summarize c = count();

print '-- the list form still works --';
print 'a' in ('a', 'b');
print 'c' in ('a', 'b');
print 'A' in~ ('a', 'b');

print '-- in~ does not take a tabular expression --';
let S = datatable (s:string) ['x', 'y']; print v = 'x' | where v in~ (S | project s); -- { clientError SYNTAX_ERROR }

print '-- datetime bin_at with a negative bin size is null --';
print isnull(bin_at(datetime(2026-08-01 12:34:56), -1h, datetime(2026-08-01 00:00:00)));
print isnull(bin_at(datetime(2026-08-01), -1d, datetime(2026-07-01)));

print '-- and a positive one keeps rounding from the fixed point --';
print bin_at(datetime(2026-08-01 12:34:56), 1h, datetime(2026-08-01 00:30:00));
print bin_at(6.5, 2.5, -0.5);
