-- Regressions for `bin` and two-argument `floor` returning null for a negative bin size
-- (per row, since the bin size may be a column), and for a parameterless function whose
-- tabular body is a bare tabular name (`let F = () { Base };`).

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- a negative bin size yields null --';
print isnull(bin(4, -1));
print isnull(floor(4, -1));
print isnull(bin(4.5, -0.5));
print isnull(bin(-4, -5));
print isnull(bin_at(4, -1, 0));
print isnull(bin(datetime(2026-08-01 12:34:56), -1h));

print '-- and a per-row one nulls only its rows --';
datatable (x:long, b:long) [4, -1, 7, 2, -4, 5] | project r = bin(x, b);
datatable (x:real, b:real) [4.5, -0.5, 4.5, 0.5] | project r = bin(x, b);

print '-- non-negative bins keep their values --';
print bin(-4, 5);
print bin(4, 5);
print bin(4.5, 0.5);

print '-- a tabular body needs no pipe or source keyword to be tabular --';
let Base = datatable (n:long) [1, 2, 3];
let F = () { Base };
let T = F;
T | count;

let Base2 = datatable (n:long) [1, 2];
let F2 = () { Base2 | where n > 1 };
let T2 = F2;
T2 | count;

let G = () { let Inner = datatable (n:long) [1]; Inner };
let U = G;
U | count;

let H = () { let Src = () { datatable (n:long) [1, 2, 3, 4] }; let Mid = Src; Mid };
let V = H;
V | count;

print '-- a scalar body stays scalar --';
let s = () { 1 + 2 };
let x = s;
print x;
