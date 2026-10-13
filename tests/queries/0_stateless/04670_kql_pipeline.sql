-- Core KQL tabular operators.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- print --';
print 1;
print x = 1, y = 'two';
print 1 + 2 * 3;

print '-- datatable --';
datatable (Name:string, Score:long) ['a', 10, 'b', 20, 'c', 30];

print '-- where --';
datatable (Name:string, Score:long) ['a', 10, 'b', 20, 'c', 30] | where Score > 15;
datatable (Name:string, Score:long) ['a', 10, 'b', 20, 'c', 30] | where Score > 5 | where Name != 'b';

print '-- project --';
datatable (Name:string, Score:long) ['a', 10, 'b', 20] | project Name;
datatable (Name:string, Score:long) ['a', 10, 'b', 20] | project Doubled = Score * 2;

print '-- extend --';
datatable (Name:string, Score:long) ['a', 10, 'b', 20] | extend Doubled = Score * 2;
-- An extend that reuses a name replaces the column rather than duplicating it.
datatable (Name:string, Score:long) ['a', 10] | extend Score = Score + 1;
-- A later operator can refer to a column an earlier extend introduced.
datatable (Name:string, Score:long) ['a', 10, 'b', 20] | extend D = Score * 2 | where D > 25 | project Name, D;

print '-- project-away / project-keep / project-rename --';
datatable (A:long, B:long, C:long) [1, 2, 3] | project-away B;
datatable (A:long, B:long, C:long) [1, 2, 3] | project-keep A, C;
datatable (A:long, B:long) [1, 2] | project-rename Renamed = A;

print '-- sort --';
datatable (N:long) [3, 1, 2] | sort by N;
datatable (N:long) [3, 1, 2] | sort by N asc;
datatable (N:long) [3, 1, 2] | order by N asc;

print '-- take / top --';
datatable (N:long) [3, 1, 2] | sort by N asc | take 2;
datatable (N:long) [3, 1, 2] | top 2 by N;
datatable (N:long) [3, 1, 2] | sort by N asc | limit 1;

print '-- summarize --';
datatable (K:string, V:long) ['a', 1, 'a', 2, 'b', 3] | summarize sum(V) by K | sort by K asc;
datatable (K:string, V:long) ['a', 1, 'a', 2, 'b', 3] | summarize Total = sum(V) by K | sort by K asc;
datatable (K:string, V:long) ['a', 1, 'a', 2, 'b', 3] | summarize count() by K | sort by K asc;
datatable (V:long) [1, 2, 3] | summarize avg(V), min(V), max(V);
-- A filter after summarize applies to the aggregate, like HAVING.
datatable (K:string, V:long) ['a', 1, 'a', 2, 'b', 3] | summarize T = sum(V) by K | where T > 2 | sort by K asc;

print '-- distinct / count --';
datatable (N:long) [1, 1, 2] | distinct N | sort by N asc;
datatable (N:long) [1, 1, 2] | count;
datatable (N:long) [1, 1, 2] | count as Total;

print '-- range --';
range x from 1 to 5 step 1;
range x from 0 to 10 step 5;

print '-- mv-expand --';
print a = dynamic([1, 2, 3]) | mv-expand a;

print '-- union --';
union (print a = 1), (print a = 2) | sort by a asc;

print '-- join --';
datatable (K:long, L:string) [1, 'x', 2, 'y']
| join (datatable (K:long, R:string) [1, 'p', 3, 'q']) on K
| sort by K asc;

datatable (K:long, L:string) [1, 'x', 2, 'y']
| join kind=leftouter (datatable (K:long, R:string) [1, 'p']) on K
| sort by K asc;

print '-- let --';
let threshold = 15;
datatable (N:long) [10, 20] | where N > threshold;

let Numbers = datatable (N:long) [1, 2, 3];
Numbers | where N > 1 | sort by N asc;

print '-- render and as are no-ops --';
datatable (N:long) [1] | as Result | render table;

SET dialect = 'clickhouse';
