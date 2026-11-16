-- `expressionLooksTabular` recognized a physical table only when it was one bare word, so the
-- `database.table` source form that `parseSource` already accepts fell into the scalar parser and
-- died on the `.` as unsupported member access. Every carrier of that classifier is covered here:
-- a `let` binding, a tabular function body reached through a second `let`, and `in (...)`.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- a let binds a database-qualified table --';
let T = system.one; T | count;
let T = system.one; T | project x = dummy;

print '-- through a parameterless tabular function body --';
let F = () { system.one }; let T = F; T | count;
let F = () { system.one | project x = dummy }; F | project x;

print '-- the quoted identifier form of the table name --';
let T = system.['one']; T | count;

print '-- a tabular in (...) subquery over a qualified table --';
print flag = 0 in (system.one);
print flag = 1 in (system.one);

print '-- the pipelined form is unaffected --';
let T = system.numbers | take 3; T | count;

print '-- a scalar binding stays a scalar --';
let x = 1; print y = x;
let x = 1; let y = x; print z = y + 1;
