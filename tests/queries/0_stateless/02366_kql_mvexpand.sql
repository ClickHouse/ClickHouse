-- datatable(a: int, b: dynamic, c: dynamic, d: dynamic) [
--    1, dynamic(['Salmon', 'Steak', 'Chicken']), dynamic([1, 2, 3, 4]), dynamic([5, 6, 7, 8])
-- ]

DROP TABLE IF EXISTS mv_expand_test_table;
CREATE TABLE mv_expand_test_table
(    
   a UInt8,
   b Array(String),
   c Array(Int8),
   d Array(Int8)
) ENGINE = Memory;
INSERT INTO mv_expand_test_table VALUES (1, ['Salmon', 'Steak','Chicken'],[1,2,3,4],[5,6,7,8]);
set dialect='kusto';
print '-- mv-expand --';
print '-- mv_expand_test_table | mv-expand c --';
mv_expand_test_table | mv-expand c;
print '-- mv_expand_test_table | mv-expand c, d --';
mv_expand_test_table | mv-expand c, d;
print '-- mv_expand_test_table | mv-expand b | mv-expand c --';
mv_expand_test_table | mv-expand b | mv-expand c;
print '-- mv_expand_test_table | mv-expand with_itemindex=index b, c, d --';
mv_expand_test_table | mv-expand with_itemindex=index b, c, d;
print '-- mv_expand_test_table | mv-expand array_concat(c,d) --';
mv_expand_test_table | mv-expand array_concat(c,d);
print '-- mv_expand_test_table | mv-expand x = c, y = d --';
mv_expand_test_table | mv-expand x = c, y = d;
print '-- mv_expand_test_table | mv-expand xy = array_concat(c, d) --';
mv_expand_test_table | mv-expand xy = array_concat(c, d);
print '-- mv_expand_test_table | mv-expand xy = array_concat(c, d) limit 2| summarize count() by xy --';
mv_expand_test_table | mv-expand xy = array_concat(c, d) limit 2| summarize count() by xy;
print '-- mv_expand_test_table | mv-expand with_itemindex=index c,d to typeof(bool) --';
mv_expand_test_table | mv-expand with_itemindex=index c,d to typeof(bool);
print '-- mv_expand_test_table | mv-expand c to typeof(bool) --';
mv_expand_test_table | mv-expand c to typeof(bool);
SET max_query_size = 28;
SET dialect='kusto';
mv_expand_test_table | mv-expand c, d; -- { serverError SYNTAX_ERROR }
SET max_query_size=262144;
