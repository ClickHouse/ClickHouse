DROP TABLE IF EXISTS mv_expend_test_table;
CREATE TABLE mv_expend_test_table
(    
   a UInt8,
   b Array(String),
   c Array(Int8),
   d Array(Int8)
) ENGINE = Memory;
INSERT INTO mv_expend_test_table VALUES (1, ['Salmon', 'Steak','Chicken'],[1,2,3,4],[5,6,7,8]);
set dialect='kusto';
print '-- mv-expend';
mv_expend_test_table | mv-expand c;
mv_expend_test_table | mv-expand c, d;
mv_expend_test_table | mv-expand b | mv-expand c;
mv_expend_test_table | mv-expand with_itemindex=index b, c, d;
mv_expend_test_table | mv-expand array_concat(c,d);
mv_expend_test_table | mv-expand x = c, y = d;
mv_expend_test_table | mv-expand xy = array_concat(c, d);
mv_expend_test_table | mv-expand xy = array_concat(c, d) limit 2| summarize count() by xy;
mv_expend_test_table | mv-expand with_itemindex=index c,d to typeof(bool);
-- mv_expend_test_table | mv-expand c to typeof(bool);
