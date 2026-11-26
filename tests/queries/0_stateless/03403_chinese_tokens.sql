-- Tags: no-fasttest
-- no-fasttest: chinese tokens use Jieba

SELECT 'Chinese tokenizer';
SELECT '-- coarse_grained (default)';
SELECT tokens('', 'chinese');
SELECT tokens('他来到了网易杭研大厦', 'chinese');
SELECT tokens('我来自北京邮电大学。', 'chinese');
SELECT tokens('南京市长江大桥', 'chinese');
SELECT tokens('我来自北京邮电大学。。。学号123456', 'chinese');
SELECT tokens('小明硕士毕业于中国科学院计算所，后在日本京都大学深造', 'chinese');
SELECT '-- coarse_grained';
SELECT tokens('', 'chinese');
SELECT tokens('他来到了网易杭研大厦', 'chinese', 'coarse_grained');
SELECT tokens('我来自北京邮电大学。', 'chinese', 'coarse_grained');
SELECT tokens('南京市长江大桥', 'chinese', 'coarse_grained');
SELECT tokens('我来自北京邮电大学。。。学号123456', 'chinese', 'coarse_grained');
SELECT tokens('小明硕士毕业于中国科学院计算所，后在日本京都大学深造', 'chinese', 'coarse_grained');
SELECT '-- fine_grained';
SELECT tokens('', 'chinese', 'fine_grained');
SELECT tokens('他来到了网易杭研大厦', 'chinese', 'fine_grained');
SELECT tokens('我来自北京邮电大学。', 'chinese', 'fine_grained');
SELECT tokens('南京市长江大桥', 'chinese', 'fine_grained');
SELECT tokens('我来自北京邮电大学。。。学号123456', 'chinese', 'fine_grained');
SELECT tokens('小明硕士毕业于中国科学院计算所，后在日本京都大学深造', 'chinese', 'fine_grained');

SELECT '-- mixed ASCII and Chinese';
SELECT tokens('ClickHouse是一个数据库', 'chinese');
SELECT tokens('使用5G网络下载iPhone6s', 'chinese');
SELECT tokens('email: foo@bar.com 你好', 'chinese');
SELECT tokens('ClickHouse是一个数据库', 'chinese', 'fine_grained');

SELECT '-- FixedString (NUL padding is treated as a separator)';
SELECT tokens('北京'::FixedString(20), 'chinese');
SELECT tokens('北京大学'::FixedString(20), 'chinese');
SELECT tokens('5G网络'::FixedString(20), 'chinese');
SELECT tokens(CAST('北京' AS FixedString(20)), 'chinese') = tokens('北京', 'chinese');
