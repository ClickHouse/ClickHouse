-- Tags: no-fasttest

SELECT 'With constant values.';
WITH '他来到了网易杭研大厦' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '南京市长江大桥' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。。。学号123456' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '小明硕士毕业于中国科学院计算所，后在日本京都大学深造' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;

SELECT 'With non-const values.';
WITH '他来到了网易杭研大厦' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '南京市长江大桥' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。。。学号123456' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '小明硕士毕业于中国科学院计算所，后在日本京都大学深造' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;

SELECT 'With fixed string values.';
WITH '他来到了网易杭研大厦' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '南京市长江大桥' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。。。学号123456' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '小明硕士毕业于中国科学院计算所，后在日本京都大学深造' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;

SELECT 'With non-const and fixed string values.';
WITH '他来到了网易杭研大厦' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '南京市长江大桥' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。。。学号123456' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;
WITH '小明硕士毕业于中国科学院计算所，后在日本京都大学深造' as text
SELECT tokenize(text, 'chinese') as tokenized, length(tokenized) as length;

SELECT 'With constant values, coarse grained.';
WITH '他来到了网易杭研大厦' as text
SELECT tokenize(text, 'chinese', 'coarse-grained') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。' as text
SELECT tokenize(text, 'chinese', 'coarse-grained') as tokenized, length(tokenized) as length;
WITH '南京市长江大桥' as text
SELECT tokenize(text, 'chinese', 'coarse-grained') as tokenized, length(tokenized) as length;
WITH '我来自北京邮电大学。。。学号123456' as text
SELECT tokenize(text, 'chinese', 'coarse-grained') as tokenized, length(tokenized) as length;
WITH '小明硕士毕业于中国科学院计算所，后在日本京都大学深造' as text
SELECT tokenize(text, 'chinese', 'coarse-grained') as tokenized, length(tokenized) as length;
