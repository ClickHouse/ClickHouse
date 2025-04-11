SELECT tokenizeChinese('他来到了网易杭研大厦');
SELECT length(tokenizeChinese('他来到了网易杭研大厦'));

SELECT tokenizeChinese('我来自北京邮电大学。');
SELECT length(tokenizeChinese('我来自北京邮电大学。'));

SELECT tokenizeChinese('南京市长江大桥');
SELECT length(tokenizeChinese('南京市长江大桥'));

SELECT tokenizeChinese('我来自北京邮电大学。。。学号123456');
SELECT length(tokenizeChinese('我来自北京邮电大学。。。学号123456'));

SELECT tokenizeChinese('小明硕士毕业于中国科学院计算所，后在日本京都大学深造');
SELECT length(tokenizeChinese('小明硕士毕业于中国科学院计算所，后在日本京都大学深造'));
