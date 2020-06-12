SELECT count(*) FROM hits_10m;
SELECT count(*) FROM hits_10m WHERE AdvEngineID != 0;
SELECT sum(AdvEngineID), count(*), avg(ResolutionWidth) FROM hits_10m;
SELECT sum(UserID) FROM hits_10m;
SELECT count(DISTINCT UserID) FROM hits_10m;
SELECT count(DISTINCT SearchPhrase) FROM hits_10m;
SELECT min(EventDate), max(EventDate) FROM hits_10m;

SELECT AdvEngineID, count(*) AS c FROM hits_10m WHERE AdvEngineID != 0 GROUP BY AdvEngineID ORDER BY c  DESC;
-- мощная фильтрация. После фильтрации почти ничего не остаётся, но делаем ещё агрегацию.;

SELECT RegionID, count(DISTINCT UserID) AS u FROM hits_10m GROUP BY RegionID ORDER BY u DESC LIMIT 10;
-- агрегация, среднее количество ключей.;

SELECT RegionID, sum(AdvEngineID), count(*) AS c, avg(ResolutionWidth), count(DISTINCT UserID) FROM hits_10m GROUP BY RegionID ORDER BY c DESC LIMIT 10;
-- агрегация, среднее количество ключей, несколько агрегатных функций.;

SELECT MobilePhoneModel, count(DISTINCT UserID) AS u FROM hits_10m WHERE MobilePhoneModel != '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
-- мощная фильтрация по строкам, затем агрегация по строкам.;

SELECT MobilePhone, MobilePhoneModel, count(DISTINCT UserID) AS u FROM hits_10m WHERE MobilePhoneModel != '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
-- мощная фильтрация по строкам, затем агрегация по паре из числа и строки.;

SELECT SearchPhrase, count(*) AS c FROM hits_10m WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
-- средняя фильтрация по строкам, затем агрегация по строкам, большое количество ключей.;

SELECT SearchPhrase, count(DISTINCT UserID) AS u FROM hits_10m WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
-- агрегация чуть сложнее.;

SELECT SearchEngineID, SearchPhrase, count(*) AS c FROM hits_10m WHERE SearchPhrase != '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
-- агрегация по числу и строке, большое количество ключей.;

SELECT UserID, count(*) AS c FROM hits_10m GROUP BY UserID ORDER BY c DESC LIMIT 10;
-- агрегация по очень большому количеству ключей, может не хватить оперативки.;

SELECT UserID, SearchPhrase, count(*) AS c FROM hits_10m GROUP BY UserID, SearchPhrase ORDER BY c DESC LIMIT 10;
-- ещё более сложная агрегация.;

SELECT UserID, SearchPhrase, count(*) AS c FROM hits_10m GROUP BY UserID, SearchPhrase LIMIT 10;
-- то же самое, но без сортировки.;

SELECT UserID, minute(EventTime), SearchPhrase, count(*) AS c FROM hits_10m GROUP BY UserID, minute(EventTime), SearchPhrase ORDER BY c DESC LIMIT 10;
-- ещё более сложная агрегация, не стоит выполнять на больших таблицах.;

SELECT UserID FROM hits_10m WHERE UserID = 12345678901234567890;
-- мощная фильтрация по столбцу типа UInt64.;

SELECT count(*) AS c FROM hits_10m WHERE URL LIKE '%metrika%';
-- фильтрация по поиску подстроки в строке.;

SELECT SearchPhrase, MAX(URL), count(*) AS c FROM hits_10m WHERE URL LIKE '%metrika%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
-- вынимаем большие столбцы, фильтрация по строке.;

SELECT SearchPhrase, MAX(URL), MAX(Title), count(*) AS c, count(DISTINCT UserID) FROM hits_10m WHERE Title LIKE '%Яндекс%' AND URL NOT LIKE '%.yandex.%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
-- чуть больше столбцы.;

SELECT * FROM hits_10m WHERE URL LIKE '%metrika%' ORDER BY EventTime LIMIT 10;
-- плохой запрос - вынимаем все столбцы.;

SELECT SearchPhrase, EventTime FROM hits_10m WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10;
-- большая сортировка.;

SELECT SearchPhrase FROM hits_10m WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10;
-- большая сортировка по строкам.;

SELECT SearchPhrase, EventTime FROM hits_10m WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10;
-- большая сортировка по кортежу.;

SELECT CounterID, avg(length(URL)) AS l, count(*) AS c FROM hits_10m WHERE URL != '' GROUP BY CounterID HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25;
-- считаем средние длины URL для крупных счётчиков.;

SELECT SUBSTRING(SUBSTRING(Referer, instr(Referer, '//') + 2), 1, if(0 < instr(SUBSTRING(Referer, instr(Referer, '//') + 2), '/') - 1, instr(SUBSTRING(Referer, instr(Referer, '//') + 2), '/' ) - 1, 0)), avg(length(Referer)) AS l, count(*) AS c, MAX(Referer) FROM hits_100m WHERE Referer != '' GROUP BY SUBSTRING(SUBSTRING(Referer, instr(Referer, '//') + 2), 1, if(0 < instr(SUBSTRING(Referer, instr(Referer, '//') + 2), '/') - 1, instr(SUBSTRING(Referer, instr(Referer, '//') + 2), '/' ) - 1, 0))  HAVING count(*) > 100000 ORDER BY l DESC LIMIT 25;
-- то же самое, но с разбивкой по доменам.;

SELECT sum(ResolutionWidth), sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 3), sum(ResolutionWidth + 4), sum(ResolutionWidth + 5), sum(ResolutionWidth + 6), sum(ResolutionWidth + 7), sum(ResolutionWidth + 8), sum(ResolutionWidth + 9), sum(ResolutionWidth + 10), sum(ResolutionWidth + 11), sum(ResolutionWidth + 12), sum(ResolutionWidth + 13), sum(ResolutionWidth + 14), sum(ResolutionWidth + 15), sum(ResolutionWidth + 16), sum(ResolutionWidth + 17), sum(ResolutionWidth + 18), sum(ResolutionWidth + 19), sum(ResolutionWidth + 20), sum(ResolutionWidth + 21), sum(ResolutionWidth + 22), sum(ResolutionWidth + 23), sum(ResolutionWidth + 24), sum(ResolutionWidth + 25), sum(ResolutionWidth + 26), sum(ResolutionWidth + 27), sum(ResolutionWidth + 28), sum(ResolutionWidth + 29), sum(ResolutionWidth + 30), sum(ResolutionWidth + 31), sum(ResolutionWidth + 32), sum(ResolutionWidth + 33), sum(ResolutionWidth + 34), sum(ResolutionWidth + 35), sum(ResolutionWidth + 36), sum(ResolutionWidth + 37), sum(ResolutionWidth + 38), sum(ResolutionWidth + 39), sum(ResolutionWidth + 40), sum(ResolutionWidth + 41), sum(ResolutionWidth + 42), sum(ResolutionWidth + 43), sum(ResolutionWidth + 44), sum(ResolutionWidth + 45), sum(ResolutionWidth + 46), sum(ResolutionWidth + 47), sum(ResolutionWidth + 48), sum(ResolutionWidth + 49), sum(ResolutionWidth + 50), sum(ResolutionWidth + 51), sum(ResolutionWidth + 52), sum(ResolutionWidth + 53), sum(ResolutionWidth + 54), sum(ResolutionWidth + 55), sum(ResolutionWidth + 56), sum(ResolutionWidth + 57), sum(ResolutionWidth + 58), sum(ResolutionWidth + 59), sum(ResolutionWidth + 60), sum(ResolutionWidth + 61), sum(ResolutionWidth + 62), sum(ResolutionWidth + 63), sum(ResolutionWidth + 64), sum(ResolutionWidth + 65), sum(ResolutionWidth + 66), sum(ResolutionWidth + 67), sum(ResolutionWidth + 68), sum(ResolutionWidth + 69), sum(ResolutionWidth + 70), sum(ResolutionWidth + 71), sum(ResolutionWidth + 72), sum(ResolutionWidth + 73), sum(ResolutionWidth + 74), sum(ResolutionWidth + 75), sum(ResolutionWidth + 76), sum(ResolutionWidth + 77), sum(ResolutionWidth + 78), sum(ResolutionWidth + 79), sum(ResolutionWidth + 80), sum(ResolutionWidth + 81), sum(ResolutionWidth + 82), sum(ResolutionWidth + 83), sum(ResolutionWidth + 84), sum(ResolutionWidth + 85), sum(ResolutionWidth + 86), sum(ResolutionWidth + 87), sum(ResolutionWidth + 88), sum(ResolutionWidth + 89) FROM hits_10m;
-- много тупых агрегатных функций.;

SELECT SearchEngineID, ClientIP, count(*) AS c, sum(Refresh), avg(ResolutionWidth) FROM hits_10m WHERE SearchPhrase != '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
-- сложная агрегация, для больших таблиц может не хватить оперативки.;

SELECT WatchID, ClientIP, count(*) AS c, sum(Refresh), avg(ResolutionWidth) FROM hits_10m WHERE SearchPhrase != '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
-- агрегация по двум полям, которая ничего не агрегирует. Для больших таблиц выполнить не получится.;

SELECT WatchID, ClientIP, count(*) AS c, sum(Refresh), avg(ResolutionWidth) FROM hits_10m GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
-- то же самое, но ещё и без фильтрации.;

SELECT URL, count(*) AS c FROM hits_10m GROUP BY URL ORDER BY c DESC LIMIT 10;
-- агрегация по URL.;

SELECT 1, URL, count(*) AS c FROM hits_10m GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
-- агрегация по URL и числу.;

SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, count(*) AS c FROM hits_10m GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
 
SELECT URL,     count(*) AS PageViews FROM hits_10m WHERE CounterID = 62 AND EventDate >= TIMESTAMP('2013-07-01') AND EventDate <= TIMESTAMP('2013-07-31') AND NOT DontCountHits != 0 AND NOT Refresh != 0 AND URL != '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;


SELECT Title, count(*) AS PageViews FROM hits_10m WHERE CounterID = 62 AND EventDate >= TIMESTAMP('2013-07-01') AND EventDate <= TIMESTAMP('2013-07-31') AND NOT DontCountHits != 0 AND NOT Refresh != 0 AND Title != '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;


SELECT URL, count(*) AS PageViews FROM hits_10m WHERE CounterID = 62 AND EventDate >= TIMESTAMP('2013-07-01') AND EventDate <= TIMESTAMP('2013-07-31') AND NOT Refresh != 0 AND IsLink != 0 AND NOT IsDownload != 0  GROUP BY URL ORDER BY PageViews DESC LIMIT 1000;

SELECT TraficSourceID, SearchEngineID, AdvEngineID, URL, count(*) as c, if(SearchEngineID   = 0 AND AdvEngineID = 0 , Referer, '') as src FROM hits_100m WHERE CounterID = 62 AND EventDate >= TIMESTAMP('2013-07-01') AND EventDate <= TIMESTAMP('2013-07-31') AND NOT Refresh != 0 GROUP BY TraficSourceID, SearchEngineID,  AdvEngineID, URL, if(SearchEngineID   = 0 AND AdvEngineID = 0 , Referer, '') ORDER BY c DESC LIMIT 1000;

SELECT URLHash, EventDate, count(*) AS PageViews FROM hits_10m WHERE   CounterID = 62 AND EventDate >= TIMESTAMP('2013-07-01') AND EventDate <= TIMESTAMP('2013-07-31') AND NOT Refresh != 0 AND TraficSourceID IN (-1, 6) AND RefererHash =  6202628419148573758  GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 100000; 

SELECT WindowClientWidth, WindowClientHeight, count(*)  AS PageViews FROM hits_10m WHERE CounterID = 62 AND EventDate >= TIMESTAMP('2013-07-01') AND EventDate       <= TIMESTAMP('2013-07-31') AND NOT Refresh != 0 AND NOT DontCountHits != 0 AND URLHash =  6202628419148573758 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY      PageViews  DESC LIMIT 10000; 

SELECT unix_timestamp(EventTime) - SECOND(EventTime) AS m, count(*) FROM hits_10m WHERE CounterID = 62 AND EventDate >= TIMESTAMP('2013-07-01') AND EventDate <= TIMESTAMP('2013-07-02') AND NOT Refresh != 0 AND NOT DontCountHits != 0 GROUP BY unix_timestamp(EventTime) - SECOND(EventTime) ORDER BY m; 
