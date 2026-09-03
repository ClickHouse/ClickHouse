-- concat with empty string to defeat injectiveness of toString assumption.
SELECT concat('', toString(toDateTime('1981-09-29 00:00:00', 'Europe/Moscow') + INTERVAL number * 300 SECOND)) AS k FROM numbers(10000) GROUP BY k HAVING count() > 1 ORDER BY k;
SELECT concat('', toString(toDateTime('2018-09-19 00:00:00', 'Asia/Tehran') + INTERVAL number * 300 SECOND)) AS k FROM numbers(1000) GROUP BY k HAVING count() > 1 ORDER BY k;
