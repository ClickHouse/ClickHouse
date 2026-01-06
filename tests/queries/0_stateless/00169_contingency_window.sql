-- Tags: stateful

WITH URLDomain AS a, URLDomain AS b
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2) FROM test.hits;

WITH URLDomain AS a, RefererDomain AS b, cityHash64(WatchID) AS sample_hash
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2)
FROM test.hits WHERE sample_hash % 10 = 0;

WITH URLDomain AS a, CounterID AS b, cityHash64(WatchID) AS sample_hash
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2)
FROM test.hits WHERE sample_hash % 10 = 0;

WITH ClientIP AS a, RemoteIP AS b, cityHash64(WatchID) AS sample_hash
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2)
FROM test.hits WHERE sample_hash % 10 = 0;

WITH ResolutionWidth AS a, ResolutionHeight AS b
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2) FROM test.hits;
