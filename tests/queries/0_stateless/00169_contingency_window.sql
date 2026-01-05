-- Tags: stateful

WITH URLDomain AS a, URLDomain AS b
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2) FROM test.hits;

WITH URLDomain AS a, RefererDomain AS b
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2) FROM test.hits;

WITH URLDomain AS a, CounterID AS b
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2) FROM test.hits;

WITH ClientIP AS a, RemoteIP AS b
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2) FROM test.hits;

WITH ResolutionWidth AS a, ResolutionHeight AS b
SELECT round(cramersVWindow(a, b), 2), round(cramersVBiasCorrectedWindow(a, b), 2), round(theilsUWindow(a, b), 2), round(theilsUWindow(b, a), 2), round(contingencyWindow(a, b), 2) FROM test.hits;
