SELECT any(toTypeName(s)) FROM (SELECT ('a' :: String) as s) t1 FULL JOIN (SELECT ('b' :: LowCardinality(String)) as s) t2 USING (s);
