-- { echoOn }

SELECT arrayFilter(x -> (x IS NOT NULL), []);

SELECT arrayFilter(x -> (x IS NOT NULL), [NULL]);

SELECT arrayFilter(x -> (x IS NOT NULL), [1]);

SELECT arrayFilter(x -> (x IS NULL), []);

SELECT arrayFilter(x -> (x IS NULL), [NULL]);

SELECT arrayFilter(x -> (x IS NULL), [1]);
