-- Tags: no-fasttest

SELECT regionToName(number::UInt32, 'en') FROM numbers(13);
SELECT regionToName(number::UInt32, 'xy') FROM numbers(13); -- { serverError POCO_EXCEPTION }

SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
SELECT regionToName(number::UInt32, 'en'), regionToArea(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
SELECT regionToName(number::UInt32, 'en'), regionToDistrict(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(13) AS n1 CROSS JOIN numbers(13) AS n2;
SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(13);
SELECT regionToName(number::UInt32, 'es') FROM numbers(4);
