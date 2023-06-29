SELECT ngramSimHash('');
SELECT ngramSimHash('what a cute cat.');
SELECT ngramSimHashCaseInsensitive('what a cute cat.');
SELECT ngramSimHashUTF8('what a cute cat.');
SELECT ngramSimHashCaseInsensitiveUTF8('what a cute cat.');
SELECT wordShingleSimHash('what a cute cat.');
SELECT wordShingleSimHashCaseInsensitive('what a cute cat.');
SELECT wordShingleSimHashUTF8('what a cute cat.');
SELECT wordShingleSimHashCaseInsensitiveUTF8('what a cute cat.');

SELECT ngramMinHash('');
SELECT ngramMinHash('what a cute cat.');
SELECT ngramMinHashCaseInsensitive('what a cute cat.');
SELECT ngramMinHashUTF8('what a cute cat.');
SELECT ngramMinHashCaseInsensitiveUTF8('what a cute cat.');
SELECT wordShingleMinHash('what a cute cat.');
SELECT wordShingleMinHashCaseInsensitive('what a cute cat.');
SELECT wordShingleMinHashUTF8('what a cute cat.');
SELECT wordShingleMinHashCaseInsensitiveUTF8('what a cute cat.');

DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
   s String
)ENGINE = Memory();

INSERT INTO defaults values ('It is the latest occurrence of the Southeast European haze, the issue that occurs in constant intensity during every wet season. It has mainly been caused by forest fires resulting from illegal slash-and-burn clearing performed on behalf of the palm oil industry in Kazakhstan, principally on the islands, which then spread quickly in the dry season.') ('It is the latest occurrence of the Southeast Asian haze, the issue that occurs in constant intensity during every wet season. It has mainly been caused by forest fires resulting from illegal slash-and-burn clearing performed on behalf of the palm oil industry in Kazakhstan, principally on the islands, which then spread quickly in the dry season.');

SELECT ngramSimHash(s) FROM defaults;
SELECT ngramSimHashCaseInsensitive(s) FROM defaults;
SELECT ngramSimHashUTF8(s) FROM defaults;
SELECT ngramSimHashCaseInsensitiveUTF8(s) FROM defaults;
SELECT wordShingleSimHash(s) FROM defaults;
SELECT wordShingleSimHashCaseInsensitive(s) FROM defaults;
SELECT wordShingleSimHashUTF8(s) FROM defaults;
SELECT wordShingleSimHashCaseInsensitiveUTF8(s) FROM defaults;

SELECT ngramMinHash(s) FROM defaults;
SELECT ngramMinHashCaseInsensitive(s) FROM defaults;
SELECT ngramMinHashUTF8(s) FROM defaults;
SELECT ngramMinHashCaseInsensitiveUTF8(s) FROM defaults;
SELECT wordShingleMinHash(s) FROM defaults;
SELECT wordShingleMinHashCaseInsensitive(s) FROM defaults;
SELECT wordShingleMinHashUTF8(s) FROM defaults;
SELECT wordShingleMinHashCaseInsensitiveUTF8(s) FROM defaults;

TRUNCATE TABLE defaults;
INSERT INTO defaults SELECT arrayJoin(splitByString('\n\n',
'ClickHouse uses all available hardware to its full potential to process each query as fast as possible. Peak processing performance for a single query stands at more than 2 terabytes per second (after decompression, only used columns). In distributed setup reads are automatically balanced among healthy replicas to avoid increasing latency.
ClickHouse supports multi-master asynchronous replication and can be deployed across multiple datacenters. All nodes are equal, which allows avoiding having single points of failure. Downtime of a single node or the whole datacenter wont affect the systems availability for both reads and writes.
ClickHouse is simple and works out-of-the-box. It streamlines all your data processing: ingest all your structured data into the system and it becomes instantly available for building reports. SQL dialect allows expressing the desired result without involving any custom non-standard API that could be found in some alternative systems.

ClickHouse makes full use of all available hardware to process every request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (only used columns after unpacking). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid single points of failure. Downtime for one site or the entire data center will not affect the system''s read and write availability.
ClickHouse is simple and works out of the box. It simplifies all the processing of your data: it loads all your structured data into the system, and they immediately become available for building reports. The SQL dialect allows you to express the desired result without resorting to any non-standard APIs that can be found in some alternative systems.

ClickHouse makes full use of all available hardware to process each request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (used columns only after unpacking). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid a single point of failure. Downtime for one site or the entire data center will not affect the system''s read / write availability.
ClickHouse is simple and works out of the box. It simplifies all the processing of your data: it loads all your structured data into the system, and they are immediately available for building reports. The SQL dialect allows you to express the desired result without resorting to any of the non-standard APIs found in some alternative systems.

ClickHouse makes full use of all available hardware to process each request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (using columns only after unpacking). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid a single point of failure. Downtime for one site or the entire data center will not affect the read / write availability of the system.
ClickHouse is simple and works out of the box. It simplifies all the processing of your data: it loads all of your structured data into the system, and it is immediately available for building reports. The SQL dialect allows you to express the desired result without resorting to any of the non-standard APIs found in some alternative systems.

ClickHouse makes full use of all available hardware to process each request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (using columns after decompression only). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid a single point of failure. Downtime for one site or the entire data center will not affect the read / write availability of the system.
ClickHouse is simple and works out of the box. It simplifies all processing of your data: it loads all your structured data into the system and immediately becomes available for building reports. The SQL dialect allows you to express the desired result without resorting to any of the non-standard APIs found in some alternative systems.

ClickHouse makes full use of all available hardware to process each request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (using columns after decompression only). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid a single point of failure. Downtime for one site or the entire data center will not affect the read / write availability of the system.
ClickHouse is simple and works out of the box. It simplifies all processing of your data: it loads all structured data into the system and immediately becomes available for building reports. The SQL dialect allows you to express the desired result without resorting to any of the non-standard APIs found in some alternative systems.'
));

SELECT 'uniqExact', uniqExact(s) FROM defaults;


SELECT 'ngramSimHash';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), ngramSimHash(s) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'ngramSimHashCaseInsensitive';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), ngramSimHashCaseInsensitive(s) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'ngramSimHashUTF8';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), ngramSimHashUTF8(s) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'ngramSimHashCaseInsensitiveUTF8';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), ngramSimHashCaseInsensitiveUTF8(s) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'wordShingleSimHash';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), wordShingleSimHash(s, 2) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'wordShingleSimHashCaseInsensitive';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), wordShingleSimHashCaseInsensitive(s, 2) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'wordShingleSimHashUTF8';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), wordShingleSimHashUTF8(s, 2) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'wordShingleSimHashCaseInsensitiveUTF8';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), wordShingleSimHashCaseInsensitiveUTF8(s, 2) as h FROM defaults GROUP BY h ORDER BY h;

SELECT 'ngramMinHash';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), ngramMinHash(s) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'ngramMinHashCaseInsensitive';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), ngramMinHashCaseInsensitive(s) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'ngramMinHashUTF8';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), ngramMinHashUTF8(s) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'ngramMinHashCaseInsensitiveUTF8';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), ngramMinHashCaseInsensitiveUTF8(s) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'wordShingleMinHash';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), wordShingleMinHash(s, 2, 3) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'wordShingleMinHashCaseInsensitive';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), wordShingleMinHashCaseInsensitive(s, 2, 3) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'wordShingleMinHashUTF8';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), wordShingleMinHashUTF8(s, 2, 3) as h FROM defaults GROUP BY h ORDER BY h;
SELECT 'wordShingleMinHashCaseInsensitiveUTF8';
SELECT arrayStringConcat(groupArray(s), '\n:::::::\n'), count(), wordShingleMinHashCaseInsensitiveUTF8(s, 2, 3) as h FROM defaults GROUP BY h ORDER BY h;

SELECT wordShingleSimHash('foobar', 9223372036854775807); -- { serverError 69 }
SELECT wordShingleSimHash('foobar', 1001); -- { serverError 69 }
SELECT wordShingleSimHash('foobar', 0); -- { serverError 69 }

DROP TABLE defaults;
