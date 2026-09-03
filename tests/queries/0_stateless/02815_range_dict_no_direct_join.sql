CREATE TABLE discounts
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
ENGINE = Memory;

INSERT INTO discounts VALUES (1, '2015-01-01', Null, 0.1);
INSERT INTO discounts VALUES (1, '2015-01-15', Null, 0.2);
INSERT INTO discounts VALUES (2, '2015-01-01', '2015-01-15', 0.3);
INSERT INTO discounts VALUES (2, '2015-01-04', '2015-01-10', 0.4);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-15', 0.5);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-10', 0.6);

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
RANGE(MIN discount_start_date MAX discount_end_date);

CREATE TABLE ids (id UInt64) ENGINE = Memory;
INSERT INTO ids SELECT * FROM numbers(10);

SELECT id, amount FROM ids INNER JOIN discounts_dict ON id = advertiser_id ORDER BY id, amount SETTINGS join_algorithm = 'direct,hash';
SELECT id, amount FROM ids INNER JOIN discounts_dict ON id = advertiser_id ORDER BY id, amount SETTINGS join_algorithm = 'default';
SELECT id, amount FROM ids INNER JOIN discounts_dict ON id = advertiser_id ORDER BY id, amount SETTINGS join_algorithm = 'direct'; -- { serverError NOT_IMPLEMENTED }
