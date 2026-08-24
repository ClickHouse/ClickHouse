-- TinyLog
DROP TABLE IF EXISTS nested_01800_tiny_log;
CREATE TABLE nested_01800_tiny_log (`column` Nested(name String, names Array(String), types Array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3)))) ENGINE = TinyLog;
INSERT INTO nested_01800_tiny_log VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);
SELECT 10 FROM nested_01800_tiny_log FORMAT Null;
DROP TABLE nested_01800_tiny_log;

-- StripeLog
DROP TABLE IF EXISTS nested_01800_stripe_log;
CREATE TABLE nested_01800_stripe_log (`column` Nested(name String, names Array(String), types Array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3)))) ENGINE = StripeLog;
INSERT INTO nested_01800_stripe_log VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);
SELECT 10 FROM nested_01800_stripe_log FORMAT Null;
DROP TABLE nested_01800_stripe_log;

-- Log
DROP TABLE IF EXISTS nested_01800_log;
CREATE TABLE nested_01800_log (`column` Nested(name String, names Array(String), types Array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3)))) ENGINE = Log;
INSERT INTO nested_01800_log VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);
SELECT 10 FROM nested_01800_log FORMAT Null;
DROP TABLE nested_01800_log;
