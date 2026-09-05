

CREATE FUNCTION IF NOT EXISTS unhexPrefixed AS value -> unhex(substring(value, 3));
CREATE FUNCTION IF NOT EXISTS hex2bytes AS address -> CAST(unhexPrefixed(address), 'FixedString(20)');
CREATE FUNCTION IF NOT EXISTS bytes2hex AS address -> concat('0x', lower(hex(address)));

CREATE TABLE test
(
    `transfer_id` String,
    `address` FixedString(20),
    `value` UInt256,
    `block_timestamp` DateTime('UTC'),
    `token_address` FixedString(20)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_timestamp)
PRIMARY KEY (address, block_timestamp)
ORDER BY (address, block_timestamp);

INSERT INTO test SELECT 'token-transfer-0x758f1bbabb160683e1c80ed52dcd24a32b599d40edf1cec91b5f1199c0e392a2-56', hex2bytes('0xd387a6e4e84a6c86bd90c158c6028a58cc8ac459'), 3000000000000000000000, '2024-01-02 16:54:59', 'abc';

CREATE TABLE token_data
(
    token_address_hex String,
    chain String,
    is_blacklisted Bool
)
ENGINE = TinyLog;

INSERT INTO token_data SELECT bytes2hex('abc'), 'zksync', false;

CREATE DICTIONARY token_data_map
(
    token_address_hex String,
    chain String,
    is_blacklisted Bool
)
PRIMARY KEY token_address_hex, chain
SOURCE(Clickhouse(table token_data))
LIFETIME(MIN 200 MAX 300)
LAYOUT(COMPLEX_KEY_HASHED_ARRAY());

SELECT block_timestamp
FROM
(
    SELECT
        block_timestamp,
        bytes2hex(token_address) AS token_address_hex
    FROM
    (
        SELECT
            transfer_id,
            address,
            value,
            block_timestamp,
            token_address,
            'zksync' AS chain
        FROM test
    )
    WHERE (address = hex2bytes('0xd387a6e4e84a6c86bd90c158c6028a58cc8ac459')) AND (transfer_id NOT LIKE 'gas%') AND (value > 0) AND (dictGetOrDefault(token_data_map, 'is_blacklisted', (token_address_hex, 'zksync'), true))
)
SETTINGS max_threads = 1, short_circuit_function_evaluation = 'enable', enable_analyzer = 0;
