SELECT generateUUIDv7(1) = generateUUIDv7(2);

SELECT generateUUIDv7() = generateUUIDv7(1);

SELECT generateUUIDv7(1) = generateUUIDv7(1);

SELECT generateUUIDv7WithCounter(1) = generateUUIDv7WithCounter(2);

SELECT generateUUIDv7WithCounter() = generateUUIDv7WithCounter(1);

SELECT generateUUIDv7WithCounter(1) = generateUUIDv7WithCounter(1);

SELECT generateUUIDv7WithFastCounter(1) = generateUUIDv7WithFastCounter(2);

SELECT generateUUIDv7WithFastCounter() = generateUUIDv7WithFastCounter(1);

SELECT generateUUIDv7WithFastCounter(1) = generateUUIDv7WithFastCounter(1);
