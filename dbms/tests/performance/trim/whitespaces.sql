CREATE TABLE whitespaces
(
    value String
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY tuple()

INSERT INTO whitespaces SELECT value
FROM
(
    SELECT
        arrayStringConcat(groupArray(' ')) AS spaces,
        concat(spaces, toString(any(number)), spaces) AS value
    FROM numbers(100000000)
    GROUP BY pow(number, intHash32(number) % 4) % 12345678
) -- repeat something like this multiple times and/or just copy whitespaces table into itself
