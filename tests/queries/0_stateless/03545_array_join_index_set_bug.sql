drop table if exists right;
CREATE TABLE right
(
    `array_in_index` Array(String),
    `array_not_in_index` Array(String),
    `Id` String,
    INDEX index_document_udm_type_names array_in_index TYPE set(100) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY tuple();

insert into right select [''], [''], toString(number) from numbers(1000);

SELECT COUNT() AS x
FROM
(
    SELECT
        array_in_index,
        arrayJoin(array_not_in_index) AS array_not_in_index_joined
    FROM right
) AS T00J0
WHERE T00J0.array_not_in_index_joined = '' AND has(T00J0.array_in_index, 'abc');
