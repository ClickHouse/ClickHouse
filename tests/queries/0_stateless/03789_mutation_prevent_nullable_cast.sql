DROP TABLE IF EXISTS warden_a;
CREATE TABLE warden_a
(
    `salary` Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO warden_a
VALUES
    ('1,  2 , 3')
    ('wrong'),
    (NULL);

ALTER TABLE warden_a
ADD COLUMN IF NOT EXISTS `parsed_salary` Array(Int64);
ALTER TABLE
    warden_a
UPDATE
    `parsed_salary` = multiIf(
        salary IS NULL, CAST('[]', 'Array(Int64)'),
        CAST(
            coalesce(
                arrayCompact(
                    arrayMap(
                        x -> toInt64OrNull(trimBoth(x)),
                        splitByString(',', ifNull(salary, ''))
                    )
                ),
                []
            ),
            'Array(Int64)'
        )
    )
WHERE
    1
SETTINGS validate_mutation_query = 1; -- { serverError 349 }

TRUNCATE TABLE warden_a;

ALTER TABLE warden_a
ADD COLUMN IF NOT EXISTS `parsed_salary` Array(Int64);
ALTER TABLE
    warden_a
UPDATE
    `parsed_salary` = multiIf(
        salary IS NULL, CAST('[]', 'Array(Int64)'),
        CAST(
            coalesce(
                arrayCompact(
                    arrayMap(
                        x -> toInt64OrNull(trimBoth(x)),
                        splitByString(',', ifNull(salary, ''))
                    )
                ),
                []
            ),
            'Array(Int64)'
        )
    )
WHERE
    1
SETTINGS validate_mutation_query = 0;
