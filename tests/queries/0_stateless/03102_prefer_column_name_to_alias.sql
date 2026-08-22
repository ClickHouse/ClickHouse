-- https://github.com/ClickHouse/ClickHouse/issues/54954

DROP TABLE IF EXISTS loans;

CREATE TABLE loans (loan_number int, security_id text) ENGINE=Memory;

SET enable_analyzer=1;

INSERT INTO loans VALUES (1, 'AAA');
INSERT INTO loans VALUES (1, 'AAA');
INSERT INTO loans VALUES (1, 'AAA');
INSERT INTO loans VALUES (1, 'AAA');
INSERT INTO loans VALUES (1, 'AAA');
INSERT INTO loans VALUES (1, 'BBB');
INSERT INTO loans VALUES (1, 'BBB');
INSERT INTO loans VALUES (1, 'BBB');
INSERT INTO loans VALUES (1, 'BBB');
INSERT INTO loans VALUES (1, 'BBB');
INSERT INTO loans VALUES (1, 'BBB');


with block_0 as (
  select * from loans
),
block_1 as (
  select sum(loan_number) as loan_number from block_0 group by security_id
)
select loan_number from block_1 where loan_number > 3 order by loan_number settings prefer_column_name_to_alias = 1;

DROP TABLE IF EXISTS loans;
