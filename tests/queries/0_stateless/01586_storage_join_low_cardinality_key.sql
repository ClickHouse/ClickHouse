CREATE TABLE low_card
(
    `lc` LowCardinality(String)
)
ENGINE = Join(ANY, LEFT, lc);

INSERT INTO low_card VALUES ( '1' );

SELECT * FROM low_card;
SELECT * FROM low_card WHERE lc = '1';
SELECT CAST(lc AS String) FROM low_card;

DROP TABLE low_card;
