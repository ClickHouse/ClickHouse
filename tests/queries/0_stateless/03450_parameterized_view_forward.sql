DROP TABLE IF EXISTS inner_view, outer_view_hardcoded_ok, outer_view_parameterized_ko;

CREATE VIEW inner_view AS
    SELECT {inner_a:Int32} + {inner_b:Int32} `n`;

CREATE VIEW outer_view_hardcoded_ok AS
    SELECT n * 2 `c`
    FROM inner_view(inner_a=1, inner_b=2);

SELECT * FROM outer_view_hardcoded_ok;

CREATE VIEW outer_view_parameterized_ko AS
SELECT n * 2 `c`
FROM inner_view(inner_a={a:Int32}, inner_b={b:Int32});

SELECT * FROM outer_view_parameterized_ko(a=1, b=2);

DROP TABLE inner_view, outer_view_hardcoded_ok, outer_view_parameterized_ko;
