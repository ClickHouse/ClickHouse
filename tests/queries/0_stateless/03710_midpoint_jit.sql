DROP TABLE IF EXISTS midpoint_jit;
CREATE TABLE midpoint_jit
(
    xi Int64,
    yi Int64,
    xf Float64,
    yf Float64
)
ENGINE = Memory;

INSERT INTO midpoint_jit
SELECT
    number,
    number + 1,
    toFloat64(number) + 0.25,
    toFloat64(number) + 0.75,
FROM numbers(1000000);

SELECT 'Int, Int: JIT Off';
SELECT sum(midpoint(midpoint(xi, yi), midpoint(yi, xi))) AS s
FROM midpoint_jit
SETTINGS
    compile_expressions = 0,
    min_count_to_compile_expression = 0,
    log_comment = 'midpoint_jit_int_0';

SELECT 'Int, Int: JIT On';
SELECT sum(midpoint(midpoint(xi, yi), midpoint(yi, xi))) AS s
FROM midpoint_jit
SETTINGS
    compile_expressions = 1,
    min_count_to_compile_expression = 0,
    log_comment = 'midpoint_jit_int_1';

SELECT 'Float, Float: JIT Off';
SELECT sum(midpoint(midpoint(xf, yf), midpoint(yf, xf))) AS s
FROM midpoint_jit
SETTINGS
    compile_expressions = 0,
    min_count_to_compile_expression = 0,
    log_comment = 'midpoint_jit_float_0';

SELECT 'Float, Float: JIT On';
SELECT sum(midpoint(midpoint(xf, yf), midpoint(yf, xf))) AS s
FROM midpoint_jit
SETTINGS
    compile_expressions = 1,
    min_count_to_compile_expression = 0,
    log_comment = 'midpoint_jit_float_1';

TRUNCATE TABLE midpoint_jit;

INSERT INTO midpoint_jit
SELECT
    -(number),
    -(number + 1),
    -(toFloat64(number) + 0.25),
    -(toFloat64(number) + 0.75),
FROM numbers(1000000);

SELECT 'Int, Int: JIT Off';
SELECT sum(midpoint(midpoint(xi, yi), midpoint(yi, xi))) AS s
FROM midpoint_jit
SETTINGS
    compile_expressions = 0,
    min_count_to_compile_expression = 0,
    log_comment = 'neg_midpoint_jit_int_0';

SELECT 'Int, Int: JIT On';
SELECT sum(midpoint(midpoint(xi, yi), midpoint(yi, xi))) AS s
FROM midpoint_jit
SETTINGS
    compile_expressions = 1,
    min_count_to_compile_expression = 0,
    log_comment = 'neg_midpoint_jit_int_1';

SELECT 'Float, Float: JIT Off';
SELECT sum(midpoint(midpoint(xf, yf), midpoint(yf, xf))) AS s
FROM midpoint_jit
SETTINGS
    compile_expressions = 0,
    min_count_to_compile_expression = 0,
    log_comment = 'neg_midpoint_jit_float_0';

SELECT 'Float, Float: JIT On';
SELECT sum(midpoint(midpoint(xf, yf), midpoint(yf, xf))) AS s
FROM midpoint_jit
SETTINGS
    compile_expressions = 1,
    min_count_to_compile_expression = 0,
    log_comment = 'neg_midpoint_jit_float_1';
