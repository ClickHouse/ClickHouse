-- Values that compare equal only after an unchecked scaling or unit conversion must not be replaced with a constant.

SELECT 'decimal scale';
SELECT d FROM values('d Decimal64(0)', 0, 70368744177664) WHERE d = toDecimal64('0', 18) ORDER BY d
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 0;
SELECT d FROM values('d Decimal64(0)', 0, 70368744177664) WHERE d = toDecimal64('0', 18) ORDER BY d
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 1;

SELECT 'tuple decimal scale';
SELECT t FROM values('t Tuple(Decimal64(0))', tuple(0), tuple(70368744177664)) WHERE t = tuple(toDecimal64('0', 18)) ORDER BY t
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 0;
SELECT t FROM values('t Tuple(Decimal64(0))', tuple(0), tuple(70368744177664)) WHERE t = tuple(toDecimal64('0', 18)) ORDER BY t
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 1;

SELECT 'integer vs decimal';
SELECT u FROM values('u UInt128', 0, 1298074214633706907132624082305024) WHERE u = toDecimal64('0', 18) ORDER BY u
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 0;
SELECT u FROM values('u UInt128', 0, 1298074214633706907132624082305024) WHERE u = toDecimal64('0', 18) ORDER BY u
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 1;

SELECT 'interval units';
SELECT toInt64(i) AS x FROM values('i IntervalYear', toIntervalYear(0), toIntervalYear(4611686018427387904)) WHERE i = toIntervalMonth(0) ORDER BY x
SETTINGS optimize_constant_columns_after_filter = 0;
SELECT toInt64(i) AS x FROM values('i IntervalYear', toIntervalYear(0), toIntervalYear(4611686018427387904)) WHERE i = toIntervalMonth(0) ORDER BY x
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'integer vs dynamic decimal';
SELECT u FROM values('u UInt64', 0, 70368744177664) WHERE u = CAST(toDecimal64('0', 18), 'Dynamic') ORDER BY u
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 0;
SELECT u FROM values('u UInt64', 0, 70368744177664) WHERE u = CAST(toDecimal64('0', 18), 'Dynamic') ORDER BY u
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 1;

SELECT 'tuple integer vs tuple dynamic decimal';
SELECT t FROM values('t Tuple(UInt64)', tuple(0), tuple(70368744177664)) WHERE t = tuple(CAST(toDecimal64('0', 18), 'Dynamic')) ORDER BY t
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 0;
SELECT t FROM values('t Tuple(UInt64)', tuple(0), tuple(70368744177664)) WHERE t = tuple(CAST(toDecimal64('0', 18), 'Dynamic')) ORDER BY t
SETTINGS decimal_check_overflow = 0, optimize_constant_columns_after_filter = 1;
