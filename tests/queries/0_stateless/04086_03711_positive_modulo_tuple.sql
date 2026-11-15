-- Test for positiveModulo on tuples: should return modulo results, not division results
-- See https://github.com/ClickHouse/ClickHouse/issues/101699

-- Basic positive modulo on tuple
SELECT 'positiveModulo on tuple';
SELECT positiveModulo((10, 20, 30), 7);

-- Compare with regular modulo (should give same result for positive values)
SELECT 'modulo on tuple';
SELECT modulo((10, 20, 30), 7);

-- Negative values: positiveModulo should return non-negative results
SELECT 'positiveModulo negative values';
SELECT positiveModulo((-3, -10, -17), 7);

-- Regular modulo on negative values (returns negative)
SELECT 'modulo negative values';
SELECT modulo((-3, -10, -17), 7);

-- Verify tuplePositiveModuloByNumber works directly
SELECT 'tuplePositiveModuloByNumber';
SELECT tuplePositiveModuloByNumber((-3, -10, -17), 7);
