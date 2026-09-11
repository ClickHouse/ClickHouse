SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

-- KQL `floor(x)` with a single argument: rounds down to the nearest integer
-- (equivalent to `bin(x, 1)`).
print floor(2.7);
print floor(-2.7);
print floor(0);
print floor(0.0);

-- Two-argument form is still supported and matches `bin`.
print floor(2.7, 0.5);
print floor(7, 3);

-- Runtime guard: non-positive bin size returns NULL instead of dividing by zero.
print floor(5.0, 0);
