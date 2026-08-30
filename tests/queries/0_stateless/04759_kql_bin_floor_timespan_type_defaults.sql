-- Regressions for `bin` flooring negative integers, the `timespan` scalar type being an
-- `Interval`, literal-only function parameter defaults, and rebinding a parameterless
-- tabular function without parentheses.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- bin rounds an integer down, not toward zero --';
print bin(-4, 5);         // -5
print bin(4, 5);          // 0
print bin(-15, 5);        // -15, an exact multiple needs no adjustment
print bin(-1, 9223372036854775807);  // a huge bin size must not overflow the adjustment
print bin(5, 9223372036854775807);   // 0

print '-- bin_at inherits the floor through its delegate --';
print bin_at(-4, 5, 0);   // -5
print bin_at(-1, 5, 3);   // -2
print bin_at(6.5, 2.5, -0.5);  // 4.5

print '-- a timespan column is an interval, not a bare number --';
SET interval_output_format = 'kusto';
datatable (t:timespan) [1d, 30m] | project t;
datatable (t:timespan) [1d] | project d = datetime(2017-01-01) + t;
print isnull(timespan(null));
SET interval_output_format = 'numeric';

print '-- a parameter default must be a literal --';
let f = (a:long = 1 + 2) { a }; print f();   -- { clientError SYNTAX_ERROR }
let g = (a:long = xyz) { a }; print g();   -- { clientError SYNTAX_ERROR }
let h = (a:long = -2) { a };
print h();                // -2, a signed literal is a literal
let s = (a:timespan = 1h) { a };
print s() == 1h;          // true

print '-- a parameterless tabular function may be rebound without parentheses --';
let Numbers = () { datatable (n:long) [1, 2, 3] };
let T = Numbers;
T | count;

SET dialect = 'clickhouse';
