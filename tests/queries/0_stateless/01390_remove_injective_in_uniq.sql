set enable_debug_queries = 1;
set optimize_injective_functions_inside_uniq = 1;

analyze select uniq(x), uniqExact(x), uniqHLL12(x), uniqCombined(x), uniqCombined64(x)
from (select number % 2 as x from numbers(10));

analyze select uniq(x + y), uniqExact(x + y), uniqHLL12(x + y), uniqCombined(x + y), uniqCombined64(x + y)
from (select number % 2 as x, number % 3 y from numbers(10));

analyze select uniq(-x), uniqExact(-x), uniqHLL12(-x), uniqCombined(-x), uniqCombined64(-x)
from (select number % 2 as x from numbers(10));

analyze select uniq(bitNot(x)), uniqExact(bitNot(x)), uniqHLL12(bitNot(x)), uniqCombined(bitNot(x)), uniqCombined64(bitNot(x))
from (select number % 2 as x from numbers(10));

analyze select uniq(bitNot(-x)), uniqExact(bitNot(-x)), uniqHLL12(bitNot(-x)), uniqCombined(bitNot(-x)), uniqCombined64(bitNot(-x))
from (select number % 2 as x from numbers(10));

analyze select uniq(-bitNot(-x)), uniqExact(-bitNot(-x)), uniqHLL12(-bitNot(-x)), uniqCombined(-bitNot(-x)), uniqCombined64(-bitNot(-x))
from (select number % 2 as x from numbers(10));

analyze select count(distinct -bitNot(-x)) from (select number % 2 as x from numbers(10));
analyze select uniq(concatAssumeInjective('x', 'y')) from numbers(10);


set optimize_injective_functions_inside_uniq = 0;

analyze select uniq(x), uniqExact(x), uniqHLL12(x), uniqCombined(x), uniqCombined64(x)
from (select number % 2 as x from numbers(10));

analyze select uniq(x + y), uniqExact(x + y), uniqHLL12(x + y), uniqCombined(x + y), uniqCombined64(x + y)
from (select number % 2 as x, number % 3 y from numbers(10));

analyze select uniq(-x), uniqExact(-x), uniqHLL12(-x), uniqCombined(-x), uniqCombined64(-x)
from (select number % 2 as x from numbers(10));

analyze select uniq(bitNot(x)), uniqExact(bitNot(x)), uniqHLL12(bitNot(x)), uniqCombined(bitNot(x)), uniqCombined64(bitNot(x))
from (select number % 2 as x from numbers(10));

analyze select uniq(bitNot(-x)), uniqExact(bitNot(-x)), uniqHLL12(bitNot(-x)), uniqCombined(bitNot(-x)), uniqCombined64(bitNot(-x))
from (select number % 2 as x from numbers(10));

analyze select uniq(-bitNot(-x)), uniqExact(-bitNot(-x)), uniqHLL12(-bitNot(-x)), uniqCombined(-bitNot(-x)), uniqCombined64(-bitNot(-x))
from (select number % 2 as x from numbers(10));

analyze select count(distinct -bitNot(-x)) from (select number % 2 as x from numbers(10));
analyze select uniq(concatAssumeInjective('x', 'y')) from numbers(10);
