select number as x, number + 1 as y from numbers(5) where number % 3 == 1 order by y, x with fill from 1 to 4, y with fill from 2 to 5; -- {serverError INVALID_WITH_FILL_EXPRESSION}

