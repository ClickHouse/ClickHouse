select -number % 2 as i, toDecimal32(number % 20, 3) as j from numbers(600) order by i, j;
