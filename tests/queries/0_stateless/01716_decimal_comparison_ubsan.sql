SET decimal_check_overflow = 0;
SELECT toDecimal64(0, 8) = 9223372036854775807;
