SELECT -number % -9223372036854775808 FROM system.numbers; -- { serverError ILLEGAL_DIVISION }
