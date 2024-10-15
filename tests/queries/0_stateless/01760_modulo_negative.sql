SELECT -number % -9223372036854775808 FROM system.numbers; -- { serverError 153 }
