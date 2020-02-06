SELECT arrayReduce('aggThrow(0.0001)', range(number % 10)) FROM system.numbers FORMAT Null; -- { serverError 503 }
