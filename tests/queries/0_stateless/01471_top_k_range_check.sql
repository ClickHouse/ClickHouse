SELECT length(topKWeighted(2, -9223372036854775808)(number, 1025)) FROM system.numbers; -- { serverError 69 }
