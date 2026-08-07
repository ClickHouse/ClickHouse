SELECT length(topKWeighted(2, -9223372036854775808)(number, 1025)) FROM system.numbers; -- { serverError ARGUMENT_OUT_OF_BOUND }
