SELECT quantileDeterministicState(number, 0) FROM numbers(8193); -- { serverError BAD_ARGUMENTS }
