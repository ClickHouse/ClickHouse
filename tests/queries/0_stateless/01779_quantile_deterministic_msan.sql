SELECT cityHash64(toString(quantileDeterministicState(number, sipHash64(number)))) FROM numbers(8193);
