SELECT quantileDeterministic(number, sipHash64(number)) FROM remote('127.0.0.{1,2}', numbers(8193));
