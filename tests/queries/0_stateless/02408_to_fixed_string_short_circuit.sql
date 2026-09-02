select if(number < 0, toFixedString(materialize('123'), 2), NULL) from numbers(2);
