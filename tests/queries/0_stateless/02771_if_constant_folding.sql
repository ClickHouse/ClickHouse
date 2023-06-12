SELECT cast(number, if(1 = 1, 'UInt64', toString(number))) FROM numbers(5);
