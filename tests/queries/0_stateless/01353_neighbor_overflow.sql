SELECT neighbor(toString(number), -9223372036854775808) FROM numbers(100); -- { serverError 69 }
WITH neighbor(toString(number), toInt64(rand64())) AS x SELECT * FROM system.numbers WHERE NOT ignore(x); -- { serverError 69 }
