SELECT dummy FROM (SELECT dummy, NOT dummy AS x FROM remote('127.0.0.{1,2}', system.one) GROUP BY dummy HAVING x);
