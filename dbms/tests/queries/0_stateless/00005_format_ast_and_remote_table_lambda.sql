SELECT count() FROM remote('127.0.0.{1,2}', system, one) WHERE arrayExists((x) -> x = 1, [1, 2, 3])
