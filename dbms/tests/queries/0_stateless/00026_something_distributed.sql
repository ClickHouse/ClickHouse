SELECT NOT dummy FROM remote('localhost,127.0.0.{1,2}', system, one) WHERE NOT dummy
