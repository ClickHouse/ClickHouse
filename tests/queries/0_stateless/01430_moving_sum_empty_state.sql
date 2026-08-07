SELECT groupArrayMovingSum(10)(0) FROM remote('127.0.0.{1,2}', numbers(0))
