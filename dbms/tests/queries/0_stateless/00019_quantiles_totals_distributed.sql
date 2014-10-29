SELECT quantilesTiming(0.1, 0.5, 0.9)(dummy) FROM remote('127.0.0.{1,2}', system, one) GROUP BY 1 WITH TOTALS
