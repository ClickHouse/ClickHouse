SELECT DISTINCT number FROM remote('127.0.0.{2,3}', system.numbers) LIMIT 10
