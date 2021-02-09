-- somehow this one is too fast in release mode
-- select * from remote('badhost{0..200}.test', system, one) settings max_execution_time = 1; -- { serverError 159 }

-- addresses from TEST-NET-1
select * from remote('192.0.2.{0..200}', system, one) settings max_execution_time = 1; -- { serverError 159 }
