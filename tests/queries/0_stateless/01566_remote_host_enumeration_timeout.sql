select * from remote('badhost{0..100}', system, one) settings max_execution_time = 1; -- { serverError 159 }
select * from remote('192.0.2.{0..100}', system, one) settings max_execution_time = 1; -- { serverError 159 }
