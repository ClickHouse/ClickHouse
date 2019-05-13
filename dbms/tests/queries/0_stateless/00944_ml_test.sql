CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.defaults;
CREATE TABLE IF NOT EXISTS test.defaults
(
    param1 Float64,
    param2 Float64,
    target Float64,
    predict1 Float64,
    predict2 Float64
) ENGINE = Memory;
insert into test.defaults values (-3.273, -1.452, 4.267, 20.0, 40.0), (0.121, -0.615, 4.290, 20.0, 40.0);

DROP TABLE IF EXISTS test.model;
create table test.model engine = Memory as select LinearRegressionState(0.1, 0.0, 2, 'SGD')(target, param1, param2) as state from test.defaults;

select ans < -61.374 and ans > -61.375 from
(with (select state from remote('127.0.0.1', test.model)) as model select evalMLMethod(model, predict1, predict2) as ans from remote('127.0.0.1', test.defaults));
