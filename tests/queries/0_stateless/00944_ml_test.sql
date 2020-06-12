DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
    param1 Float64,
    param2 Float64,
    target Float64,
    predict1 Float64,
    predict2 Float64
) ENGINE = Memory;
insert into defaults values (-3.273, -1.452, 4.267, 20.0, 40.0), (0.121, -0.615, 4.290, 20.0, 40.0);

DROP TABLE IF EXISTS model;
create table model engine = Memory as select stochasticLinearRegressionState(0.1, 0.0, 2, 'SGD')(target, param1, param2) as state from defaults;

select ans < -61.374 and ans > -61.375 from
(with (select state from remote('127.0.0.1', currentDatabase(), model)) as model select evalMLMethod(model, predict1, predict2) as ans from remote('127.0.0.1', currentDatabase(), defaults));

SELECT 0 < ans[1] and ans[1] < 0.15 and 0.95 < ans[2] and ans[2] < 1.0 and 0 < ans[3] and ans[3] < 0.05 FROM
(SELECT stochasticLinearRegression(0.000001, 0.01, 100, 'SGD')(number, rand() % 100, number) AS ans FROM numbers(1000));

DROP TABLE model;
DROP TABLE defaults;
