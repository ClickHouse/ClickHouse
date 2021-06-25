select abs(0) = 0;
select abs(1) = 1;
select abs(1) = 1;
select abs(0.0) = 0;
select abs(1.0) = 1.0;
select abs(-1.0) = 1.0;
select abs(-128) = 128;
select abs(127) = 127;
select sum(abs(number - 10 as x) = (x < 0 ? -x : x)) / count() from system.one array join range(1000000) as number;

select sqrt(0) = 0;
select sqrt(1) = 1;
select sqrt(4) = 2;
select sum(sqrt(x * x) = x) / count() from system.one array join range(1000000) as x;

select cbrt(0) = 0;
select cbrt(1) = 1;
select cbrt(8) = 2;
select sum(abs(cbrt(x * x * x) - x) < 1.0e-9) / count() from system.one array join range(1000000) as x;

select pow(1, 0) = 1;
select pow(2, 0) = 1;
select sum(pow(x, 0) = 1) / count() from system.one array join range(1000000) as x;
select pow(1, 1) = 1;
select pow(2, 1) = 2;
select sum(abs(pow(x, 1) - x) < 1.0e-9) / count() from system.one array join range(1000000) as x;
select sum(pow(x, 2) = x * x) / count() from system.one array join range(10000) as x;

select tgamma(0) = inf;
select tgamma(1) = 1;
select tgamma(2) = 1;
select tgamma(3) = 2;
select tgamma(4) = 6;

select sum(abs(lgamma(x + 1) - log(tgamma(x + 1))) < 1.0e-8) / count() from system.one array join range(10) as x;

select abs(e() - arraySum(arrayMap(x -> 1 / tgamma(x + 1), range(13)))) < 1.0e-9;

select log(0) = -inf;
select log(1) = 0;
select abs(log(e()) - 1) < 1e-8;
select abs(log(exp(1)) - 1) < 1e-8;
select abs(log(exp(2)) - 2) < 1e-8;
select sum(abs(log(exp(x)) - x) < 1e-8) / count() from system.one array join range(100) as x;

select exp2(-1) = 1/2;
select exp2(0) = 1;
select exp2(1) = 2;
select exp2(2) = 4;
select exp2(3) = 8;
select sum(exp2(x) = pow(2, x)) / count() from system.one array join range(1000) as x;

select log2(0) = -inf;
select log2(1) = 0;
select log2(2) = 1;
select log2(4) = 2;
select sum(abs(log2(exp2(x)) - x) < 1.0e-9) / count() from system.one array join range(1000) as x;

select sin(0) = 0;
select sin(pi() / 4) = 1 / sqrt(2);
select sin(pi() / 2) = 1;
select sin(3 * pi() / 2) = -1;
select sum(sin(pi() / 2 + 2 * pi() * x) = 1) / count() from system.one array join range(1000000) as x;

select cos(0) = 1;
select abs(cos(pi() / 4) - 1 / sqrt(2)) < 1.0e-9;
select cos(pi() / 2) < 1.0e-9;
select sum(abs(cos(2 * pi() * x)) - 1 < 1.0e-9) / count() from system.one array join range(1000000) as x;

select tan(0) = 0;
select abs(tan(pi() / 4) - 1) < 1.0e-9;
select sum(abs(tan(pi() / 4 + 2 * pi() * x) - 1) < 1.0e-8) / count() from system.one array join range(1000000) as x;

select asin(0) = 0;
select asin(1) = pi() / 2;
select asin(-1) = -pi() / 2;

select acos(0) = pi() / 2;
select acos(1) = 0;
select acos(-1) = pi();

select atan(0) = 0;
select atan(1) = pi() / 4;

select erf(0) = 0;
select erf(-10) = -1;
select erf(10) = 1;

select erfc(0) = 1;
select erfc(-10) = 2;
select erfc(28) = 0;
