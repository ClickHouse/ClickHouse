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

select log1p(-1) = -inf;
select log1p(0) = 0;
select abs(log1p(exp(2) - 1) - 2) < 1e8;
select abs(log1p(exp(3) - 1) - 3) < 1e8;
select sum(abs(log1p(exp(x) - 1) - x) < 1e-8) / count() from system.one array join range(100) as x;

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

select atan2(0, 1) = 0;
select atan2(0, 2) = 0;
select atan2(1, 0) = pi() / 2;
select atan2(1, 1) = pi() / 4;
select atan2(-1, -1) = -3 * pi() / 4;

select hypot(0, 1) = 1;
select hypot(1, 0) = 1;
select hypot(1, 1) = sqrt(2);
select hypot(-1, 1) = sqrt(2);
select hypot(3, 4) = 5;

select sinh(0) = 0;
select sinh(1) = -sinh(-1);
select abs(sinh(1) - 0.5 * (e() - exp(-1))) < 1e-6;
select abs(sinh(2) - 0.5 * (exp(2) - exp(-2))) < 1e-6;
select sum(abs(sinh(x) - 0.5 * (exp(x) - exp(-x))) < 1e-6) / count() from system.one array join range(10) as x;

select cosh(0) = 1;
select cosh(1) = cosh(-1);
select abs(cosh(1) - 0.5 * (e() + exp(-1))) < 1e-6;
select abs(pow(cosh(1), 2) - pow(sinh(1), 2) - 1) < 1e-6;
select sum(abs(cosh(x) * cosh(x) - sinh(x) * sinh(x) - 1) < 1e-6) / count() from system.one array join range(10) as x;

select asinh(0) = 0;
select asinh(1) = -asinh(-1);
select abs(asinh(1) - ln(1 + sqrt(2))) < 1e-9;
select abs(asinh(sinh(1)) - 1) < 1e-9;
select sum(abs(asinh(sinh(x)) - x) < 1e-9) / count() from system.one array join range(100) as x;

select acosh(1) = 0;
select abs(acosh(2) - ln(2 + sqrt(3))) < 1e-9;
select abs(acosh(cosh(2)) - 2) < 1e-9;
select abs(acosh(cosh(3)) - 3) < 1e-9;
select sum(abs(acosh(cosh(x)) - x) < 1e-9) / count() from system.one array join range(1, 101) as x;

select atanh(0) = 0;
select atanh(0.5) = -atanh(-0.5);
select abs(atanh(0.9) - 0.5 * ln(19)) < 1e-5;
select abs(atanh(tanh(1)) - 1) < 1e-5;
select sum(abs(atanh(tanh(x)) - x) < 1e-5) / count() from system.one array join range(10) as x;

select erf(0) = 0;
select erf(-10) = -1;
select erf(10) = 1;

select erfc(0) = 1;
select erfc(-10) = 2;
select erfc(28) = 0;
