set enable_analyzer = 1;

with (select randConstant()) as a select a = a;
with (select now() + sleep(1)) as a select a = a;
with (select randConstant()) as b select b = b, a = b, `a=a` from (with (select randConstant()) as a select a, a = a as `a=a`);
