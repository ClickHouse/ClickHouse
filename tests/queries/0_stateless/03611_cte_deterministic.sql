with (select randConstant()) as a select a = a;
with (select now() + sleep(1)) as a select a = a;
