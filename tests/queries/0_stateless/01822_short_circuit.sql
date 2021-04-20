set use_short_circuit_function_evaluation = 1;

select if(number >= 0, number * (number + 1) * (number % 2), intDiv(number + 7 * (number + 1), number)) from numbers(10);
select multiIf(number == 0, 0, number == 1, intDiv(1, number), number == 2, intDiv(1, number - 1), number == 3, intDiv(1, number - 2), intDiv(1, number - 3)) from numbers(10);
select number != 0 and intDiv(1, number) == 0 and number != 2 and intDiv(1, number - 2) == 0 from numbers(10);
select number == 0 or intDiv(1, number) != 0 or number == 2 or intDiv(1, number - 2) != 0 from numbers(10);

select count() from (select if(number >= 0, number, sleep(1)) from numbers(10000000));

