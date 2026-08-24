SET send_logs_level = 'fatal';

select format('Hello {1} World {0}', materialize('first'), materialize('second')) from system.numbers limit 1;
select format('Hello {0} World {1}', materialize('first'), materialize('second')) from system.numbers limit 2;
select format('Hello {1} World {1}', materialize('first'), materialize('second')) from system.numbers limit 3;
select format('Hello {0} World {0}', materialize('first'), 'second') from system.numbers limit 2;
select format('Hellooooooooooooooooooooooooooooooooooo {0} Wooooooooooooooooooooooorld {0} {2}{2}', materialize('fiiiiiiiiiiirst'), 'second', materialize('third')) from system.numbers limit 2;


select format('{}', 'first');
select format('{}{}', 'first', toFixedString('second', 6));
select format('{{}}', materialize('first'), 'second');
select 50 = length(format((select arrayStringConcat(arrayMap(x ->'{', range(100)))), ''));
select 100 = length(format(concat((select arrayStringConcat(arrayMap(x ->'}', range(100)))), (select arrayStringConcat(arrayMap(x ->'{', range(100))))), ''));

select format('', 'first');
select concat('third', 'first', 'second')=format('{2}{0}{1}', 'first', 'second', 'third');

select format('{', ''); -- { serverError BAD_ARGUMENTS }
select format('{{}', ''); -- { serverError BAD_ARGUMENTS }
select format('{ {}', ''); -- { serverError BAD_ARGUMENTS }
select format('}', ''); -- { serverError BAD_ARGUMENTS }
select format('{{', '');
select format('{}}', ''); -- { serverError BAD_ARGUMENTS }
select format('}}', '');
select format('{2 }', ''); -- { serverError BAD_ARGUMENTS }
select format('{}{}{}{}{}{} }{}', '', '', '', '', '', '', ''); -- { serverError BAD_ARGUMENTS }
select format('{sometext}', ''); -- { serverError BAD_ARGUMENTS }
select format('{\0sometext}', ''); -- { serverError BAD_ARGUMENTS }
select format('{1023}', ''); -- { serverError BAD_ARGUMENTS }
select format('{10000000000000000000000000000000000000000000000000}', ''); -- { serverError BAD_ARGUMENTS }
select format('{} {0}', '', ''); -- { serverError BAD_ARGUMENTS }
select format('{0} {}', '', ''); -- { serverError BAD_ARGUMENTS }
select format('Hello {} World {} {}{}', 'first', 'second', 'third') from system.numbers limit 2; -- { serverError BAD_ARGUMENTS }
select format('Hello {0} World {1} {2}{3}', 'first', 'second', 'third') from system.numbers limit 2; -- { serverError BAD_ARGUMENTS }

select 50 = length(format((select arrayStringConcat(arrayMap(x ->'{', range(101)))), ''));  -- { serverError BAD_ARGUMENTS }

select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), materialize(toFixedString('c', 1))) == 'abc';

select '{{}' == format('{{{}', '{}');

select '{ key: fn, value: concat }' == format('{}{}{}{}{}', '{ key: ', toFixedString('fn', 2), ', value: ', 'concat', ' }');

select format('{}{}', 'a', 'b') == 'ab';
select format('{}{}', 'a', materialize('b')) == 'ab';
select format('{}{}', materialize('a'), 'b') == 'ab';
select format('{}{}', materialize('a'), materialize('b')) == 'ab';

select format('{}{}', 'a', toFixedString('b', 1)) == 'ab';
select format('{}{}', 'a', materialize(toFixedString('b', 1))) == 'ab';
select format('{}{}', materialize('a'), toFixedString('b', 1)) == 'ab';
select format('{}{}', materialize('a'), materialize(toFixedString('b', 1))) == 'ab';

select format('{}{}', toFixedString('a', 1), 'b') == 'ab';
select format('{}{}', toFixedString('a', 1), materialize('b')) == 'ab';
select format('{}{}', materialize(toFixedString('a', 1)), 'b') == 'ab';
select format('{}{}', materialize(toFixedString('a', 1)), materialize('b')) == 'ab';

select format('{}{}', toFixedString('a', 1), toFixedString('b', 1)) == 'ab';
select format('{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1))) == 'ab';
select format('{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1)) == 'ab';
select format('{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1))) == 'ab';

select format('{}{}', 'a', 'b') == 'ab' from system.numbers limit 5;
select format('{}{}', 'a', materialize('b')) == 'ab' from system.numbers limit 5;
select format('{}{}', materialize('a'), 'b') == 'ab' from system.numbers limit 5;
select format('{}{}', materialize('a'), materialize('b')) == 'ab' from system.numbers limit 5;

select format('{}{}', 'a', toFixedString('b', 1)) == 'ab' from system.numbers limit 5;
select format('{}{}', 'a', materialize(toFixedString('b', 1))) == 'ab' from system.numbers limit 5;
select format('{}{}', materialize('a'), toFixedString('b', 1)) == 'ab' from system.numbers limit 5;
select format('{}{}', materialize('a'), materialize(toFixedString('b', 1))) == 'ab' from system.numbers limit 5;

select format('{}{}', toFixedString('a', 1), 'b') == 'ab' from system.numbers limit 5;
select format('{}{}', toFixedString('a', 1), materialize('b')) == 'ab' from system.numbers limit 5;
select format('{}{}', materialize(toFixedString('a', 1)), 'b') == 'ab' from system.numbers limit 5;
select format('{}{}', materialize(toFixedString('a', 1)), materialize('b')) == 'ab' from system.numbers limit 5;

select format('{}{}', toFixedString('a', 1), toFixedString('b', 1)) == 'ab' from system.numbers limit 5;
select format('{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1))) == 'ab' from system.numbers limit 5;
select format('{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1)) == 'ab' from system.numbers limit 5;
select format('{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1))) == 'ab' from system.numbers limit 5;

select format('{}{}{}', 'a', 'b', 'c') == 'abc';
select format('{}{}{}', 'a', 'b', materialize('c')) == 'abc';
select format('{}{}{}', 'a', materialize('b'), 'c') == 'abc';
select format('{}{}{}', 'a', materialize('b'), materialize('c')) == 'abc';
select format('{}{}{}', materialize('a'), 'b', 'c') == 'abc';
select format('{}{}{}', materialize('a'), 'b', materialize('c')) == 'abc';
select format('{}{}{}', materialize('a'), materialize('b'), 'c') == 'abc';
select format('{}{}{}', materialize('a'), materialize('b'), materialize('c')) == 'abc';

select format('{}{}{}', 'a', 'b', toFixedString('c', 1)) == 'abc';
select format('{}{}{}', 'a', 'b', materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', 'a', materialize('b'), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', 'a', materialize('b'), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize('a'), 'b', toFixedString('c', 1)) == 'abc';
select format('{}{}{}', materialize('a'), 'b', materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize('a'), materialize('b'), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', materialize('a'), materialize('b'), materialize(toFixedString('c', 1))) == 'abc';

select format('{}{}{}', 'a', toFixedString('b', 1), 'c') == 'abc';
select format('{}{}{}', 'a', toFixedString('b', 1), materialize('c')) == 'abc';
select format('{}{}{}', 'a', materialize(toFixedString('b', 1)), 'c') == 'abc';
select format('{}{}{}', 'a', materialize(toFixedString('b', 1)), materialize('c')) == 'abc';
select format('{}{}{}', materialize('a'), toFixedString('b', 1), 'c') == 'abc';
select format('{}{}{}', materialize('a'), toFixedString('b', 1), materialize('c')) == 'abc';
select format('{}{}{}', materialize('a'), materialize(toFixedString('b', 1)), 'c') == 'abc';
select format('{}{}{}', materialize('a'), materialize(toFixedString('b', 1)), materialize('c')) == 'abc';

select format('{}{}{}', 'a', toFixedString('b', 1), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', 'a', toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', 'a', materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', 'a', materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize('a'), toFixedString('b', 1), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', materialize('a'), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize('a'), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', materialize('a'), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';

select format('{}{}{}', toFixedString('a', 1), 'b', 'c') == 'abc';
select format('{}{}{}', toFixedString('a', 1), 'b', materialize('c')) == 'abc';
select format('{}{}{}', toFixedString('a', 1), materialize('b'), 'c') == 'abc';
select format('{}{}{}', toFixedString('a', 1), materialize('b'), materialize('c')) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), 'b', 'c') == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), 'b', materialize('c')) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), 'c') == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), materialize('c')) == 'abc';

select format('{}{}{}', toFixedString('a', 1), 'b', toFixedString('c', 1)) == 'abc';
select format('{}{}{}', toFixedString('a', 1), 'b', materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', toFixedString('a', 1), materialize('b'), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', toFixedString('a', 1), materialize('b'), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), 'b', toFixedString('c', 1)) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), 'b', materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), materialize(toFixedString('c', 1))) == 'abc';

select format('{}{}{}', toFixedString('a', 1), toFixedString('b', 1), 'c') == 'abc';
select format('{}{}{}', toFixedString('a', 1), toFixedString('b', 1), materialize('c')) == 'abc';
select format('{}{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1)), 'c') == 'abc';
select format('{}{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1)), materialize('c')) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1), 'c') == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1), materialize('c')) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), 'c') == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize('c')) == 'abc';

select format('{}{}{}', toFixedString('a', 1), toFixedString('b', 1), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', toFixedString('a', 1), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';

select format('{}{}{}', 'a', 'b', 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), 'b', 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;

select format('{}{}{}', 'a', 'b', toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', 'b', materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', materialize('b'), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', materialize('b'), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), 'b', toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), 'b', materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), materialize('b'), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), materialize('b'), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;

select format('{}{}{}', 'a', toFixedString('b', 1), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', toFixedString('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', materialize(toFixedString('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', materialize(toFixedString('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), toFixedString('b', 1), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), toFixedString('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), materialize(toFixedString('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), materialize(toFixedString('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;

select format('{}{}{}', 'a', toFixedString('b', 1), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', 'a', materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), toFixedString('b', 1), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize('a'), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;

select format('{}{}{}', toFixedString('a', 1), 'b', 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), 'b', 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;

select format('{}{}{}', toFixedString('a', 1), 'b', toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), 'b', materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), materialize('b'), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), materialize('b'), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), 'b', toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), 'b', materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;

select format('{}{}{}', toFixedString('a', 1), toFixedString('b', 1), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), toFixedString('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;

select format('{}{}{}', toFixedString('a', 1), toFixedString('b', 1), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', toFixedString('a', 1), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
