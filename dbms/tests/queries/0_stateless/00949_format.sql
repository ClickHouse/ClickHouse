SET send_logs_level = 'none';

select format('Hello {1} World {0}', 'first', 'second') from system.numbers limit 1;
select format('Hello {0} World {1}', 'first', 'second') from system.numbers limit 2;
select format('Hello {1} World {1}', 'first', 'second') from system.numbers limit 3;
select format('Hello {0} World {0}', 'first', 'second') from system.numbers limit 2;
select format('Hellooooooooooooooooooooooooooooooooooo {0} Wooooooooooooooooooooooorld {0} {2}{2}', 'fiiiiiiiiiiirst', 'second', 'third') from system.numbers limit 2;


select format('{}', 'first');
select format('{}{}', 'first', 'second');
select format('{{}}', 'first', 'second');
select 50 = length(format((select arrayStringConcat(arrayMap(x ->'{', range(100)))), ''));
select 100 = length(format(concat((select arrayStringConcat(arrayMap(x ->'}', range(100)))), (select arrayStringConcat(arrayMap(x ->'{', range(100))))), ''));

select format('', 'first');
select concat('third', 'first', 'second')=format('{2}{0}{1}', 'first', 'second', 'third');

select format('{', ''); -- { serverError 49 }
select format('{{}', ''); -- { serverError 49 }
select format('{ {}', ''); -- { serverError 49 }
select format('}', ''); -- { serverError 49 }
select format('{{', '');
select format('{}}', ''); -- { serverError 49 }
select format('}}', '');
select format('{2 }', ''); -- { serverError 49 }
select format('{}{}{}{}{}{} }{}', '', '', '', '', '', '', ''); -- { serverError 49 }
select format('{sometext}', ''); -- { serverError 49 }
select format('{\0sometext}', ''); -- { serverError 49 }
select format('{1023}', ''); -- { serverError 49 }
select format('{10000000000000000000000000000000000000000000000000}', ''); -- { serverError 49 }
select format('{} {0}', '', ''); -- { serverError 49 }
select format('{0} {}', '', ''); -- { serverError 49 }
select format('Hello {} World {} {}{}', 'first', 'second', 'third') from system.numbers limit 2; -- { serverError 49 }
select format('Hello {0} World {1} {2}{3}', 'first', 'second', 'third') from system.numbers limit 2; -- { serverError 49 }

select 50 = length(format((select arrayStringConcat(arrayMap(x ->'{', range(101)))), ''));  -- { serverError 49 }

select format('{}{}{}', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';
select format('{}{}{}', materialize(toFixedString('a', 1)), materialize('b'), materialize(toFixedString('c', 1))) == 'abc';
