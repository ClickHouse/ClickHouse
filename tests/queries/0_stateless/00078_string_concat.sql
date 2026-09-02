select '{ key: fn, value: concat }' == concat('{ key: ', toFixedString('fn', 2), ', value: ', 'concat', ' }');

select concat('a', 'b') == 'ab';
select concat('a', materialize('b')) == 'ab';
select concat(materialize('a'), 'b') == 'ab';
select concat(materialize('a'), materialize('b')) == 'ab';

select concat('a', toFixedString('b', 1)) == 'ab';
select concat('a', materialize(toFixedString('b', 1))) == 'ab';
select concat(materialize('a'), toFixedString('b', 1)) == 'ab';
select concat(materialize('a'), materialize(toFixedString('b', 1))) == 'ab';

select concat(toFixedString('a', 1), 'b') == 'ab';
select concat(toFixedString('a', 1), materialize('b')) == 'ab';
select concat(materialize(toFixedString('a', 1)), 'b') == 'ab';
select concat(materialize(toFixedString('a', 1)), materialize('b')) == 'ab';

select concat(toFixedString('a', 1), toFixedString('b', 1)) == 'ab';
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1))) == 'ab';
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1)) == 'ab';
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1))) == 'ab';

select concat('a', 'b') == 'ab' from system.numbers limit 5;
select concat('a', materialize('b')) == 'ab' from system.numbers limit 5;
select concat(materialize('a'), 'b') == 'ab' from system.numbers limit 5;
select concat(materialize('a'), materialize('b')) == 'ab' from system.numbers limit 5;

select concat('a', toFixedString('b', 1)) == 'ab' from system.numbers limit 5;
select concat('a', materialize(toFixedString('b', 1))) == 'ab' from system.numbers limit 5;
select concat(materialize('a'), toFixedString('b', 1)) == 'ab' from system.numbers limit 5;
select concat(materialize('a'), materialize(toFixedString('b', 1))) == 'ab' from system.numbers limit 5;

select concat(toFixedString('a', 1), 'b') == 'ab' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize('b')) == 'ab' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), 'b') == 'ab' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize('b')) == 'ab' from system.numbers limit 5;

select concat(toFixedString('a', 1), toFixedString('b', 1)) == 'ab' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1))) == 'ab' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1)) == 'ab' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1))) == 'ab' from system.numbers limit 5;

select concat('a', 'b', 'c') == 'abc';
select concat('a', 'b', materialize('c')) == 'abc';
select concat('a', materialize('b'), 'c') == 'abc';
select concat('a', materialize('b'), materialize('c')) == 'abc';
select concat(materialize('a'), 'b', 'c') == 'abc';
select concat(materialize('a'), 'b', materialize('c')) == 'abc';
select concat(materialize('a'), materialize('b'), 'c') == 'abc';
select concat(materialize('a'), materialize('b'), materialize('c')) == 'abc';

select concat('a', 'b', toFixedString('c', 1)) == 'abc';
select concat('a', 'b', materialize(toFixedString('c', 1))) == 'abc';
select concat('a', materialize('b'), toFixedString('c', 1)) == 'abc';
select concat('a', materialize('b'), materialize(toFixedString('c', 1))) == 'abc';
select concat(materialize('a'), 'b', toFixedString('c', 1)) == 'abc';
select concat(materialize('a'), 'b', materialize(toFixedString('c', 1))) == 'abc';
select concat(materialize('a'), materialize('b'), toFixedString('c', 1)) == 'abc';
select concat(materialize('a'), materialize('b'), materialize(toFixedString('c', 1))) == 'abc';

select concat('a', toFixedString('b', 1), 'c') == 'abc';
select concat('a', toFixedString('b', 1), materialize('c')) == 'abc';
select concat('a', materialize(toFixedString('b', 1)), 'c') == 'abc';
select concat('a', materialize(toFixedString('b', 1)), materialize('c')) == 'abc';
select concat(materialize('a'), toFixedString('b', 1), 'c') == 'abc';
select concat(materialize('a'), toFixedString('b', 1), materialize('c')) == 'abc';
select concat(materialize('a'), materialize(toFixedString('b', 1)), 'c') == 'abc';
select concat(materialize('a'), materialize(toFixedString('b', 1)), materialize('c')) == 'abc';

select concat('a', toFixedString('b', 1), toFixedString('c', 1)) == 'abc';
select concat('a', toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc';
select concat('a', materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc';
select concat('a', materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';
select concat(materialize('a'), toFixedString('b', 1), toFixedString('c', 1)) == 'abc';
select concat(materialize('a'), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc';
select concat(materialize('a'), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc';
select concat(materialize('a'), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';

select concat(toFixedString('a', 1), 'b', 'c') == 'abc';
select concat(toFixedString('a', 1), 'b', materialize('c')) == 'abc';
select concat(toFixedString('a', 1), materialize('b'), 'c') == 'abc';
select concat(toFixedString('a', 1), materialize('b'), materialize('c')) == 'abc';
select concat(materialize(toFixedString('a', 1)), 'b', 'c') == 'abc';
select concat(materialize(toFixedString('a', 1)), 'b', materialize('c')) == 'abc';
select concat(materialize(toFixedString('a', 1)), materialize('b'), 'c') == 'abc';
select concat(materialize(toFixedString('a', 1)), materialize('b'), materialize('c')) == 'abc';

select concat(toFixedString('a', 1), 'b', toFixedString('c', 1)) == 'abc';
select concat(toFixedString('a', 1), 'b', materialize(toFixedString('c', 1))) == 'abc';
select concat(toFixedString('a', 1), materialize('b'), toFixedString('c', 1)) == 'abc';
select concat(toFixedString('a', 1), materialize('b'), materialize(toFixedString('c', 1))) == 'abc';
select concat(materialize(toFixedString('a', 1)), 'b', toFixedString('c', 1)) == 'abc';
select concat(materialize(toFixedString('a', 1)), 'b', materialize(toFixedString('c', 1))) == 'abc';
select concat(materialize(toFixedString('a', 1)), materialize('b'), toFixedString('c', 1)) == 'abc';
select concat(materialize(toFixedString('a', 1)), materialize('b'), materialize(toFixedString('c', 1))) == 'abc';

select concat(toFixedString('a', 1), toFixedString('b', 1), 'c') == 'abc';
select concat(toFixedString('a', 1), toFixedString('b', 1), materialize('c')) == 'abc';
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1)), 'c') == 'abc';
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1)), materialize('c')) == 'abc';
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1), 'c') == 'abc';
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1), materialize('c')) == 'abc';
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), 'c') == 'abc';
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize('c')) == 'abc';

select concat(toFixedString('a', 1), toFixedString('b', 1), toFixedString('c', 1)) == 'abc';
select concat(toFixedString('a', 1), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc';
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc';
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1), toFixedString('c', 1)) == 'abc';
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc';
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc';
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc';

select concat('a', 'b', 'c') == 'abc' from system.numbers limit 5;
select concat('a', 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select concat('a', materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select concat('a', materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), 'b', 'c') == 'abc' from system.numbers limit 5;
select concat(materialize('a'), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;

select concat('a', 'b', toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat('a', 'b', materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat('a', materialize('b'), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat('a', materialize('b'), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), 'b', toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), 'b', materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize('b'), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize('b'), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;

select concat('a', toFixedString('b', 1), 'c') == 'abc' from system.numbers limit 5;
select concat('a', toFixedString('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select concat('a', materialize(toFixedString('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select concat('a', materialize(toFixedString('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), toFixedString('b', 1), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize('a'), toFixedString('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize(toFixedString('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize(toFixedString('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;

select concat('a', toFixedString('b', 1), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat('a', toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat('a', materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat('a', materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), toFixedString('b', 1), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;

select concat(toFixedString('a', 1), 'b', 'c') == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), 'b', 'c') == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;

select concat(toFixedString('a', 1), 'b', toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), 'b', materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize('b'), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize('b'), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), 'b', toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), 'b', materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize('b'), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize('b'), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;

select concat(toFixedString('a', 1), toFixedString('b', 1), 'c') == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), toFixedString('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;

select concat(toFixedString('a', 1), toFixedString('b', 1), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(toFixedString('a', 1), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), toFixedString('b', 1), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), toFixedString('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1)), materialize(toFixedString('c', 1))) == 'abc' from system.numbers limit 5;
