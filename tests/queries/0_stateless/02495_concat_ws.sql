select concatWs('|', 'a', 'b') == 'a|b';
select concatWs('|', 'a', materialize('b')) == 'a|b';
select concatWs('|', materialize('a'), 'b') == 'a|b';
select concatWs('|', materialize('a'), materialize('b')) == 'a|b';

select concatWs('|', 'a', toFixedString('b', 1)) == 'a|b';
select concatWs('|', 'a', materialize(toFixedString('b', 1))) == 'a|b';
select concatWs('|', materialize('a'), toFixedString('b', 1)) == 'a|b';
select concatWs('|', materialize('a'), materialize(toFixedString('b', 1))) == 'a|b';

select concatWs('|', toFixedString('a', 1), 'b') == 'a|b';
select concatWs('|', toFixedString('a', 1), materialize('b')) == 'a|b';
select concatWs('|', materialize(toFixedString('a', 1)), 'b') == 'a|b';
select concatWs('|', materialize(toFixedString('a', 1)), materialize('b')) == 'a|b';

select concatWs('|', toFixedString('a', 1), toFixedString('b', 1)) == 'a|b';
select concatWs('|', toFixedString('a', 1), materialize(toFixedString('b', 1))) == 'a|b';
select concatWs('|', materialize(toFixedString('a', 1)), toFixedString('b', 1)) == 'a|b';
select concatWs('|', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1))) == 'a|b';

select concatWs(null, 'a', 'b') == null;
select concatWs('1', null, 'b') == null;
select concatWs('1', 'a', null) == null;

select concatWs(materialize('|'), 'a', 'b'); -- { serverError 44 }
select concatWs();                           -- { serverError 42 }
select concatWs('|', 'a', 100);              -- { serverError 43 }
