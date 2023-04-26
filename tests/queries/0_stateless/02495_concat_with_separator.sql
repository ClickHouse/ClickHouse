select concatWithSeparator('|', 'a', 'b') == 'a|b';
select concatWithSeparator('|', 'a', materialize('b')) == 'a|b';
select concatWithSeparator('|', materialize('a'), 'b') == 'a|b';
select concatWithSeparator('|', materialize('a'), materialize('b')) == 'a|b';

select concatWithSeparator('|', 'a', toFixedString('b', 1)) == 'a|b';
select concatWithSeparator('|', 'a', materialize(toFixedString('b', 1))) == 'a|b';
select concatWithSeparator('|', materialize('a'), toFixedString('b', 1)) == 'a|b';
select concatWithSeparator('|', materialize('a'), materialize(toFixedString('b', 1))) == 'a|b';

select concatWithSeparator('|', toFixedString('a', 1), 'b') == 'a|b';
select concatWithSeparator('|', toFixedString('a', 1), materialize('b')) == 'a|b';
select concatWithSeparator('|', materialize(toFixedString('a', 1)), 'b') == 'a|b';
select concatWithSeparator('|', materialize(toFixedString('a', 1)), materialize('b')) == 'a|b';

select concatWithSeparator('|', toFixedString('a', 1), toFixedString('b', 1)) == 'a|b';
select concatWithSeparator('|', toFixedString('a', 1), materialize(toFixedString('b', 1))) == 'a|b';
select concatWithSeparator('|', materialize(toFixedString('a', 1)), toFixedString('b', 1)) == 'a|b';
select concatWithSeparator('|', materialize(toFixedString('a', 1)), materialize(toFixedString('b', 1))) == 'a|b';

select concatWithSeparator(null, 'a', 'b') == null;
select concatWithSeparator('1', null, 'b') == null;
select concatWithSeparator('1', 'a', null) == null;

select concatWithSeparator(materialize('|'), 'a', 'b'); -- { serverError 44 }
select concatWithSeparator();                           -- { serverError 42 }
select concatWithSeparator('|', 'a', 100);              -- { serverError 43 }
