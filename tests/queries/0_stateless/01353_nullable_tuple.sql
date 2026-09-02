select 'single argument';
select tuple(number) = tuple(number) from numbers(1);
select tuple(number) = tuple(number + 1) from numbers(1);
select tuple(toNullable(number)) = tuple(number) from numbers(1);
select tuple(toNullable(number)) = tuple(number + 1) from numbers(1);
select tuple(toNullable(number)) = tuple(toNullable(number)) from numbers(1);
select tuple(toNullable(number)) = tuple(toNullable(number + 1)) from numbers(1);
select '- 1';
select tuple(toNullable(number)) < tuple(number + 1) from numbers(1);
select tuple(number) < tuple(toNullable(number + 1)) from numbers(1);
select tuple(toNullable(number)) < tuple(toNullable(number + 1)) from numbers(1);

select tuple(toNullable(number)) > tuple(number + 1) from numbers(1);
select tuple(number) > tuple(toNullable(number + 1)) from numbers(1);
select tuple(toNullable(number)) > tuple(toNullable(number + 1)) from numbers(1);

select tuple(toNullable(number + 1)) < tuple(number) from numbers(1);
select tuple(number + 1) < tuple(toNullable(number)) from numbers(1);
select tuple(toNullable(number + 1)) < tuple(toNullable(number + 1)) from numbers(1);

select tuple(toNullable(number + 1)) > tuple(number) from numbers(1);
select tuple(number + 1) > tuple(toNullable(number)) from numbers(1);
select tuple(toNullable(number + 1)) > tuple(toNullable(number)) from numbers(1);

select '- 2';
select tuple(toNullable(number)) <= tuple(number + 1) from numbers(1);
select tuple(number) <= tuple(toNullable(number + 1)) from numbers(1);
select tuple(toNullable(number)) <= tuple(toNullable(number + 1)) from numbers(1);

select tuple(toNullable(number)) >= tuple(number + 1) from numbers(1);
select tuple(number) > tuple(toNullable(number + 1)) from numbers(1);
select tuple(toNullable(number)) >= tuple(toNullable(number + 1)) from numbers(1);

select tuple(toNullable(number + 1)) <= tuple(number) from numbers(1);
select tuple(number + 1) <= tuple(toNullable(number)) from numbers(1);
select tuple(toNullable(number + 1)) <= tuple(toNullable(number + 1)) from numbers(1);

select tuple(toNullable(number + 1)) >= tuple(number) from numbers(1);
select tuple(number + 1) >= tuple(toNullable(number)) from numbers(1);
select tuple(toNullable(number + 1)) >= tuple(toNullable(number)) from numbers(1);

select '- 3';
select tuple(toNullable(number)) <= tuple(number) from numbers(1);
select tuple(number) <= tuple(toNullable(number)) from numbers(1);
select tuple(toNullable(number)) <= tuple(toNullable(number)) from numbers(1);

select tuple(toNullable(number)) >= tuple(number) from numbers(1);
select tuple(number) >= tuple(toNullable(number)) from numbers(1);
select tuple(toNullable(number)) >= tuple(toNullable(number)) from numbers(1);

select '- 4';
select tuple(number) = tuple(materialize(toUInt64OrNull(''))) from numbers(1);
select tuple(materialize(toUInt64OrNull(''))) = tuple(materialize(toUInt64OrNull(''))) from numbers(1);
select tuple(number) <= tuple(materialize(toUInt64OrNull(''))) from numbers(1);
select tuple(materialize(toUInt64OrNull(''))) <= tuple(materialize(toUInt64OrNull(''))) from numbers(1);
select tuple(number) >= tuple(materialize(toUInt64OrNull(''))) from numbers(1);
select tuple(materialize(toUInt64OrNull(''))) >= tuple(materialize(toUInt64OrNull(''))) from numbers(1);

select 'two arguments';
select tuple(toNullable(number), number) = tuple(number, number) from numbers(1);
select tuple(toNullable(number), toNullable(number)) = tuple(number, number) from numbers(1);
select tuple(toNullable(number), toNullable(number)) = tuple(toNullable(number), number) from numbers(1);
select tuple(toNullable(number), toNullable(number)) = tuple(toNullable(number), toNullable(number)) from numbers(1);
select tuple(number, toNullable(number)) = tuple(toNullable(number), toNullable(number)) from numbers(1);
select tuple(number, toNullable(number)) = tuple(toNullable(number), number) from numbers(1);

select '- 1';
select tuple(toNullable(number), number) < tuple(number, number) from numbers(1);
select tuple(toNullable(number), toNullable(number)) < tuple(number, number) from numbers(1);
select tuple(toNullable(number), toNullable(number)) < tuple(toNullable(number), number) from numbers(1);
select tuple(toNullable(number), toNullable(number)) < tuple(toNullable(number), toNullable(number)) from numbers(1);
select tuple(number, toNullable(number)) < tuple(toNullable(number), toNullable(number)) from numbers(1);
select tuple(number, toNullable(number)) < tuple(toNullable(number), number) from numbers(1);

select '- 2';
select tuple(toNullable(number), number) < tuple(number, number + 1) from numbers(1);
select tuple(toNullable(number), toNullable(number)) < tuple(number, number + 1) from numbers(1);
select tuple(toNullable(number), toNullable(number)) < tuple(toNullable(number + 1), number) from numbers(1);
select tuple(toNullable(number), toNullable(number)) < tuple(toNullable(number + 1), toNullable(number)) from numbers(1);
select tuple(number, toNullable(number)) < tuple(toNullable(number), toNullable(number + 1)) from numbers(1);
select tuple(number, toNullable(number)) < tuple(toNullable(number), number + 1) from numbers(1);

select '- 3';
select tuple(materialize(toUInt64OrNull('')), number) = tuple(number, number) from numbers(1);
select tuple(materialize(toUInt64OrNull('')), number) = tuple(number, toUInt64OrNull('')) from numbers(1);
select tuple(materialize(toUInt64OrNull('')), toUInt64OrNull('')) = tuple(toUInt64OrNull(''), toUInt64OrNull('')) from numbers(1);
select tuple(number, materialize(toUInt64OrNull(''))) < tuple(number, number) from numbers(1);
select tuple(number, materialize(toUInt64OrNull(''))) <= tuple(number, number) from numbers(1);
select tuple(number, materialize(toUInt64OrNull(''))) < tuple(number + 1, number) from numbers(1);
select tuple(number, materialize(toUInt64OrNull(''))) > tuple(number, number) from numbers(1);
select tuple(number, materialize(toUInt64OrNull(''))) >= tuple(number, number) from numbers(1);
select tuple(number, materialize(toUInt64OrNull(''))) > tuple(number + 1, number) from numbers(1);

select 'many arguments';
select tuple(toNullable(number), number, number) = tuple(number, number, number) from numbers(1);
select tuple(toNullable(number), materialize('a'), number) = tuple(number, materialize('a'), number) from numbers(1);
select tuple(toNullable(number), materialize('a'), number) = tuple(number, materialize('a'), number + 1) from numbers(1);
select tuple(toNullable(number), number, number) < tuple(number, number, number) from numbers(1);
select tuple(toNullable(number), number, number) <= tuple(number, number, number) from numbers(1);
select tuple(toNullable(number), materialize('a'), number) < tuple(number, materialize('a'), number) from numbers(1);
select tuple(toNullable(number), materialize('a'), number) < tuple(number, materialize('a'), number + 1) from numbers(1);
select tuple(toNullable(number), number, materialize(toUInt64OrNull(''))) = tuple(number, number, number) from numbers(1);
select tuple(toNullable(number), materialize('a'), materialize(toUInt64OrNull(''))) = tuple(number, materialize('a'), number) from numbers(1);
select tuple(toNullable(number), materialize('a'), materialize(toUInt64OrNull(''))) = tuple(number, materialize('a'), number + 1) from numbers(1);
select tuple(toNullable(number), number, materialize(toUInt64OrNull(''))) <= tuple(number, number, number) from numbers(1);
select tuple(toNullable(number), materialize('a'), materialize(toUInt64OrNull(''))) <= tuple(number, materialize('a'), number) from numbers(1);
select tuple(toNullable(number), materialize('a'), materialize(toUInt64OrNull(''))) <= tuple(number, materialize('a'), number + 1) from numbers(1);
