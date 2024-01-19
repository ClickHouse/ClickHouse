select '-- Interval --';
select toIntervalNanosecond(1000) as i, toTypeName(i);

select '-- Unary Operations --';
select -toIntervalNanosecond(1000) as i, toTypeName(i);

select sign(toIntervalNanosecond(-1000)) as i, toTypeName(i);

select abs(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitCount(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitNot(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select __bitSwapLastTwo(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select __bitWrapperFunc(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select intExp2(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select intExp10(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select roundAge(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select roundDuration(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select roundToExp2(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select byteSwap(toIntervalNanosecond(-1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select '-- Binary Operations --';
select (toIntervalNanosecond(1000) - toIntervalNanosecond(2000)) as i, toTypeName(i);
select (toIntervalNanosecond(1000) + toIntervalNanosecond(2000)) as i, toTypeName(i);
select (toIntervalNanosecond(2500) / toIntervalNanosecond(1000)) as i, toTypeName(i);
select (toIntervalNanosecond(2500) % toIntervalNanosecond(1000)) as i, toTypeName(i);
select toIntervalNanosecond(1000) / 0;
select toIntervalNanosecond(1000) / toIntervalNanosecond(0);

select (toIntervalNanosecond(2500) * toIntervalNanosecond(1000)) as i, toTypeName(i); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select greatest(toIntervalNanosecond(2500), toIntervalNanosecond(1000)) as i, toTypeName(i);
select intDiv(toIntervalNanosecond(2500), toIntervalNanosecond(1000)) as i, toTypeName(i);
select intDivOrZero(toIntervalNanosecond(2500), toIntervalNanosecond(0)) as i, toTypeName(i);
select least(toIntervalNanosecond(2500), toIntervalNanosecond(1000)) as i, toTypeName(i);
select moduloOrZero(toIntervalNanosecond(2500), toIntervalNanosecond(0)) as i, toTypeName(i);

select bitAnd(toIntervalNanosecond(1000), toIntervalNanosecond(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select __bitBoolMaskAnd(toIntervalNanosecond(1000), toIntervalNanosecond(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select __bitBoolMaskOr(toIntervalNanosecond(1000), toIntervalNanosecond(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitHammingDistance(toIntervalNanosecond(1000), toIntervalNanosecond(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitOr(toIntervalNanosecond(1000), toIntervalNanosecond(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitRotateLeft(toIntervalNanosecond(1000), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitRotateRight(toIntervalNanosecond(1000), 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitShiftLeft(toIntervalNanosecond(1000), 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitShiftRight(toIntervalNanosecond(1000), 4); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitTest(toIntervalNanosecond(1000), 5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select bitXor(toIntervalNanosecond(1000), toIntervalNanosecond(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select '-- Conversion --';
select toFloat64(toIntervalNanosecond(1000));
select toInt64(toIntervalNanosecond(1000));
select toString(toIntervalNanosecond(1000));
