select (toInt128(-1) + toInt8(1)) x, (toInt256(-1) + toInt8(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toInt16(1)) x, (toInt256(-1) + toInt16(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toInt32(1)) x, (toInt256(-1) + toInt32(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toInt64(1)) x, (toInt256(-1) + toInt64(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toUInt8(1)) x, (toInt256(-1) + toUInt8(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toUInt16(1)) x, (toInt256(-1) + toUInt16(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toUInt32(1)) x, (toInt256(-1) + toUInt32(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toUInt64(1)) x, (toInt256(-1) + toUInt64(1)) y, toTypeName(x), toTypeName(y);

select (toInt128(-1) + toInt128(1)) x, (toInt256(-1) + toInt128(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toInt256(1)) x, (toInt256(-1) + toInt256(1)) y, toTypeName(x), toTypeName(y);
--select (toInt128(-1) + toUInt128(1)) x, (toInt256(-1) + toUInt128(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) + toUInt256(1)) x, (toInt256(-1) + toUInt256(1)) y, toTypeName(x), toTypeName(y);


select (toInt128(-1) - toInt8(1)) x, (toInt256(-1) - toInt8(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toInt16(1)) x, (toInt256(-1) - toInt16(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toInt32(1)) x, (toInt256(-1) - toInt32(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toInt64(1)) x, (toInt256(-1) - toInt64(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toUInt8(1)) x, (toInt256(-1) - toUInt8(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toUInt16(1)) x, (toInt256(-1) - toUInt16(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toUInt32(1)) x, (toInt256(-1) - toUInt32(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toUInt64(1)) x, (toInt256(-1) - toUInt64(1)) y, toTypeName(x), toTypeName(y);

select (toInt128(-1) - toInt128(1)) x, (toInt256(-1) - toInt128(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toInt256(1)) x, (toInt256(-1) - toInt256(1)) y, toTypeName(x), toTypeName(y);
--select (toInt128(-1) - toUInt128(1)) x, (toInt256(-1) - toUInt128(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) - toUInt256(1)) x, (toInt256(-1) - toUInt256(1)) y, toTypeName(x), toTypeName(y);


select (toInt128(-1) * toInt8(1)) x, (toInt256(-1) * toInt8(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toInt16(1)) x, (toInt256(-1) * toInt16(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toInt32(1)) x, (toInt256(-1) * toInt32(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toInt64(1)) x, (toInt256(-1) * toInt64(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toUInt8(1)) x, (toInt256(-1) * toUInt8(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toUInt16(1)) x, (toInt256(-1) * toUInt16(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toUInt32(1)) x, (toInt256(-1) * toUInt32(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toUInt64(1)) x, (toInt256(-1) * toUInt64(1)) y, toTypeName(x), toTypeName(y);

select (toInt128(-1) * toInt128(1)) x, (toInt256(-1) * toInt128(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toInt256(1)) x, (toInt256(-1) * toInt256(1)) y, toTypeName(x), toTypeName(y);
--select (toInt128(-1) * toUInt128(1)) x, (toInt256(-1) * toUInt128(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) * toUInt256(1)) x, (toInt256(-1) * toUInt256(1)) y, toTypeName(x), toTypeName(y);


select intDiv(toInt128(-1), toInt8(-1)) x, intDiv(toInt256(-1), toInt8(-1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toInt16(-1)) x, intDiv(toInt256(-1), toInt16(-1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toInt32(-1)) x, intDiv(toInt256(-1), toInt32(-1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toInt64(-1)) x, intDiv(toInt256(-1), toInt64(-1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toUInt8(1)) x, intDiv(toInt256(-1), toUInt8(1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toUInt16(1)) x, intDiv(toInt256(-1), toUInt16(1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toUInt32(1)) x, intDiv(toInt256(-1), toUInt32(1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toUInt64(1)) x, intDiv(toInt256(-1), toUInt64(1)) y, toTypeName(x), toTypeName(y);

select intDiv(toInt128(-1), toInt128(-1)) x, intDiv(toInt256(-1), toInt128(-1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toInt256(-1)) x, intDiv(toInt256(-1), toInt256(-1)) y, toTypeName(x), toTypeName(y);
--select intDiv(toInt128(-1), toUInt128(1)) x, intDiv(toInt256(-1), toUInt128(1)) y, toTypeName(x), toTypeName(y);
select intDiv(toInt128(-1), toUInt256(1)) x, intDiv(toInt256(-1), toUInt256(1)) y, toTypeName(x), toTypeName(y);


select (toInt128(-1) / toInt8(-1)) x, (toInt256(-1) / toInt8(-1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toInt16(-1)) x, (toInt256(-1) / toInt16(-1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toInt32(-1)) x, (toInt256(-1) / toInt32(-1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toInt64(-1)) x, (toInt256(-1) / toInt64(-1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toUInt8(1)) x, (toInt256(-1) / toUInt8(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toUInt16(1)) x, (toInt256(-1) / toUInt16(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toUInt32(1)) x, (toInt256(-1) / toUInt32(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toUInt64(1)) x, (toInt256(-1) / toUInt64(1)) y, toTypeName(x), toTypeName(y);

select (toInt128(-1) / toInt128(-1)) x, (toInt256(-1) / toInt128(-1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toInt256(-1)) x, (toInt256(-1) / toInt256(-1)) y, toTypeName(x), toTypeName(y);
--select (toInt128(-1) / toUInt128(1)) x, (toInt256(-1) / toUInt128(1)) y, toTypeName(x), toTypeName(y);
select (toInt128(-1) / toUInt256(1)) x, (toInt256(-1) / toUInt256(1)) y, toTypeName(x), toTypeName(y);
