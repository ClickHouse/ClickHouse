SELECT 'Const Offset';
select 1 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 2 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 3 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 4 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 5 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 6 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 7 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 8 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 9 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select 10 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select 11 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select 12 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select 13 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select 14 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select 15 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select 16 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));

select -1 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -2 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -3 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -4 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -5 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -6 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -7 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -8 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -9 as offset, 'Hello' as s,  subString(bin(s), offset), bin(bitSlice(s, offset));
select -10 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select -11 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select -12 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select -13 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select -14 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select -15 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select -16 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));


SELECT 'Const Truncate Offset';
select 41 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));
select -41 as offset, 'Hello' as s, subString(bin(s), offset), bin(bitSlice(s, offset));

SELECT 'Const Nullable Offset';
select 1 as offset, null as s,          subString(bin(s), offset), bin(bitSlice(s, offset));
select null as offset, 'Hello' as s,    subString(bin(s), offset), bin(bitSlice(s, offset));
select null as offset, null as s,       subString(bin(s), offset), bin(bitSlice(s, offset));

SELECT 'Const Offset, Const Length';
select 1 as offset, 1 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 2 as offset, 2 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 3 as offset, 3 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 4 as offset, 4 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 5 as offset, 5 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 6 as offset, 6 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 7 as offset, 7 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 8 as offset, 8 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 9 as offset, 9 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 10 as offset, 10 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 11 as offset, 11 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 12 as offset, 12 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 13 as offset, 13 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 14 as offset, 14 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 15 as offset, 15 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 16 as offset, 16 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));

select 1 as offset, -1 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 2 as offset, -2 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 3 as offset, -3 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 4 as offset, -4 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 5 as offset, -5 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 6 as offset, -6 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 7 as offset, -7 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 8 as offset, -8 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 9 as offset, -9 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 10 as offset, -10 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 11 as offset, -11 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 12 as offset, -12 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 13 as offset, -13 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 14 as offset, -14 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 15 as offset, -15 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 16 as offset, -16 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));

select -1 as offset, 1 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -2 as offset, 2 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -3 as offset, 3 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -4 as offset, 4 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -5 as offset, 5 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -6 as offset, 6 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -7 as offset, 7 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -8 as offset, 8 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -9 as offset, 9 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -10 as offset, 10 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -11 as offset, 11 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -12 as offset, 12 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -13 as offset, 13 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -14 as offset, 14 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -15 as offset, 15 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -16 as offset, 16 as length, 'Hello' as s, subString(bin(s), offset, length), bin(bitSlice(s, offset, length));

select -1 as offset, -16 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -2 as offset, -15 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -3 as offset, -14 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -4 as offset, -13 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -5 as offset, -12 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -6 as offset, -11 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -7 as offset, -10 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -8 as offset, -9 as length, 'Hello' as s,    subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -9 as offset, -8 as length, 'Hello' as s,    subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -10 as offset, -7 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -11 as offset, -6 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -12 as offset, -5 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -13 as offset, -4 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -14 as offset, -3 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -15 as offset, -2 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -16 as offset, -1 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));

select 'Const Truncate Offset, Const Truncate Length';
select 36 as offset, 8 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 41 as offset, 1 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -44 as offset, -36 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -41 as offset, -40 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select -41 as offset,  41 as length, 'Hello' as s,  subString(bin(s), offset, length), bin(bitSlice(s, offset, length));

select 'Const Nullable Offset, Const Nullable Length';
select 1 as offset, 1 as length, null as s,         subString(bin(s), offset , length), bin(bitSlice(s, offset, length));
select null as offset, 1 as length, 'Hello' as s,   subString(bin(s), offset, length), bin(bitSlice(s, offset, length));
select 1 as offset, null as length, 'Hello' as s,   subString(bin(s), offset , length), bin(bitSlice(s, offset, length));
select null as offset, null as length, null as s,   subString(bin(s), offset , length), bin(bitSlice(s, offset, length));

select 'Dynamic Offset, Dynamic Length';
select number as offset, number as length, 'Hello' as s,        subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(16);
select number as offset, -number as length, 'Hello' as s,       subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(16);
select -number as offset, -16+number as length, 'Hello' as s,   subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(16);
select -number as offset, number as length, 'Hello' as s,        subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(16);

select 'Dynamic Truncate Offset, Dynamic Truncate Length';
select number-8 as offset, 8 as length, 'Hello' as s,        subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(9);
select -4 as offset, number as length, 'Hello' as s,       subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(9);
select -36-number as offset, 8 as length, 'Hello' as s,       subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(9);
select -44 as offset, number as length, 'Hello' as s,       subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(9);
select -44 as offset, number + 40 as length, 'Hello' as s,       subString(bin(s), offset , length), bin(bitSlice(s, offset, length)) from numbers(9);

select 'Dynamic Nullable Offset, Dynamic Nullable Length';
select if(number%4 ==1 or number%8==7, null, number) as offset, if(number%4==2 or number%8==7, null, number) as length,if(number%4 ==3, null, 'Hello') as s,
       subString(bin(s), offset, length), bin(bitSlice(s, offset , length))
from numbers(16);
