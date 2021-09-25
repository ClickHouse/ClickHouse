select bin('');
select bin(0);
select bin(1);
select bin(10);
select bin(127);
select bin(255);
select bin(256);
select bin(511);
select bin(512);
select bin('0');
select bin('10');
select bin('测试');
select bin(toFixedString('测试', 10));
select bin(toFloat32(1.2));
select bin(toFloat64(1.2));
select bin(toDecimal32(1.2, 8));
select bin(toDecimal64(1.2, 17));
select bin('12332424');
select bin(materialize('12332424'));
select bin(toNullable(materialize('12332424')));
select bin(toLowCardinality(materialize('12332424')));

select unbin('');
select unbin('0') == '\0';
select unbin('00110000'); -- 0
select unbin('0011000100110000'); -- 10
select unbin('111001101011010110001011111010001010111110010101'); -- 测试
select unbin(materialize('00110000'));
select unbin(toNullable(materialize('00110000')));
select unbin(toLowCardinality(materialize('00110000')));

select unbin(bin('')) == '';
select bin(unbin('')) == '';
select bin(unbin('0')) == '00000000';

-- hex and bin consistent for corner cases
select hex('') == bin('');
select unhex('') == unbin('');
select unhex('0') == unbin('0');

-- hex and bin support AggregateFunction
select hex(sumState(number)) == hex(toString(sumState(number))) from numbers(10);
select hex(avgState(number)) == hex(toString(avgState(number))) from numbers(99);
select hex(avgState(number)) from numbers(10);
select bin(avgState(number)) from numbers(10);
