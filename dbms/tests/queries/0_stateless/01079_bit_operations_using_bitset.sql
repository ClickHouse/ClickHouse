SELECT IPv6NumToString(bitAnd(IPv6StringToNum('2001:0db8:85a3:8d3a:b2da:8a2e:0370:7334'), IPv6StringToNum('ffff:ffff:ffff:0000:0000:0000:0000:0000'))) FROM system.numbers LIMIT 10;

SELECT IPv6NumToString(bitAnd(materialize(IPv6StringToNum('2001:0db8:85a3:8d3a:b2da:8a2e:0370:7334')), IPv6StringToNum('ffff:ffff:ffff:0000:0000:0000:0000:0000'))) FROM system.numbers LIMIT 10;

SELECT IPv6NumToString(bitAnd(IPv6StringToNum('2001:0db8:85a3:8d3a:b2da:8a2e:0370:7334'), materialize(IPv6StringToNum('ffff:ffff:ffff:0000:0000:0000:0000:0000')))) FROM system.numbers LIMIT 10;

SELECT IPv6NumToString(bitAnd(IPv6StringToNum('2001:0db8:85a3:8d3a:b2da:8a2e:0370:7334'), materialize(IPv6StringToNum('ffff:ffff:ffff:0000:0000:0000:0000:0000')))) FROM system.numbers LIMIT 10;
