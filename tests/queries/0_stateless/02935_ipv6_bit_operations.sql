WITH toIPv6('FFFF:0000:FFFF:0000:FFFF:0000:FFFF:0000') AS ip1, toIPv6('0000:FFFF:0000:FFFF:0000:FFFF:0000:FFFF') AS ip2,
     CAST('226854911280625642308916404954512140970', 'UInt128') AS n1, CAST('113427455640312821154458202477256070485', 'UInt128') AS n2
SELECT bin(ip1), bin(ip2), bin(n1), bin(n2),
       bin(bitAnd(ip1, n1)), bin(bitAnd(n1, ip1)), bin(bitAnd(ip2, n1)), bin(bitAnd(n1, ip2)),
       bin(bitAnd(ip1, n2)), bin(bitAnd(n2, ip1)), bin(bitAnd(ip2, n2)), bin(bitAnd(n2, ip2)),
       bin(bitOr(ip1, n1)), bin(bitOr(n1, ip1)), bin(bitOr(ip2, n1)), bin(bitOr(n1, ip2)),
       bin(bitOr(ip1, n2)), bin(bitOr(n2, ip1)), bin(bitOr(ip2, n2)), bin(bitOr(n2, ip2));
