-- the behaviour on overflow can be implementation specific
-- and we don't care about the results, but no buffer overflow should be possible.
SELECT length(h3kRing(9223372036854775807, 1000)) FORMAT Null;
SELECT h3kRing(toUInt64(0xFFFFFFFF), 1000) FORMAT Null;
SELECT h3kRing(0xFFFFFFFFF, 1000) FORMAT Null;
SELECT h3kRing(0xFFFFFFFFFFFFFF, 1000) FORMAT Null;
SELECT h3GetBaseCell(0xFFFFFFFFFFFFFF) FORMAT Null;
SELECT h3GetResolution(0xFFFFFFFFFFFFFF) FORMAT Null;
SELECT h3kRing(0xFFFFFFFFFFFFFF, 10) FORMAT Null;
