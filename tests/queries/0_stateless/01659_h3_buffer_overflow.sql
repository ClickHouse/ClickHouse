-- Tags: no-fasttest

-- the behaviour on overflow can be implementation specific
-- and we don't care about the results, but no buffer overflow should be possible.
SELECT length(h3kRing(9223372036854775807, 1000)) FORMAT Null;
SELECT h3kRing(toUInt64(0xFFFFFFFF), 1000) FORMAT Null;
SELECT h3kRing(0xFFFFFFFFF, 1000) FORMAT Null;
SELECT h3kRing(0xFFFFFFFFFFFFFF, 1000) FORMAT Null;
SELECT h3GetBaseCell(0xFFFFFFFFFFFFFF) FORMAT Null;
SELECT h3GetResolution(0xFFFFFFFFFFFFFF) FORMAT Null;
SELECT h3kRing(0xFFFFFFFFFFFFFF, toUInt16(10)) FORMAT Null;
SELECT h3ToGeo(0xFFFFFFFFFFFFFF) FORMAT Null;
SELECT h3HexRing(0xFFFFFFFFFFFFFF, toUInt16(10)) FORMAT Null; -- { serverError 117 }
SELECT h3HexRing(0xFFFFFFFFFFFFFF, toUInt16(10000)) FORMAT Null; -- { serverError 117 }
SELECT length(h3HexRing(581276613233082367, toUInt16(1))) FORMAT Null;
