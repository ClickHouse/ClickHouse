select [(toUInt8(3), toUInt8(3))] = [(toInt16(3), toInt16(3))];
select hasAny([(toInt16(3), toInt16(3))],[(toInt16(3), toInt16(3))]);
select arrayFilter(x -> x = (toInt16(3), toInt16(3)), arrayZip([toUInt8(3)], [toUInt8(3)]));
select hasAny([(toUInt8(3), toUInt8(3))],[(toInt16(3), toInt16(3))]);
