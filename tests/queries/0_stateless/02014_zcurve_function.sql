select zCurve(toUInt32(1048575)) = 4503595332403200;
select zCurve(nan) = 18444492273895866368;
select zCurve(-1) = 9151314442816847872;

select zCurve(toInt8(0), toInt8(0)) = 13835058055282163712;
select zCurve(toInt8(1), toInt8(1)) = 13835902480212295680;
select zCurve(toInt8(127), toInt8(127)) = 18446462598732840960;
select zCurve(toInt8(127), toInt8(-1)) = 9223090561878065152;
select zCurve(toInt8(-1), toInt8(-1)) = 4611404543450677248;
select zCurve(toInt8(1), toInt16(1)) = 13835339538848808960;
select zCurve(toInt8(1), toInt32(1)) = 13835339530258874370;

select zCurve(toInt16(1), toInt8(1)) = 13835621009530552320;
select zCurve(toInt16(-1), toInt8(1)) = 10761163658185670656;
select zCurve(toInt16(0), toInt16(0)) = 13835058055282163712;
select zCurve(toInt16(1), toInt16(1)) = 13835058068167065600;
select zCurve(toInt16(-1), toInt16(-1)) = 4611686014132420608;
select zCurve(toInt16(32767), toInt16(32767)) = 18446744069414584320;

select zCurve(toInt32(0), toInt32(0)) = 13835058055282163712;
select zCurve(toInt32(1), toInt8(1)) = 13835621005235585025;
select zCurve(toInt32(1), toInt16(1)) = 13835058063872098305;
select zCurve(toInt32(1), toInt32(1)) = 13835058055282163715;
select zCurve(toInt32(-1), toInt32(-1)) = 4611686018427387903;
select zCurve(toInt32(-1), toInt32(1)) = 10760600709663905111;

select zCurve(nan, toInt64(1)) = 15372286636465324032;
select zCurve(0, -1) = 3074269695633784832;
select zCurve(-1, -1) = 4611404543450677248;