SELECT toUInt32OrZero('123a'), toUInt32OrZero('456');
SELECT toUInt32OrZero(arrayJoin(['123a', '456']));

SELECT toFloat64OrZero('123.456a'), toFloat64OrZero('456.789');
SELECT toFloat64OrZero(arrayJoin(['123.456a', '456.789']));
