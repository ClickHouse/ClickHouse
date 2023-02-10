-- { echo }
SELECT toUInt32OrDefault(toNullable(toUInt32(1))) SETTINGS cast_keep_nullable=1;
SELECT toUInt32OrDefault(toNullable(toUInt32(1)), toNullable(toUInt32(2))) SETTINGS cast_keep_nullable=1;
SELECT toUInt32OrDefault(toUInt32(1)) SETTINGS cast_keep_nullable=1;
SELECT toUInt32OrDefault(toUInt32(1), toUInt32(2)) SETTINGS cast_keep_nullable=1;
