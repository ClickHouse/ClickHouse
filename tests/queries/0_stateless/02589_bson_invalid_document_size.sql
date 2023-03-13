select * from format(BSONEachRow, 'x UInt32', x'00000000'); -- {serverError INCORRECT_DATA}

