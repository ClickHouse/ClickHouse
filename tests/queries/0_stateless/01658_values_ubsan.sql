SELECT * FROM VALUES('x UInt8, y UInt16', 1 + 2, 'Hello'); -- { serverError BAD_ARGUMENTS }
