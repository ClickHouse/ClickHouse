SELECT * FROM format(Native, '\x01\x01\x01x\x0CArray(UInt8)\x01\x00\xBD\xEF\xBF\xBD\xEF\xBF\xBD\xEF'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
