select tupleElement(arrayJoin(['{"a" : 42}', '{"a.b" : 42}'])::JSON(a UInt64), 'a');

