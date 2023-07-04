desc format(JSONEachRow, '{"x" : [[42, null], [24, null]]}');
desc format(JSONEachRow, '{"x" : [[[42, null], []], 24]}');
desc format(JSONEachRow, '{"x" : {"key" : [42, null]}}');

