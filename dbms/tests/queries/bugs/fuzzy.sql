SELECT globalNotIn(['"wh'], [NULL]);
SELECT globalIn([''], [NULL])
SELECT notIn([['']], [[NULL]]);

SELECT ( SELECT toDecimal128([], rowNumberInBlock()) ) , lcm('', [[(CAST(('>A') AS String))]]);
SELECT truncate(895, -16);
