SELECT dumpColumnStructure(s), s
FROM values('s String', 'ok')
WHERE s = CAST('ok', 'Enum8(\'ok\' = 1)')
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT dumpColumnStructure(t), t.1
FROM values('t Tuple(String)', tuple('ok'))
WHERE t = tuple(CAST('ok', 'Enum8(\'ok\' = 1)'))
SETTINGS optimize_constant_columns_after_filter = 1;
