SELECT CAST(materialize(CAST(NULL, 'Nullable(Enum(\'A\' = 1, \'B\' = 2))')), 'Nullable(String)');
SELECT CAST(CAST(NULL, 'Nullable(Enum(\'A\' = 1, \'B\' = 2))'), 'Nullable(String)');
SELECT CAST(materialize(CAST(1, 'Nullable(Enum(\'A\' = 1, \'B\' = 2))')), 'Nullable(String)');
SELECT CAST(CAST(1, 'Nullable(Enum(\'A\' = 1, \'B\' = 2))'), 'Nullable(String)');
