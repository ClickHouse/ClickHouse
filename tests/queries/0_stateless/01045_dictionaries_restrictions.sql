
CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.restricted_dict (
  key UInt64,
  value String
)
PRIMARY KEY key
SOURCE(EXECUTABLE(COMMAND 'echo -E "1\thello"' FORMAT TabSeparated))
LIFETIME(MIN 0 MAX 1)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

-- because of lazy load we can check only in dictGet query
select dictGetString({CLICKHOUSE_DATABASE:String} || '.restricted_dict', 'value', toUInt64(1));  -- {serverError DICTIONARY_ACCESS_DENIED}

select 'Ok.';

DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.restricted_dict;

