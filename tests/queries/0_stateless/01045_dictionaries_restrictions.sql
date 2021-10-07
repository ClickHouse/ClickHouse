DROP DATABASE IF EXISTS dictdb_01045;

CREATE DATABASE dictdb_01045;

CREATE DICTIONARY dictdb_01045.restricted_dict (
  key UInt64,
  value String
)
PRIMARY KEY key
SOURCE(EXECUTABLE(COMMAND 'echo -E "1\thello"' FORMAT TabSeparated))
LIFETIME(MIN 0 MAX 1)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

-- because of lazy load we can check only in dictGet query
select dictGetString('dictdb_01045.restricted_dict', 'value', toUInt64(1));  -- {serverError 482}

select 'Ok.';

DROP DICTIONARY IF EXISTS dictdb_01045.restricted_dict;

DROP DATABASE IF EXISTS dictdb_01045;
