-- The `library` dictionary source was removed, so it is no longer a known source type.
DROP DICTIONARY IF EXISTS dict_with_library_source;

CREATE DICTIONARY dict_with_library_source (id UInt64, value String)
PRIMARY KEY id
SOURCE(LIBRARY(PATH '/dev/null'))
LAYOUT(FLAT())
LIFETIME(0); -- { serverError UNKNOWN_ELEMENT_IN_CONFIG }

-- The `dictionaries_lib_path` server setting is obsolete now.
SELECT name, is_obsolete FROM system.server_settings WHERE name = 'dictionaries_lib_path';
