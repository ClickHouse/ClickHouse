-- A setting whose default value is the empty string is documented in `system.documentation` as
-- `**Default:** *empty string*` (italic prose), not as empty backticks `**Default:** ``` `` ``` which
-- would read as if the value were missing. `compatibility` is such a setting (its default is empty).

SELECT description LIKE '%**Default:** *empty string*%'
FROM system.documentation WHERE type = 'Setting' AND name = 'compatibility';

-- The empty-backtick form is no longer produced for this setting's default.
SELECT position(description, '**Default:** ``') = 0
FROM system.documentation WHERE type = 'Setting' AND name = 'compatibility';
