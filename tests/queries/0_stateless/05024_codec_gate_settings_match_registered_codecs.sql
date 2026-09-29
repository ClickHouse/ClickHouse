-- Tags: no-fasttest
-- no-fasttest: `enable_sz3_codec` has no registered codec when the server is built without the sz3 library.

-- Every non-obsolete builtin `enable_<family>_codec` setting must gate a registered codec of the same tier.
SELECT s.name, s.tier, c.name, c.tier
FROM system.settings AS s
LEFT JOIN system.codecs AS c ON lower(c.name) = extract(s.name, '^enable_(.+)_codec$')
WHERE match(s.name, '^enable_.+_codec$') AND s.tier != 'Obsolete' AND (c.name = '' OR c.tier != s.tier);
