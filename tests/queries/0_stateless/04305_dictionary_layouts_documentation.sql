-- Dictionary layouts expose embedded documentation via system.dictionary_layouts.

-- Every registered layout must have a non-empty description and syntax.
SELECT count() > 15 AS has_layouts, countIf(length(description) = 0) AS undocumented, countIf(length(syntax) = 0) AS no_syntax
FROM system.dictionary_layouts;

-- Representative layouts and their complex-key flag.
SELECT name, is_complex, length(description) > 0 AS has_description
FROM system.dictionary_layouts
WHERE name IN ('flat', 'hashed', 'complex_key_hashed', 'cache', 'ip_trie', 'polygon')
ORDER BY name;

-- Related layouts are exposed as an array.
SELECT related
FROM system.dictionary_layouts
WHERE name = 'hashed';
