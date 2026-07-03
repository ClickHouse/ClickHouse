-- Regression test for the analyzer function-resolution cache.
-- getSetting and getSettingOrDefault read the current settings, which can differ between
-- scopes. Sibling subqueries with different SETTINGS and identical calls must each observe
-- their own scope's setting value and must not reuse a cached function base from the first branch.

SELECT v FROM
(
    (SELECT getSettingOrDefault('max_block_size', 0::UInt64) AS v SETTINGS max_block_size = 111)
    UNION ALL
    (SELECT getSettingOrDefault('max_block_size', 0::UInt64) AS v SETTINGS max_block_size = 222)
)
ORDER BY v;

SELECT v FROM
(
    (SELECT getSetting('max_block_size') AS v SETTINGS max_block_size = 333)
    UNION ALL
    (SELECT getSetting('max_block_size') AS v SETTINGS max_block_size = 444)
)
ORDER BY v;
