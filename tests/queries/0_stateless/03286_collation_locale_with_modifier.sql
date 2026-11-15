-- Tags: no-fasttest
-- no-fasttest: needs ICU

SELECT 'Sort by standard Turkish locale';
SELECT arrayJoin(['kk 50', 'KK 01', '    KK 2', '  KK    3', 'kk 1', 'x9y99', 'x9y100']) item
ORDER BY item ASC COLLATE 'tr';

SELECT 'Sort by Turkish locale with modifiers';
SELECT arrayJoin(['kk 50', 'KK 01', '    KK 2', '  KK    3', 'kk 1', 'x9y99', 'x9y100']) item
ORDER BY item ASC COLLATE 'tr-u-kn-true-ka-shifted';
