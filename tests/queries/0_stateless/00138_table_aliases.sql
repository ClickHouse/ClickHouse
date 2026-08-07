SELECT * FROM `system`.`one` AS `xxx`;
SELECT k, s FROM (SELECT 1 AS k FROM `system`.`one`) AS `xxx` ANY LEFT JOIN (SELECT 1 AS k, 'Hello' AS s) AS `yyy` USING k;
