SELECT * FROM `system`.`one` AS `xxx`;
SELECT 1 AS k, s FROM `system`.`one` AS `xxx` ANY LEFT JOIN (SELECT 1 AS k, 'Hello' AS s) AS `yyy` USING k;
