drop table if exists view_bug_const;
CREATE VIEW view_bug_const AS SELECT 'World' AS hello FROM (SELECT number FROM system.numbers LIMIT 1) AS n1 JOIN (SELECT number FROM system.numbers LIMIT 1) AS n2 USING (number);
select * from view_bug_const;
drop table if exists view_bug_const;
