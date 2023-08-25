ATTACH VIEW referential_constraints
    (
     `constraint_name` Nullable(String),
     `constraint_schema` String,
     `table_name` String,
     `update_rule` String,
     `delete_rule` String,
     `CONSTRAINT_NAME` Nullable(String),
     `CONSTRAINT_SCHEMA` String,
     `TABLE_NAME` String,
     `UPDATE_RULE` String,
     `DELETE_RULE` String
        ) AS
SELECT NULL AS `constraint_name`,
       ''   AS `constraint_schema`,
       ''   AS `table_name`,
       ''   AS `update_rule`,
       ''   AS `delete_rule`,

       NULL AS `CONSTRAINT_NAME`,
       ''   AS `CONSTRAINT_SCHEMA`,
       ''   AS `TABLE_NAME`,
       ''   AS `UPDATE_RULE`,
       ''   AS `DELETE_RULE`
WHERE false;