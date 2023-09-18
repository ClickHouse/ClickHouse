ATTACH VIEW referential_constraints
    (
     `constraint_name` Nullable(String),
     `constraint_schema` String,
     `table_name` String,
     `update_rule` String,
     `delete_rule` String,
     `CONSTRAINT_NAME` Nullable(String) ALIAS constraint_name,
     `CONSTRAINT_SCHEMA` String ALIAS constraint_schema,
     `TABLE_NAME` String ALIAS table_name,
     `UPDATE_RULE` String ALIAS update_rule,
     `DELETE_RULE` String ALIAS delete_rule
) AS
SELECT NULL AS `constraint_name`,
       ''   AS `constraint_schema`,
       ''   AS `table_name`,
       ''   AS `update_rule`,
       ''   AS `delete_rule`
WHERE false; -- make sure this view is always empty
