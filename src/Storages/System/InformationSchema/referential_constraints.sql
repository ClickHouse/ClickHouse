ATTACH VIEW referential_constraints
    (
     `constraint_catalog` String,
     `constraint_schema` String,
     `constraint_name` Nullable(String),
     `unique_constraint_catalog` String,
     `unique_constraint_schema` String,
     `unique_constraint_name` Nullable(String),
     `match_option` String,
     `update_rule` String,
     `delete_rule` String,
     `table_name` String,
     `referenced_table_name` String,
     `CONSTRAINT_CATALOG` String ALIAS constraint_catalog,
     `CONSTRAINT_SCHEMA` String ALIAS constraint_schema,
     `CONSTRAINT_NAME` Nullable(String) ALIAS constraint_name,
     `UNIQUE_CONSTRAINT_CATALOG` String ALIAS unqiue_constraint_catalog,
     `UNIQUE_CONSTRAINT_SCHEMA` String ALIAS unqiue_constraint_schema,
     `UNIQUE_CONSTRAINT_NAME` Nullable(String) ALIAS unqiue_constraint_name,
     `MATCH_OPTION` String ALIAS match_option,
     `UPDATE_RULE` String ALIAS update_rule,
     `DELETE_RULE` String ALIAS delete_rule
     `TABLE_NAME` String ALIAS table_name,
     `REFERENCED_TABLE_NAME` String ALIAS referenced_table_name
) AS
SELECT ''   AS `constraint_catalog`,
       NULL AS `constraint_name`,
       ''   AS `constraint_schema`,
       ''   AS `unique_constraint_catalog`,
       NULL AS `unique_constraint_name`,
       ''   AS `unique_constraint_schema`,
       ''   AS `match_option`,
       ''   AS `update_rule`,
       ''   AS `delete_rule`
       ''   AS `table_name`,
       ''   AS `referenced_table_name`
WHERE false; -- make sure this view is always empty
