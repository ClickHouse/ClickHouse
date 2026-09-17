-- Parse, format, reparse, and compare without executing the embedded statements.

WITH formatQuerySingleLine($$create type LocalId as UInt64$$) AS formatted SELECT 'create_unqualified', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$CREATE TYPE IF NOT EXISTS app.UserId ON CLUSTER 'cluster.name' AS UInt64$$) AS formatted SELECT 'create_qualified_if_not_exists', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;

WITH formatQuerySingleLine($$CREATE TYPE app.AllKinds(T TYPE, B Bool, U8 UInt8, U16 UInt16, U32 UInt32, U64 UInt64, I8 Int8, I16 Int16, I32 Int32, I64 Int64, S String) AS app.Bundle(T, B, U8, U16, U32, U64, I8, I16, I32, I64, S)$$) AS formatted SELECT 'template_all_kinds', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$CREATE TYPE app.T(T TYPE) AS T$$) AS formatted SELECT 'template_same_name_formal', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$CREATE TYPE app.Pair(T TYPE) AS Tuple(left T, right T)$$) AS formatted SELECT 'template_type_parameter', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$CREATE TYPE ids.Raw(N UInt16) AS FixedString(N)$$) AS formatted SELECT 'template_value_parameter', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$CREATE TYPE IF NOT EXISTS `analytics.db`.`Nested.Type`(`Type Arg` TYPE, `Depth-Value` UInt16) ON CLUSTER 'cluster.name' DECREASES `Depth-Value` AS TYPE_IF(`Depth-Value` = 0, `Type Arg`, Tuple(head `Type Arg`, tail `Nested.Type`(`Type Arg`, `Depth-Value` - 1))) COMMENT 'owner\'s external\nidentifier'$$) AS formatted SELECT 'template_checked_recursive_quoted', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;

WITH formatQuerySingleLine($$ATTACH TYPE IF NOT EXISTS app.UserId REVISION 7 ON CLUSTER c UUID '01234567-89ab-cdef-0123-456789abcdef' AS UInt64 DEFINITION HASH 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' COMMENT 'restored'$$) AS formatted SELECT 'attach_internal_fields', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$ALTER TYPE IF EXISTS `analytics.db`.`User.Type` ON CLUSTER 'cluster.name' RENAME TO `Renamed.Type`$$) AS formatted SELECT 'alter_rename', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$DROP TYPE IF EXISTS app.PrincipalId ON CLUSTER c$$) AS formatted SELECT 'drop_restrict_canonical', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;

WITH formatQuerySingleLine($$SHOW TYPES LIKE '%Id'$$) AS formatted SELECT 'show_types_like', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$SHOW TYPES FROM `analytics.db` LIKE 'User.%' SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONEachRow$$) AS formatted SELECT 'show_types_output', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$SHOW CREATE TYPE `analytics.db`.`User.Type` INTO OUTFILE 'create.sql' SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONEachRow$$) AS formatted SELECT 'show_create_output', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$DESCRIBE TYPE app.UserId SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONEachRow$$) AS formatted SELECT 'describe_output', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;

WITH formatQuerySingleLine($$PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t ON CLUSTER c DROP UNUSED TYPES DRY RUN SETTINGS max_threads = 1 FORMAT JSONEachRow$$) AS formatted SELECT 'physicalize_object', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$PHYSICALIZE TYPE REFERENCES CLOSURE OF VIEW `analytics.db`.`v.name` DROP UNUSED TYPES DRY RUN$$) AS formatted SELECT 'physicalize_closure', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$PHYSICALIZE TYPE REFERENCES DATABASE app ON CLUSTER c DRY RUN INTO OUTFILE 'plan.json'$$) AS formatted SELECT 'physicalize_database', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'opaque token: owner@example.test/123'$$) AS formatted SELECT 'physicalize_apply_redacted', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;

WITH formatQuerySingleLine($$SELECT CAST(1 AS Array(app.UserId))$$) AS formatted SELECT 'cast_as_nested', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$SELECT CAST(1, 'Array(app.UserId)')$$) AS formatted SELECT 'cast_comma_nested', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$SELECT 1::Array(Tuple(app.UserId, ids.Raw(16)))$$) AS formatted SELECT 'cast_colon_nested', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$SELECT CAST(1 AS Array(`analytics.db`.`User.Type`(16, 'external')))$$) AS formatted SELECT 'cast_quoted_arguments', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$SELECT CAST(1 AS app.UserId())$$) AS formatted SELECT 'cast_empty_arguments', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
WITH formatQuerySingleLine($$SELECT CAST(1 AS UInt64)$$) AS formatted SELECT 'cast_built_in_control', formatted, formatted = formatQuerySingleLine(formatted) FORMAT TSVRaw;
