-- Tags: no-parallel
-- Tag no-parallel: creates fixed-name global roles

-- Test for issue #109298: identifier query parameters worked in `CREATE USER`
-- but were rejected with a syntax error in `CREATE ROLE` and `ALTER ROLE`.

DROP ROLE IF EXISTS role_param_04612, role_param_04612_a, role_param_04612_b, role_param_04612_renamed, role_param_04612_renamed_twice;

SET param_role_name = 'role_param_04612';
CREATE ROLE {role_name:Identifier};
SHOW CREATE ROLE role_param_04612;

SET param_role_a = 'role_param_04612_a';
CREATE ROLE {role_a:Identifier}, role_param_04612_b;
SHOW CREATE ROLE role_param_04612_a;
SHOW CREATE ROLE role_param_04612_b;

ALTER ROLE {role_name:Identifier} SETTINGS max_memory_usage = 5000001;
SHOW CREATE ROLE role_param_04612;

ALTER ROLE {role_name:Identifier} RENAME TO role_param_04612_renamed;
SHOW CREATE ROLE role_param_04612_renamed;

SET param_role_target = 'role_param_04612_renamed_twice';
ALTER ROLE role_param_04612_renamed RENAME TO {role_target:Identifier};
SHOW CREATE ROLE role_param_04612_renamed_twice;

DROP ROLE role_param_04612_renamed_twice, role_param_04612_a, role_param_04612_b;
