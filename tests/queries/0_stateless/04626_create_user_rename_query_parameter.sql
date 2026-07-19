-- Test for a bug found in the review of PR #110973: `ALTER USER x RENAME TO {new:Identifier}`
-- flattened the rename target to a string at parse time, so the query parameter was never
-- substituted and the user was silently renamed to the literal `{new:Identifier}`.

DROP USER IF EXISTS user_param_04626, user_param_04626_renamed, user_param_04626_renamed_twice;

CREATE USER user_param_04626 IDENTIFIED WITH no_password;
SHOW CREATE USER user_param_04626;

SET param_user_target = 'user_param_04626_renamed';
ALTER USER user_param_04626 RENAME TO {user_target:Identifier};
SHOW CREATE USER user_param_04626_renamed;
SHOW CREATE USER user_param_04626; -- { serverError UNKNOWN_USER }

-- The rename source can be a query parameter as well
SET param_user_source = 'user_param_04626_renamed';
SET param_user_target_twice = 'user_param_04626_renamed_twice';
ALTER USER {user_source:Identifier} RENAME TO {user_target_twice:Identifier};
SHOW CREATE USER user_param_04626_renamed_twice;

-- An unset parameter is an error instead of renaming the user to the literal parameter text
ALTER USER user_param_04626_renamed_twice RENAME TO {user_param_04626_unset:Identifier}; -- { serverError UNKNOWN_QUERY_PARAMETER }
SHOW CREATE USER user_param_04626_renamed_twice;

DROP USER user_param_04626_renamed_twice;
