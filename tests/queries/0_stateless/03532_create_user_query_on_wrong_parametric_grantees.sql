CREATE USER test_user_03532 GRANTEES {grantees:Identifier}; -- { clientError SYNTAX_ERROR }
SHOW CREATE USER test_user_03532; -- { serverError UNKNOWN_USER }
