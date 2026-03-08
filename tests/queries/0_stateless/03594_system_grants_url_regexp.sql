DROP USER IF EXISTS test_regex_grant_user;
CREATE USER test_regex_grant_user;
GRANT READ ON URL('http://localhost:912.*') TO test_regex_grant_user;
GRANT READ ON S3('s3://foo/*') TO test_regex_grant_user;

SELECT access_object 
FROM system.grants 
WHERE user_name = 'test_regex_grant_user' 
ORDER BY access_object;

DROP USER test_regex_grant_user;
