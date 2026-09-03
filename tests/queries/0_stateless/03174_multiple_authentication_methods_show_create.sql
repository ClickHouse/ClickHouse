-- Tags: no-fasttest, no-parallel

-- Create user with mix both implicit and explicit auth type, starting with with
CREATE USER u_03174_multiple_auth_show_create IDENTIFIED WITH plaintext_password by '1', by '2', bcrypt_password by '3', by '4';
SHOW CREATE USER u_03174_multiple_auth_show_create;

DROP USER IF EXISTS u_03174_multiple_auth_show_create;

-- Create user with mix both implicit and explicit auth type, starting with by
CREATE USER u_03174_multiple_auth_show_create IDENTIFIED by '1', plaintext_password by '2', bcrypt_password by '3', by '4';
SHOW CREATE USER u_03174_multiple_auth_show_create;

DROP USER IF EXISTS u_03174_multiple_auth_show_create;
