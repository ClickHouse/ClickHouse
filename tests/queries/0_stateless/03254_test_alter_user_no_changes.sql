-- Tags: no-parallel

create user u_03254_alter_user;
alter user u_03254_alter_user; -- { clientError SYNTAX_ERROR }
