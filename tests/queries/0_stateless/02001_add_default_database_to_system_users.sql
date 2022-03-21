-- Tags: no-parallel
-- Tag no-parallel: create user

create user if not exists u_02001 default database system;
select default_database from system.users where name = 'u_02001';
drop user if exists u_02001;
