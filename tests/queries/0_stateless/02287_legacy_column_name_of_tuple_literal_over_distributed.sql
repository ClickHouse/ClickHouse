SET legacy_column_name_of_tuple_literal=1;
SET prefer_localhost_replica=0;

select if(in(dummy, tuple(0, 1)), 'ok', 'ok') from remote('localhost', system.one);