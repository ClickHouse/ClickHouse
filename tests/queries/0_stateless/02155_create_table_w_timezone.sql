create table t02155_t64_tz ( a DateTime64(9, America/Chicago)) Engine = Memory; -- { clientError 62 }
create table t02155_t_tz ( a DateTime(America/Chicago)) Engine = Memory; -- { clientError 62 }

create table t02155_t64_tz ( a DateTime64(9, 'America/Chicago')) Engine = Memory; 
create table t02155_t_tz ( a DateTime('America/Chicago')) Engine = Memory; 

drop table t02155_t64_tz;
drop table t02155_t_tz;
