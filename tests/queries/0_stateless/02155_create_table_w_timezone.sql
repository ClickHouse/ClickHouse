create table 02155_t64_tz ( a DateTime64(9, America/Chicago)) Engine = Memory; -- { serverError 62 }
create table 02155_t_tz ( a DateTime(America/Chicago)) Engine = Memory; -- { serverError 62 }

create table 02155_t64_tz ( a DateTime64(9, 'America/Chicago')) Engine = Memory; 
create table 02155_t_tz ( a DateTime('America/Chicago')) Engine = Memory; 

drop table 02155_t64_tz;
drop table 02155_t_tz;
