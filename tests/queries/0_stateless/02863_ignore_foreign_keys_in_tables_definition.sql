-- https://github.com/ClickHouse/ClickHouse/issues/53380


drop table if exists parent;
drop table if exists child;

create table parent (id int, primary key(id)) engine MergeTree;
create table child  (id int, pid int, primary key(id), foreign key(pid)) engine MergeTree; -- { clientError SYNTAX_ERROR }
create table child  (id int, pid int, primary key(id), foreign key(pid) references) engine MergeTree; -- { clientError SYNTAX_ERROR }
create table child  (id int, pid int, primary key(id), foreign key(pid) references parent(pid)) engine MergeTree;

show create table child;

create table child2 (id int, pid int, primary key(id), 
                     foreign key(pid) references parent(pid) on delete) engine MergeTree; -- { clientError SYNTAX_ERROR }
create table child2 (id int, pid int, primary key(id), 
                     foreign key(pid) references parent(pid) on delete cascade) engine MergeTree;

show create table child2;

create table child3 (id int, pid int, primary key(id), 
                     foreign key(pid) references parent(pid) on delete cascade on update restrict) engine MergeTree;

show create table child3;

drop table child3;
drop table child2;
drop table child;
drop table parent;