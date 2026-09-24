-- Tags: no-parallel
-- Multiple workloads may be created without a parent (no `IN` clause): one tree can be managed via SQL and another
-- loaded from config. Internally each becomes a child of an implicit anonymous root workload, so
-- they are scheduled among each other by weight/priority; in system.workloads they still show an
-- empty parent. Not parallel: workloads are global server-wide state shared with other workload tests.

create resource 03588_res (write disk 03588_disk);

-- Two parentless workloads, each with a subtree: both roots coexist in one forest.
create workload 03588_a settings max_io_requests = 100 for 03588_res;
create workload 03588_a1 in 03588_a settings weight = 1;
create workload 03588_b settings max_io_requests = 200 for 03588_res;
create workload 03588_b1 in 03588_b settings weight = 1;

-- Two independent trees; each root has an empty parent (is_root = 1).
select name, empty(parent) as is_root from system.workloads where startsWith(name, '03588_') order by name;

-- A workload's parent can be changed with CREATE OR REPLACE, in either direction.
-- Promote a child to a root (drop its `IN` clause):
create or replace workload 03588_a1 settings weight = 2;
-- Demote a root to a child (add an `IN` clause):
create or replace workload 03588_a in 03588_b;
-- 03588_a1 is a root now; 03588_a is a child of 03588_b.
select name, empty(parent) as is_root from system.workloads where startsWith(name, '03588_') order by name;
-- A cycle (a workload made a child of its own descendant) is rejected.
create or replace workload 03588_b in 03588_a; -- {serverError BAD_ARGUMENTS}

-- A third parentless workload is allowed too.
create workload 03588_c;
select count() from system.workloads where startsWith(name, '03588_') and empty(parent);

-- Dropping one whole tree leaves the others intact.
drop workload 03588_a1;
drop workload 03588_a;
select name, empty(parent) as is_root from system.workloads where startsWith(name, '03588_') order by name;

-- A root can be recreated after being dropped.
create workload 03588_a;
select count() from system.workloads where startsWith(name, '03588_') and empty(parent);

-- Clean up
drop workload if exists 03588_a1;
drop workload if exists 03588_b1;
drop workload if exists 03588_a;
drop workload if exists 03588_b;
drop workload if exists 03588_c;
drop resource if exists 03588_res;
