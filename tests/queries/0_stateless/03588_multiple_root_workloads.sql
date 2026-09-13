-- Tags: no-parallel
-- Multiple independent root workloads form a forest: one tree can be managed via SQL and another
-- loaded from config. Independent roots have no scheduling relationship between them. Not parallel:
-- workloads are global server-wide state shared with other workload tests.

create resource 03588_res (write disk 03588_disk);

-- Two independent roots, each its own tree. A second root used to fail with BAD_ARGUMENTS.
create workload 03588_a settings max_io_requests = 100 for 03588_res;
create workload 03588_a1 in 03588_a settings weight = 1;
create workload 03588_b settings max_io_requests = 200 for 03588_res;
create workload 03588_b1 in 03588_b settings weight = 1;

-- Two independent trees; each root has an empty parent (is_root = 1).
select name, empty(parent) as is_root from system.workloads where startsWith(name, '03588_') order by name;

-- Changing whether a workload is a root via CREATE OR REPLACE is not allowed in either direction:
-- promoting a child to a root (dropping its PARENT) ...
create or replace workload 03588_a1 settings weight = 2; -- {serverError BAD_ARGUMENTS}
-- ... or demoting a root to a child (adding a PARENT).
create or replace workload 03588_a in 03588_b; -- {serverError BAD_ARGUMENTS}

-- A third independent root is allowed too.
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
