-- Tags: no-parallel
-- Do not run this test in parallel because `all` workload might affect other queries execution process
create resource 03232_write (write disk 03232_fake_disk);
create resource 03232_read (read disk 03232_fake_disk);
create workload all settings max_requests = 100 for 03232_write, max_requests = 200 for 03232_read;
create workload admin in all settings priority = 0;
create workload production in all settings priority = 1, weight = 9;
create workload development in all settings priority = 1, weight = 1;

create workload another_root; -- {serverError BAD_ARGUMENTS}
create workload self_ref in self_ref; -- {serverError BAD_ARGUMENTS}
drop workload all; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings priority = 0 for all; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings priority = 'invalid_value'; -- {serverError BAD_GET}
create workload invalid in all settings weight = 0; -- {serverError INVALID_SCHEDULER_NODE}
create workload invalid in all settings weight = -1; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings max_speed = -1; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings max_cost = -1; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings max_requests = -1; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings max_requests = 1.5; -- {serverError BAD_GET}

drop workload if exists production;
drop workload if exists development;
drop workload if exists admin;
drop workload if exists all;
drop resource if exists 03232_write;
drop resource if exists 03232_read;
