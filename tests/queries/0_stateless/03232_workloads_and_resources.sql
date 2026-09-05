-- Tags: no-parallel
-- Do not run this test in parallel because `all` workload might affect other queries execution process

-- Test simple resource and workload hierarchy creation
create resource 03232_write (write disk 03232_fake_disk);
create resource 03232_read (read disk 03232_fake_disk);
create resource 03232_invalid_mix (read disk 03232_another_fake_disk, master thread); -- {clientError BAD_ARGUMENTS}
create workload all settings max_io_requests = 100 for 03232_write, max_io_requests = 200 for 03232_read;
create workload admin in all settings priority = 0;
create workload production in all settings priority = 1, weight = 9;
create workload development in all settings priority = 1, weight = 1;

-- Test that illegal actions are not allowed
create workload another_root; -- {serverError BAD_ARGUMENTS}
create workload self_ref in self_ref; -- {serverError BAD_ARGUMENTS}
drop workload all; -- {serverError BAD_ARGUMENTS}
create workload invalid in 03232_write; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings priority = 0 for all; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings priority = 'invalid_value'; -- {serverError BAD_GET}
create workload invalid in all settings weight = 0; -- {serverError INVALID_SCHEDULER_NODE}
create workload invalid in all settings weight = -1; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings max_speed = -1; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings max_bytes_inflight = -1; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings unknown_setting = 42; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings max_io_requests = -1; -- {serverError BAD_ARGUMENTS}
create workload invalid in all settings max_io_requests = 1.5; -- {serverError BAD_GET}
create or replace workload all in production; -- {serverError BAD_ARGUMENTS}

-- Test CREATE OR REPLACE WORKLOAD
create or replace workload all settings max_io_requests = 200 for 03232_write, max_io_requests = 100 for 03232_read, max_concurrent_threads = 16, max_concurrent_threads_ratio_to_cores = 2.5;
create or replace workload admin in all settings priority = 1;
create or replace workload admin in all settings priority = 2;
create or replace workload admin in all settings priority = 0;
create or replace workload production in all settings priority = 1, weight = 90;
create or replace workload production in all settings priority = 0, weight = 9;
create or replace workload production in all settings priority = 2, weight = 9;
create or replace workload development in all settings priority = 1;
create or replace workload development in all settings priority = 0;
create or replace workload development in all settings priority = 2;

-- Test CREATE OR REPLACE RESOURCE
create or replace resource 03232_write (write disk 03232_fake_disk_2);
create or replace resource 03232_read (read disk 03232_fake_disk_2);

-- Test update settings with CREATE OR REPLACE WORKLOAD
create or replace workload production in all settings priority = 1, weight = 9, max_io_requests = 100;
create or replace workload development in all settings priority = 1, weight = 1, max_io_requests = 10;
create or replace workload production in all settings priority = 1, weight = 9, max_bytes_inflight = 100000;
create or replace workload development in all settings priority = 1, weight = 1, max_bytes_inflight = 10000;
create or replace workload production in all settings priority = 1, weight = 9, max_speed = 1000000;
create or replace workload development in all settings priority = 1, weight = 1, max_speed = 100000;
create or replace workload production in all settings priority = 1, weight = 9, max_speed = 1000000, max_burst = 10000000;
create or replace workload development in all settings priority = 1, weight = 1, max_speed = 100000, max_burst = 1000000;
create or replace workload all settings max_bytes_inflight = 1000000, max_speed = 100000 for 03232_write, max_speed = 200000 for 03232_read;
create or replace workload all settings max_io_requests = 100 for 03232_write, max_io_requests = 200 for 03232_read;
create or replace workload production in all settings priority = 1, weight = 9;
create or replace workload development in all settings priority = 1, weight = 1;

-- Test change parent with CREATE OR REPLACE WORKLOAD
create or replace workload development in production settings priority = 1, weight = 1;
create or replace workload development in admin settings priority = 1, weight = 1;
create or replace workload development in all settings priority = 1, weight = 1;

-- Clean up
drop workload if exists production;
drop workload if exists development;
drop workload if exists admin;
drop workload if exists all;
drop resource if exists 03232_write;
drop resource if exists 03232_read;
