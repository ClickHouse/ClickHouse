--
-- regressions
--

-- SIGSEGV regression due to QueryPlan lifetime
EXPLAIN PIPELINE graph=1 SELECT * FROM remote('127.{1,2}', system.one) FORMAT Null;
