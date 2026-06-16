SELECT map('a', 1, 'b', 2) FROM remote('127.0.0.{1,2}', system, one);
SELECT map('a', 1, 'b', 2) FROM remote('127.0.0.{1,2}');

SELECT map() from remote('127.0.0.{1,2}', system,one);

drop table if exists bug_repro_local;
CREATE TABLE bug_repro_local (`attributes` Map(LowCardinality(String), String)) ENGINE = Log as select map('','');
SELECT if(1, attributes, map()) from remote('127.0.0.{1,2}', currentDatabase(), bug_repro_local) limit 1;
