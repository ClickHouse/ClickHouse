SELECT count() FROM (SELECT DISTINCT nowInBlock() FROM system.numbers LIMIT 2);
