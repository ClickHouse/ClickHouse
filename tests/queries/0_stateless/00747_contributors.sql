SELECT if ((SELECT count(*) FROM system.contributors) > 200, 'ok', 'fail');
