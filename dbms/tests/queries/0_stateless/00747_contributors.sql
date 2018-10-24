-- Normally table should contain 250+ contributors. But when fast git clone used (--depth=X) (Travis build) table will contain only <=X contributors
SELECT if ((SELECT count(*) FROM system.contributors) > 1, 'ok', 'fail');
