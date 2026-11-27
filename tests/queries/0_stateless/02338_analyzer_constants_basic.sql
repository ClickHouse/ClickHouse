SET enable_analyzer = 1;

DESCRIBE (SELECT 1);
SELECT 1;

SELECT '--';

DESCRIBE (SELECT 'test');
SELECT 'test';

SELECT '--';

DESCRIBE (SELECT 1, 'test');
SELECT 1, 'test';

SELECT '--';

DESCRIBE (SELECT 1, 'test', [1, 2, 3]);
SELECT 1, 'test', [1, 2, 3];

SELECT '--';

DESCRIBE (SELECT 1, 'test', [1, 2, 3], ['1', '2', '3']);
SELECT 1, 'test', [1, 2, 3], ['1', '2', '3'];

SELECT '--';

DESCRIBE (SELECT NULL);
SELECT NULL;

SELECT '--';

DESCRIBE (SELECT (1, 1));
SELECT (1, 1);

SELECT '--';

DESCRIBE (SELECT [(1, 1)]);
SELECT [(1, 1)];

DESCRIBE (SELECT NULL, 1, 'test', [1, 2, 3], [(1, 1), (1, 1)]);
SELECT NULL, 1, 'test', [1, 2, 3], [(1, 1), (1, 1)];
