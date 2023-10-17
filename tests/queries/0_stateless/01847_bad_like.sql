SELECT '\w' LIKE '%\w%';
SELECT '\w' LIKE '\w%';
SELECT '\w' LIKE '%\w';
SELECT '\w' LIKE '\w';

SELECT '\\w' LIKE '%\\w%';
SELECT '\\w' LIKE '\\w%';
SELECT '\\w' LIKE '%\\w';
SELECT '\\w' LIKE '\\w';

SELECT '\i' LIKE '%\i%';
SELECT '\i' LIKE '\i%';
SELECT '\i' LIKE '%\i';
SELECT '\i' LIKE '\i';

SELECT '\\i' LIKE '%\\i%';
SELECT '\\i' LIKE '\\i%';
SELECT '\\i' LIKE '%\\i';
SELECT '\\i' LIKE '\\i';

SELECT '\\' LIKE '%\\\\%';
SELECT '\\' LIKE '\\\\%';
SELECT '\\' LIKE '%\\\\';
SELECT '\\' LIKE '\\\\';
SELECT '\\' LIKE '\\'; -- { serverError 25 }

SELECT '\\xyz\\' LIKE '\\\\%\\\\';
SELECT '\\xyz\\' LIKE '\\\\___\\\\';
SELECT '\\xyz\\' LIKE '\\\\_%_\\\\';
SELECT '\\xyz\\' LIKE '\\\\%_%\\\\';
