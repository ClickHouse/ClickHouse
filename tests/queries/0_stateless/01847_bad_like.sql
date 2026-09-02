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
SELECT '\\' LIKE '\\'; -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }

SELECT '\\xyz\\' LIKE '\\\\%\\\\';
SELECT '\\xyz\\' LIKE '\\\\___\\\\';
SELECT '\\xyz\\' LIKE '\\\\_%_\\\\';
SELECT '\\xyz\\' LIKE '\\\\%_%\\\\';
