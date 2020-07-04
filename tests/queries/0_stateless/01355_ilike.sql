SELECT 'Hello' ILIKE '';
SELECT 'Hello' ILIKE '%';
SELECT 'Hello' ILIKE '%%';
SELECT 'Hello' ILIKE '%%%';
SELECT 'Hello' ILIKE '%_%';
SELECT 'Hello' ILIKE '_';
SELECT 'Hello' ILIKE '_%';
SELECT 'Hello' ILIKE '%_';

SELECT 'Hello' ILIKE 'H%o';
SELECT 'hello' ILIKE 'H%o';
SELECT 'hello' ILIKE 'h%o';
SELECT 'Hello' ILIKE 'h%o';

SELECT materialize('prepre_f') ILIKE '%pre_f%';

SELECT 'abcdef'      ILIKE '%aBc%def%';
SELECT 'ABCDDEF'     ILIKE '%abc%def%';
SELECT 'Abc\nDef'    ILIKE '%abc%def%';
SELECT 'abc\ntdef'   ILIKE '%abc%def%';
SELECT 'abct\ndef'   ILIKE '%abc%dEf%';
SELECT 'abc\n\ndeF'  ILIKE '%abc%def%';
SELECT 'abc\n\ntdef' ILIKE '%abc%deF%';
SELECT 'Abc\nt\ndef' ILIKE '%abc%def%';
SELECT 'abct\n\ndef' ILIKE '%abc%def%';
SELECT 'ab\ndef'     ILIKE '%Abc%def%';
SELECT 'aBc\nef'     ILIKE '%ABC%DEF%';

SELECT CAST('hello' AS FixedString(5)) ILIKE '%he%o%';
