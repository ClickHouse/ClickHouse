-- Tags: no-fasttest

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

SELECT 'Hello' NOT ILIKE 'H%o';
SELECT 'hello' NOT ILIKE 'H%o';
SELECT 'hello' NOT ILIKE 'h%o';
SELECT 'Hello' NOT ILIKE 'h%o';

SELECT 'OHello' ILIKE '%lhell%';
SELECT 'Ohello' ILIKE '%hell%';
SELECT 'hEllo'  ILIKE '%HEL%';

SELECT 'OHello' NOT ILIKE '%lhell%';
SELECT 'Ohello' NOT ILIKE '%hell%';
SELECT 'hEllo'  NOT ILIKE '%HEL%';

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

SELECT 'ёЁё' ILIKE 'Ё%Ё';
SELECT 'ощщЁё' ILIKE 'Щ%Ё';
SELECT 'ощЩЁё' ILIKE '%Щ%Ё';

SELECT 'Щущпандер' ILIKE '%щп%е%';
SELECT 'Щущпандер' ILIKE '%щП%е%';
SELECT 'ощщЁё' ILIKE '%щ%';
SELECT 'ощЩЁё' ILIKE '%ё%';

SHOW TABLES NOT ILIKE '%';
CREATE TABLE test1 (x UInt8) ENGINE = Memory;
CREATE TABLE test2 (x UInt8) ENGINE = Memory;
SHOW TABLES ILIKE 'tES%';
SHOW TABLES NOT ILIKE 'TeS%';
