-- tests of SIMILAR TO pattern search

select 'SELECT \'hello\'   SIMILAR TO \'hel+o\';           -- Returns: 1 (+ = one or more)';
SELECT 'hello'   SIMILAR TO 'hel+o';           -- Returns: 1 (+ = one or more)

select 'SELECT \'helo\'    SIMILAR TO \'hel+o\';           -- Returns: 1';
SELECT 'helo'    SIMILAR TO 'hel+o';           -- Returns: 1

select 'SELECT \'heo\'     SIMILAR TO \'hel+o\';           -- Returns: 0 (+ requires at least one l)';
SELECT 'heo'     SIMILAR TO 'hel+o';           -- Returns: 0 (+ requires at least one l)

select 'SELECT \'hello\'   SIMILAR TO \'%(el|er)%\';       -- Returns: 1';
SELECT 'hello'   SIMILAR TO '%(el|er)%';       -- Returns: 1

select 'SELECT \'herring\' SIMILAR TO \'%(el|er)%\';       -- Returns: 1';
SELECT 'herring' SIMILAR TO '%(el|er)%';       -- Returns: 1

select 'SELECT \'hello\'   SIMILAR TO \'hel_o\';           -- Returns: 1 (_ = any single char)';
SELECT 'hello'   SIMILAR TO 'hel_o';           -- Returns: 1 (_ = any single char)

select 'SELECT \'hello\'   SIMILAR TO \'he[l]+o\';         -- Returns: 1';
SELECT 'hello'   SIMILAR TO 'he[l]+o';         -- Returns: 1

select 'SELECT \'hello\'   SIMILAR TO \'he[r]+o\';         -- Returns: 0';
SELECT 'hello'   SIMILAR TO 'he[r]+o';         -- Returns: 0

select 'SELECT \'test123\' SIMILAR TO \'[a-z]+[0-9]+\';    -- Returns: 1';
SELECT 'test123' SIMILAR TO '[a-z]+[0-9]+';    -- Returns: 1

-- Anchored by default (matches full string):
select 'SELECT \'hello world\' SIMILAR TO \'hello\';       -- Returns: 0 (not full match)';
SELECT 'hello world' SIMILAR TO 'hello';       -- Returns: 0 (not full match)

select 'SELECT \'hello world\' SIMILAR TO \'%hello%\';     -- Returns: 1';
SELECT 'hello world' SIMILAR TO '%hello%';     -- Returns: 1
