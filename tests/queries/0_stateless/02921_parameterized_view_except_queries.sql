select '--- Data ---';

DROP VIEW IF EXISTS V_DELTA;
DROP TABLE IF EXISTS users;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

SELECT * FROM users order by uid;

select '--- Params ---';

-- set params
set param_a1 = '10'; 
set param_a2 = '50'; 
set param_a3 = '10'; 
set param_a4 = '40';
--check
select {a1: Int32}, {a2: Int32}, {a3: Int32}, {a4: Int32};

select '--- First view ---';
-- using 4 param in the select of the view work here.
CREATE OR REPLACE VIEW V_DELTA AS
select distinct uid from 
(
select uid, name, age from users
where age >= {a1: Int32}
OR age >= {a2: Int32}
OR age >= {a3: Int32}
OR age >= {a4: Int32}
)
order by uid;

SELECT * FROM V_DELTA(a1=10, a2=50, a3=10, a4=40);


select '--- Second query result ---';

-- check individual query before the next part
select distinct uid from 
(
select uid, name, age from users
where age >= {a1: Int32} AND age <= {a2: Int32}
EXCEPT
select uid, name, age from users
where age >= {a3: Int32} AND age <= {a4: Int32}
)
order by uid;

select '--- Second view result ---';

-- using 4 param in the select of the view do not work here.
CREATE OR REPLACE VIEW V_DELTA AS
select distinct uid from 
(
select uid, name, age from users
where age >= {a1: Int32} AND age <= {a2: Int32}
EXCEPT
select uid, name, age from users
where age >= {a3: Int32} AND age <= {a4: Int32}
)
order by uid;

SELECT * FROM V_DELTA(a1=10, a2=50, a3=10, a4=40);

DROP VIEW V_DELTA;
DROP TABLE users;
