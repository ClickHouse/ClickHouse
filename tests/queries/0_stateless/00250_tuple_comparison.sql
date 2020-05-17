SELECT 
    (1, 'Hello', 23) =  (1, 'Hello', 23),
    (1, 'Hello', 23) != (1, 'Hello', 23),
    (1, 'Hello', 23) <  (1, 'Hello', 23),
    (1, 'Hello', 23) >  (1, 'Hello', 23),
    (1, 'Hello', 23) <= (1, 'Hello', 23),
    (1, 'Hello', 23) >= (1, 'Hello', 23);
SELECT
    (1, 'Hello', 23) =  (2, 'Hello', 23),
    (1, 'Hello', 23) != (2, 'Hello', 23),
    (1, 'Hello', 23) <  (2, 'Hello', 23),
    (1, 'Hello', 23) >  (2, 'Hello', 23),
    (1, 'Hello', 23) <= (2, 'Hello', 23),
    (1, 'Hello', 23) >= (2, 'Hello', 23);
SELECT
    (1, 'Hello', 23) =  (1, 'World', 23),
    (1, 'Hello', 23) != (1, 'World', 23),
    (1, 'Hello', 23) <  (1, 'World', 23),
    (1, 'Hello', 23) >  (1, 'World', 23),
    (1, 'Hello', 23) <= (1, 'World', 23),
    (1, 'Hello', 23) >= (1, 'World', 23);
SELECT
    (1, 'Hello', 23) =  (1, 'Hello', 24),
    (1, 'Hello', 23) != (1, 'Hello', 24),
    (1, 'Hello', 23) <  (1, 'Hello', 24),
    (1, 'Hello', 23) >  (1, 'Hello', 24),
    (1, 'Hello', 23) <= (1, 'Hello', 24),
    (1, 'Hello', 23) >= (1, 'Hello', 24);
SELECT
    (2, 'Hello', 23) =  (1, 'Hello', 23),
    (2, 'Hello', 23) != (1, 'Hello', 23),
    (2, 'Hello', 23) <  (1, 'Hello', 23),
    (2, 'Hello', 23) >  (1, 'Hello', 23),
    (2, 'Hello', 23) <= (1, 'Hello', 23),
    (2, 'Hello', 23) >= (1, 'Hello', 23);
SELECT
    (1, 'World', 23) =  (1, 'Hello', 23),
    (1, 'World', 23) != (1, 'Hello', 23),
    (1, 'World', 23) <  (1, 'Hello', 23),
    (1, 'World', 23) >  (1, 'Hello', 23),
    (1, 'World', 23) <= (1, 'Hello', 23),
    (1, 'World', 23) >= (1, 'Hello', 23);
SELECT
    (1, 'Hello', 24) =  (1, 'Hello', 23),
    (1, 'Hello', 24) != (1, 'Hello', 23),
    (1, 'Hello', 24) <  (1, 'Hello', 23),
    (1, 'Hello', 24) >  (1, 'Hello', 23),
    (1, 'Hello', 24) <= (1, 'Hello', 23),
    (1, 'Hello', 24) >= (1, 'Hello', 23);
SELECT
    (1, 'Hello') =  (1, 'Hello'),
    (1, 'Hello') != (1, 'Hello'),
    (1, 'Hello') <  (1, 'Hello'),
    (1, 'Hello') >  (1, 'Hello'),
    (1, 'Hello') <= (1, 'Hello'),
    (1, 'Hello') >= (1, 'Hello');
SELECT
    (1, 'Hello') =  (2, 'Hello'),
    (1, 'Hello') != (2, 'Hello'),
    (1, 'Hello') <  (2, 'Hello'),
    (1, 'Hello') >  (2, 'Hello'),
    (1, 'Hello') <= (2, 'Hello'),
    (1, 'Hello') >= (2, 'Hello');
SELECT
    (1, 'Hello') =  (1, 'World'),
    (1, 'Hello') != (1, 'World'),
    (1, 'Hello') <  (1, 'World'),
    (1, 'Hello') >  (1, 'World'),
    (1, 'Hello') <= (1, 'World'),
    (1, 'Hello') >= (1, 'World');
SELECT
    (2, 'Hello') =  (1, 'Hello'),
    (2, 'Hello') != (1, 'Hello'),
    (2, 'Hello') <  (1, 'Hello'),
    (2, 'Hello') >  (1, 'Hello'),
    (2, 'Hello') <= (1, 'Hello'),
    (2, 'Hello') >= (1, 'Hello');
SELECT
    (1, 'World') =  (1, 'Hello'),
    (1, 'World') != (1, 'Hello'),
    (1, 'World') <  (1, 'Hello'),
    (1, 'World') >  (1, 'Hello'),
    (1, 'World') <= (1, 'Hello'),
    (1, 'World') >= (1, 'Hello');
SELECT
    tuple(1) =  tuple(1),
    tuple(1) != tuple(1),
    tuple(1) <  tuple(1),
    tuple(1) >  tuple(1),
    tuple(1) <= tuple(1),
    tuple(1) >= tuple(1);
SELECT
    tuple(1) =  tuple(2),
    tuple(1) != tuple(2),
    tuple(1) <  tuple(2),
    tuple(1) >  tuple(2),
    tuple(1) <= tuple(2),
    tuple(1) >= tuple(2);
SELECT
    tuple(2) =  tuple(1),
    tuple(2) != tuple(1),
    tuple(2) <  tuple(1),
    tuple(2) >  tuple(1),
    tuple(2) <= tuple(1),
    tuple(2) >= tuple(1);
