SELECT count(in(NULL, []));
SELECT count(notIn(NULL, []));
SELECT count(nullIn(NULL, []));
SELECT count(notNullIn(NULL, []));

SELECT count(in(NULL, tuple(NULL)));
SELECT count(notIn(NULL, tuple(NULL)));
SELECT count(nullIn(NULL, tuple(NULL)));
SELECT count(notNullIn(NULL, tuple(NULL)));
