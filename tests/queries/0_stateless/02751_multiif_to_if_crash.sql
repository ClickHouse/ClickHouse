SELECT sum(A) FROM (SELECT multiIf(1, 1, NULL) as A);
