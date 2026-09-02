SELECT
    toNullable(NULL) AS a,
    toNullable('Hello') AS b,
    toNullable(toNullable(1)) AS c,
    toNullable(materialize(NULL)) AS d,
    toNullable(materialize('Hello')) AS e,
    toNullable(toNullable(materialize(1))) AS f,
    toTypeName(a),
    toTypeName(b),
    toTypeName(c),
    toTypeName(d),
    toTypeName(e),
    toTypeName(f);
