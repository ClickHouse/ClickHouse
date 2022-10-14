-- trivial

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin(arrayPopBack([(1, 0)])) as x
);

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin([(0, 0)]) as x
);

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin([(1, 0)]) as x
);

-- single category

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin([(1, 0), (1, 0), (1, 0), (1, 1), (1, 1)]) as x
);

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin([(0, 0), (0, 1), (1, 0), (1, 1)]) as x
);

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin([(0, 0), (0, 0), (1, 0), (1, 0)]) as x
);

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin([(0, 1), (0, 1), (1, 1), (1, 1)]) as x
);

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin([(0, 0), (0, 1), (1, 1), (1, 1)]) as x
);

SELECT
    categoricalInformationValue(x.1, x.2)
FROM (
    SELECT
        arrayJoin([(0, 0), (0, 1), (1, 0), (1, 0)]) as x
);

SELECT
    round(categoricalInformationValue(x.1, x.2)[1], 6),
    round((2 / 2 - 2 / 3) * (log(2 / 2) - log(2 / 3)), 6)
FROM (
    SELECT
        arrayJoin([(0, 0), (1, 0), (1, 0), (1, 1), (1, 1)]) as x
);

-- multiple category

SELECT
    categoricalInformationValue(x.1, x.2, x.3)
FROM (
    SELECT
        arrayJoin([(1, 0, 0), (1, 0, 0), (1, 0, 1), (0, 1, 0), (0, 1, 0), (0, 1, 1)]) as x
);

SELECT
    round(categoricalInformationValue(x.1, x.2, x.3)[1], 6),
    round(categoricalInformationValue(x.1, x.2, x.3)[2], 6),
    round((2 / 4 - 1 / 3) * (log(2 / 4) - log(1 / 3)), 6),
    round((2 / 4 - 2 / 3) * (log(2 / 4) - log(2 / 3)), 6)
FROM (
    SELECT
        arrayJoin([(1, 0, 0), (1, 0, 0), (1, 0, 1), (0, 1, 0), (0, 1, 0), (0, 1, 1), (0, 1, 1)]) as x
);

-- multiple category, larger data size

SELECT
    categoricalInformationValue(x.1, x.2, x.3)
FROM (
    SELECT
        arrayJoin([(1, 0, 0), (1, 0, 0), (1, 0, 1), (0, 1, 0), (0, 1, 0), (0, 1, 1)]) as x
    FROM
        numbers(1000)
);

SELECT
    round(categoricalInformationValue(x.1, x.2, x.3)[1], 6),
    round(categoricalInformationValue(x.1, x.2, x.3)[2], 6),
    round((2 / 4 - 1 / 3) * (log(2 / 4) - log(1 / 3)), 6),
    round((2 / 4 - 2 / 3) * (log(2 / 4) - log(2 / 3)), 6)
FROM (
    SELECT
        arrayJoin([(1, 0, 0), (1, 0, 0), (1, 0, 1), (0, 1, 0), (0, 1, 0), (0, 1, 1), (0, 1, 1)]) as x
    FROM
        numbers(1000)
);
