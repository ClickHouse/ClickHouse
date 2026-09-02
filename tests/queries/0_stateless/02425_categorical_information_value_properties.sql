SELECT round(arrayJoin(categoricalInformationValue(x.1, x.2)), 3) FROM (SELECT arrayJoin([(0, 0), (NULL, 2), (1, 0), (1, 1)]) AS x);
SELECT corr(c1, c2) FROM VALUES((0, 0), (NULL, 2), (1, 0), (1, 1));
SELECT round(arrayJoin(categoricalInformationValue(c1, c2)), 3) FROM VALUES((0, 0), (NULL, 2), (1, 0), (1, 1));
SELECT round(arrayJoin(categoricalInformationValue(c1, c2)), 3) FROM VALUES((0, 0), (NULL, 1), (1, 0), (1, 1));
SELECT categoricalInformationValue(c1, c2) FROM VALUES((0, 0), (NULL, 1));
SELECT categoricalInformationValue(c1, c2) FROM VALUES((NULL, 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT categoricalInformationValue(dummy, dummy);
SELECT categoricalInformationValue(dummy, dummy) WHERE 0;
SELECT categoricalInformationValue(c1, c2) FROM VALUES((toNullable(0), 0));
SELECT groupUniqArray(*) FROM VALUES(toNullable(0));
SELECT groupUniqArray(*) FROM VALUES(NULL);
SELECT categoricalInformationValue(c1, c2) FROM VALUES((NULL, NULL)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT categoricalInformationValue(c1, c2) FROM VALUES((0, 0), (NULL, 0));
SELECT quantiles(0.5, 0.9)(c1) FROM VALUES(0::Nullable(UInt8));
