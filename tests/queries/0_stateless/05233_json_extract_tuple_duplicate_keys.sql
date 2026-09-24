-- JSON objects may repeat a key, and the parsers hand every occurrence to the tuple node. Each
-- repetition writes into the same tuple element, so the nested columns have to be brought back to
-- one row per input row.

SELECT 'duplicate key, missing element';
SELECT JSONExtract('{"a":1,"a":2}', 'Tuple(a UInt8, b UInt8)');
SELECT JSONExtract(materialize('{"a":1,"a":2}'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'duplicate key, all elements present';
SELECT JSONExtract(materialize('{"a":1,"a":2,"b":3}'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'duplicate of the last element';
SELECT JSONExtract(materialize('{"a":1,"b":2,"b":3}'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'duplicate after all elements were filled';
SELECT JSONExtract(materialize('{"a":1,"b":2,"a":3}'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'keys out of order with a duplicate';
SELECT JSONExtract(materialize('{"b":2,"a":1,"b":4}'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'triple duplicate';
SELECT JSONExtract(materialize('{"a":1,"a":2,"a":3}'), 'Tuple(a UInt8, b UInt8, c UInt8)') FROM numbers(3);

SELECT 'duplicate of a key outside the tuple';
SELECT JSONExtract(materialize('{"a":1,"z":8,"z":9,"b":2}'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'duplicate with a bad value';
SELECT JSONExtract(materialize('{"a":1,"a":"oops"}'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);
SELECT JSONExtract(materialize('{"a":"oops","a":1}'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'duplicate in a nested tuple';
SELECT JSONExtract(materialize('{"n":{"x":1,"x":2},"m":7}'), 'Tuple(n Tuple(x UInt8, y UInt8), m UInt8)') FROM numbers(3);

SELECT 'duplicate with LowCardinality elements';
SELECT JSONExtract(materialize('{"a":"hi","a":"bye"}'), 'Tuple(a LowCardinality(String), b LowCardinality(String))') FROM numbers(3);
SELECT JSONExtract(materialize('{"a":"hi","a":"bye"}'), 'Tuple(a LowCardinality(FixedString(4)), b LowCardinality(FixedString(4)))') FROM numbers(3);

SELECT 'duplicate with a nullable element';
SELECT JSONExtract(materialize('{"a":1,"a":null}'), 'Tuple(a Nullable(UInt8), b Nullable(UInt8))') FROM numbers(3);

SELECT 'single element tuple';
SELECT JSONExtract(materialize('{"a":1,"a":2}'), 'Tuple(a UInt8)') FROM numbers(3);

SELECT 'unnamed tuple ignores the names';
SELECT JSONExtract(materialize('{"a":1,"a":2}'), 'Tuple(UInt8, UInt8)') FROM numbers(3);

SELECT 'arrays are unaffected';
SELECT JSONExtract(materialize('[1,2]'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);
SELECT JSONExtract(materialize('[1]'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'array with every element invalid';
SELECT JSONExtract(materialize('["x","y"]'), 'Tuple(a UInt8, b UInt8)') FROM numbers(3);

SELECT 'wide tuple, elements past the width of the bit set';
SELECT sum(t.1), sum(t.70), count() FROM (SELECT JSONExtract(materialize('{"c0":1,"c1":1,"c2":1,"c3":1,"c4":1,"c5":1,"c6":1,"c7":1,"c8":1,"c9":1,"c10":1,"c11":1,"c12":1,"c13":1,"c14":1,"c15":1,"c16":1,"c17":1,"c18":1,"c19":1,"c20":1,"c21":1,"c22":1,"c23":1,"c24":1,"c25":1,"c26":1,"c27":1,"c28":1,"c29":1,"c30":1,"c31":1,"c32":1,"c33":1,"c34":1,"c35":1,"c36":1,"c37":1,"c38":1,"c39":1,"c40":1,"c41":1,"c42":1,"c43":1,"c44":1,"c45":1,"c46":1,"c47":1,"c48":1,"c49":1,"c50":1,"c51":1,"c52":1,"c53":1,"c54":1,"c55":1,"c56":1,"c57":1,"c58":1,"c59":1,"c60":1,"c61":1,"c62":1,"c63":1,"c64":1,"c65":1,"c66":1,"c67":1,"c68":1,"c69":1}'), 'Tuple(c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8, c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8, c11 UInt8, c12 UInt8, c13 UInt8, c14 UInt8, c15 UInt8, c16 UInt8, c17 UInt8, c18 UInt8, c19 UInt8, c20 UInt8, c21 UInt8, c22 UInt8, c23 UInt8, c24 UInt8, c25 UInt8, c26 UInt8, c27 UInt8, c28 UInt8, c29 UInt8, c30 UInt8, c31 UInt8, c32 UInt8, c33 UInt8, c34 UInt8, c35 UInt8, c36 UInt8, c37 UInt8, c38 UInt8, c39 UInt8, c40 UInt8, c41 UInt8, c42 UInt8, c43 UInt8, c44 UInt8, c45 UInt8, c46 UInt8, c47 UInt8, c48 UInt8, c49 UInt8, c50 UInt8, c51 UInt8, c52 UInt8, c53 UInt8, c54 UInt8, c55 UInt8, c56 UInt8, c57 UInt8, c58 UInt8, c59 UInt8, c60 UInt8, c61 UInt8, c62 UInt8, c63 UInt8, c64 UInt8, c65 UInt8, c66 UInt8, c67 UInt8, c68 UInt8, c69 UInt8)') AS t FROM numbers(3));
SELECT sum(t.1), sum(t.70), count() FROM (SELECT JSONExtract(materialize('{"c0":1,"c1":1,"c2":1,"c3":1,"c4":1,"c5":1,"c6":1,"c7":1,"c8":1,"c9":1,"c10":1,"c11":1,"c12":1,"c13":1,"c14":1,"c15":1,"c16":1,"c17":1,"c18":1,"c19":1,"c20":1,"c21":1,"c22":1,"c23":1,"c24":1,"c25":1,"c26":1,"c27":1,"c28":1,"c29":1,"c30":1,"c31":1,"c32":1,"c33":1,"c34":1,"c35":1,"c36":1,"c37":1,"c38":1,"c39":1,"c40":1,"c41":1,"c42":1,"c43":1,"c44":1,"c45":1,"c46":1,"c47":1,"c48":1,"c49":1,"c50":1,"c51":1,"c52":1,"c53":1,"c54":1,"c55":1,"c56":1,"c57":1,"c58":1,"c59":1,"c60":1,"c61":1,"c62":1,"c63":1,"c64":1,"c65":1,"c66":1,"c67":1,"c68":1,"c69":1,"c0":2}'), 'Tuple(c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8, c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8, c11 UInt8, c12 UInt8, c13 UInt8, c14 UInt8, c15 UInt8, c16 UInt8, c17 UInt8, c18 UInt8, c19 UInt8, c20 UInt8, c21 UInt8, c22 UInt8, c23 UInt8, c24 UInt8, c25 UInt8, c26 UInt8, c27 UInt8, c28 UInt8, c29 UInt8, c30 UInt8, c31 UInt8, c32 UInt8, c33 UInt8, c34 UInt8, c35 UInt8, c36 UInt8, c37 UInt8, c38 UInt8, c39 UInt8, c40 UInt8, c41 UInt8, c42 UInt8, c43 UInt8, c44 UInt8, c45 UInt8, c46 UInt8, c47 UInt8, c48 UInt8, c49 UInt8, c50 UInt8, c51 UInt8, c52 UInt8, c53 UInt8, c54 UInt8, c55 UInt8, c56 UInt8, c57 UInt8, c58 UInt8, c59 UInt8, c60 UInt8, c61 UInt8, c62 UInt8, c63 UInt8, c64 UInt8, c65 UInt8, c66 UInt8, c67 UInt8, c68 UInt8, c69 UInt8)') AS t FROM numbers(3));
SELECT sum(t.1), sum(t.70), count() FROM (SELECT JSONExtract(materialize('{"c0":1,"c1":1,"c2":1,"c3":1,"c4":1,"c5":1,"c6":1,"c7":1,"c8":1,"c9":1,"c10":1,"c11":1,"c12":1,"c13":1,"c14":1,"c15":1,"c16":1,"c17":1,"c18":1,"c19":1,"c20":1,"c21":1,"c22":1,"c23":1,"c24":1,"c25":1,"c26":1,"c27":1,"c28":1,"c29":1,"c30":1,"c31":1,"c32":1,"c33":1,"c34":1,"c35":1,"c36":1,"c37":1,"c38":1,"c39":1,"c40":1,"c41":1,"c42":1,"c43":1,"c44":1,"c45":1,"c46":1,"c47":1,"c48":1,"c49":1,"c50":1,"c51":1,"c52":1,"c53":1,"c54":1,"c55":1,"c56":1,"c57":1,"c58":1,"c59":1,"c60":1,"c61":1,"c62":1,"c63":1,"c64":1,"c65":1,"c66":1,"c67":1,"c68":1,"c69":1,"c69":2}'), 'Tuple(c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8, c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8, c10 UInt8, c11 UInt8, c12 UInt8, c13 UInt8, c14 UInt8, c15 UInt8, c16 UInt8, c17 UInt8, c18 UInt8, c19 UInt8, c20 UInt8, c21 UInt8, c22 UInt8, c23 UInt8, c24 UInt8, c25 UInt8, c26 UInt8, c27 UInt8, c28 UInt8, c29 UInt8, c30 UInt8, c31 UInt8, c32 UInt8, c33 UInt8, c34 UInt8, c35 UInt8, c36 UInt8, c37 UInt8, c38 UInt8, c39 UInt8, c40 UInt8, c41 UInt8, c42 UInt8, c43 UInt8, c44 UInt8, c45 UInt8, c46 UInt8, c47 UInt8, c48 UInt8, c49 UInt8, c50 UInt8, c51 UInt8, c52 UInt8, c53 UInt8, c54 UInt8, c55 UInt8, c56 UInt8, c57 UInt8, c58 UInt8, c59 UInt8, c60 UInt8, c61 UInt8, c62 UInt8, c63 UInt8, c64 UInt8, c65 UInt8, c66 UInt8, c67 UInt8, c68 UInt8, c69 UInt8)') AS t FROM numbers(3));

SELECT 'the tuple elements stay in sync across many rows';
SELECT sum(t.1), sum(t.2), count() FROM (
    SELECT JSONExtract(
        if(number % 2, '{"a":1,"a":2}', '{"a":3,"b":4}'),
        'Tuple(a UInt8, b UInt8)') AS t
    FROM numbers(1000));
