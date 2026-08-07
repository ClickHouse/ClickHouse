SET enable_analyzer = 1;

SELECT
    materialize([(NULL, '11\01111111\011111', '1111')]) AS t,
    (t[1048576]).2,
    materialize(-2147483648),
    (t[-2147483648]).1
GROUP BY
    materialize([(NULL, '1')]),
    '',
    (materialize((t[1023]).2), (materialize(''), (t[2147483647]).1, materialize(9223372036854775807)), (materialize(''), materialize(NULL, 2147483647, t[65535], 256)), materialize(NULL))
; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

SELECT
    materialize([(NULL, '11\01111111\011111', '1111')]) AS t,
    (t[1048576]).2,
    materialize(-2147483648),
    (t[-2147483648]).1
GROUP BY
    materialize([(NULL, '1')]),
    '',
    (materialize((t[1023]).2), (materialize(''), (t[2147483647]).1, materialize(9223372036854775807)), (materialize(''), materialize(NULL), materialize(2147483647), materialize(t[65535]), materialize(256)), materialize(NULL))
;
