SELECT
    concat(concat(unhex('00'), concat(unhex('00'), concat(unhex(toFixedString('00', 2)), toFixedString(toFixedString(' key="v" ', 9), 9), concat(unhex('00'), toFixedString(' key="v" ', 9)), toFixedString(materialize(toLowCardinality(' key="v" ')), 9)), toFixedString(' key="v" ', 9)), toFixedString(' key="v" ', 9)), unhex('00'), ' key="v" ') AS haystack
GROUP BY
    concat(unhex('00'), toFixedString(materialize(toFixedString(' key="v" ', 9)), 9), toFixedString(toFixedString('00', 2), toNullable(2)),  toFixedString(toFixedString(toFixedString(' key="v" ', 9), 9), 9)),
    concat(' key="v" ')
SETTINGS enable_analyzer = 1;
