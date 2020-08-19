SELECT
    round(sigmoid(x), 5), round(sigmoid(toFloat32(x)), 5), round(sigmoid(toFloat64(x)), 5),
    round(tanh(x), 5), round(TANH(toFloat32(x)), 5), round(TANh(toFloat64(x)), 5)
FROM (SELECT arrayJoin([-1, 0, 1]) AS x);
