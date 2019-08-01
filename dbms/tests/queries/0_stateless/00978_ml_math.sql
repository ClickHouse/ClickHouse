SELECT 
    round(sigmoid(x), 6), round(sigmoid(toFloat32(x)), 6), round(sigmoid(toFloat64(x)), 6),
    round(tanh(x), 6), round(TANH(toFloat32(x)), 6), round(TANh(toFloat64(x)), 6)
FROM (SELECT arrayJoin([-1, 0, 1]) AS x);
