SELECT quantized16BitL2Distance(toFixedString(unhex('003C00BC'), 4), toFixedString(unhex('003C00BC'), 4));
SELECT quantized16BitL2Distance(toFixedString(unhex('003C00BC'), 4), toFixedString(unhex('00000000'), 4));
