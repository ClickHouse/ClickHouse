SELECT quantizedSFP8BitL2Distance(toFixedString(unhex('70F068E8'), 4), toFixedString(unhex('70F068E8'), 4));
SELECT quantizedSFP8BitL2Distance(toFixedString(unhex('70F068E8'), 4), toFixedString(unhex('00000000'), 4));
