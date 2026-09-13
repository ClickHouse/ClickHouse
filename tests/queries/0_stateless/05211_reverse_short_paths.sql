SELECT hex(reverse(unhex('')));
SELECT hex(reverse(unhex('01')));
SELECT hex(reverse(unhex('01020304050607')));
SELECT hex(reverse(unhex('0102030405060708')));
SELECT hex(reverse(unhex('010203040506070809')));
SELECT hex(reverse(unhex('0102030405060708090A0B0C0D0E0F')));
SELECT hex(reverse(unhex('0102030405060708090A0B0C0D0E0F10')));
SELECT hex(reverse(toFixedString(unhex('0102030405060708'), 8)));
