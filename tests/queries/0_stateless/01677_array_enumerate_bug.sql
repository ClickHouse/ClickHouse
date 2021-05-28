-- there was a bug - missing check of the total size of keys for the case with hash table with 128bit key.

SELECT arrayEnumerateUniq(arrayEnumerateUniq([toInt256(10), toInt256(100), toInt256(2)]), [toInt256(123), toInt256(1023), toInt256(123)]);

SELECT arrayEnumerateUniq(
    [111111, 222222, 333333],
    [444444, 555555, 666666],
    [111111, 222222, 333333],
    [444444, 555555, 666666],
    [111111, 222222, 333333],
    [444444, 555555, 666666],
    [111111, 222222, 333333],
    [444444, 555555, 666666]);
