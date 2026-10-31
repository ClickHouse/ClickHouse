SELECT
    startsWithCaseInsensitive([1, 2, 3], [1]),
    startsWithCaseInsensitive([1, 2, 3], [3]),
    startsWithUTF8([1, 2, 3], [1]),
    startsWithUTF8([1, 2, 3], [3]),
    startsWithCaseInsensitiveUTF8([1, 2, 3], [1]),
    startsWithCaseInsensitiveUTF8([1, 2, 3], [3]);
