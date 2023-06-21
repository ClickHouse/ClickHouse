SELECT
    bitmapHasAny(bitmapBuild([toUInt8(1)]), (
        SELECT groupBitmapState(toUInt8(1))
    )) has1,
    bitmapHasAny(bitmapBuild([toUInt64(1)]), (
        SELECT groupBitmapState(toUInt64(2))
    )) has2;

SELECT '--------------';

SELECT *
FROM
(
    SELECT
        bitmapHasAny(bitmapBuild([toUInt8(1)]), (
            SELECT groupBitmapState(toUInt8(1))
        )) has1,
        bitmapHasAny(bitmapBuild([toUInt64(1)]), (
            SELECT groupBitmapState(toUInt64(2))
        )) has2
); -- { serverError 43 }

SELECT '--------------';

SELECT *
FROM
(
    SELECT
        bitmapHasAny(bitmapBuild([toUInt8(1)]), (
            SELECT groupBitmapState(toUInt8(1))
        )) has1,
        bitmapHasAny(bitmapBuild([toUInt64(1)]), (
            SELECT groupBitmapState(toUInt64(2))
        )) has2
) SETTINGS allow_experimental_analyzer = 1;

