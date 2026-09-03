SELECT
    UserID,
    UserID = 0,
    if(UserID = 0, 'delete', 'leave')
FROM VALUES('UserID Nullable(UInt8)', (2), (0), (NULL));

SELECT '---';

SELECT arrayJoin([0, 1, 3, NULL]) AS x, x = 0, if(x = 0, 'Definitely x = 0', 'We cannot say that x = 0');
