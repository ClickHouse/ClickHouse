SELECT
    UserID,
    UserID = 0,
    if(UserID = 0, 'delete', 'leave')
FROM VALUES('UserID Nullable(UInt8)', (2), (0), (NULL));
