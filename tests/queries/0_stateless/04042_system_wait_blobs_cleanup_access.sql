-- Tags: no-parallel, no-fasttest

DROP USER IF EXISTS user_test_04042;
CREATE USER user_test_04042;

-- Without the privilege, expect ACCESS_DENIED
EXECUTE AS user_test_04042 SYSTEM WAIT BLOBS CLEANUP 's3_disk'; -- { serverError ACCESS_DENIED }

-- Grant the privilege
GRANT SYSTEM WAIT BLOBS CLEANUP ON *.* TO user_test_04042;

-- With the privilege, expect BAD_ARGUMENTS on a non-object-storage disk
EXECUTE AS user_test_04042 SYSTEM WAIT BLOBS CLEANUP 'default'; -- { serverError BAD_ARGUMENTS }

-- With the privilege and an object storage disk, should succeed
EXECUTE AS user_test_04042 SYSTEM WAIT BLOBS CLEANUP 's3_disk';

DROP USER user_test_04042;
