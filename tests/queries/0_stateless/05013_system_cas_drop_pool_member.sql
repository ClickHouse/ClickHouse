-- Grammar + dispatch only: execution needs a CA disk (covered by the integration test).
SYSTEM CAS DROP POOL MEMBER 'srv1' FROM DISK 'no_such_disk'; -- { serverError UNKNOWN_DISK }
