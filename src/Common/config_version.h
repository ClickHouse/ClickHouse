#pragma once

/// These fields are changing only on every release, but we still don't want to have them in the header file,
/// because it will make build cache less efficient.

// NOTE: has nothing common with DBMS_TCP_PROTOCOL_VERSION,
// only DBMS_TCP_PROTOCOL_VERSION should be incremented on protocol changes.
extern const unsigned VERSION_REVISION;
extern const char * VERSION_NAME;
extern const unsigned VERSION_MAJOR;
extern const unsigned VERSION_MINOR;
extern const unsigned VERSION_PATCH;
extern const char * VERSION_STRING;
extern const char * VERSION_STRING_SHORT;
extern const char * VERSION_OFFICIAL;
extern const char * VERSION_FULL;
extern const char * VERSION_DESCRIBE;
extern const unsigned VERSION_INTEGER;

/// These fields are frequently changing and we don't want to have them in the header file to allow caching.
extern const char * VERSION_GITHASH;
