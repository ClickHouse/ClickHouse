#pragma once

#include <base/types.h>


namespace DB
{

/// `SYSTEM DROP DATABASE REPLICA ... FROM ZKPATH ... WITH TABLES` restores the database into a temporary database
/// with this name prefix. Such databases left after a server failure are dropped on startup.
inline const String RESTORING_DATABASE_NAME_FOR_TABLE_DROPPING_PREFIX = ".tmp_restore_db_for_table_dropping_";

}
