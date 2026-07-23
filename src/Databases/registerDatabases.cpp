#include "config.h"

#include <Databases/DatabaseFactory.h>
#include <Databases/registerDatabases.h>


namespace DB
{

void registerDatabaseAtomic(DatabaseFactory & factory);
void registerDatabaseOrdinary(DatabaseFactory & factory);
void registerDatabaseDictionary(DatabaseFactory & factory);
void registerDatabaseMemory(DatabaseFactory & factory);
void registerDatabaseLazy(DatabaseFactory & factory);
void registerDatabaseFilesystem(DatabaseFactory & factory);
void registerDatabaseReplicated(DatabaseFactory & factory);

#if USE_MYSQL
void registerDatabaseMySQL(DatabaseFactory & factory);
#endif

#if USE_LIBPQXX
void registerDatabasePostgreSQL(DatabaseFactory & factory);

void registerDatabaseMaterializedPostgreSQL(DatabaseFactory & factory);
#endif

#if USE_SQLITE
void registerDatabaseSQLite(DatabaseFactory & factory);
#endif

#if USE_AWS_S3
void registerDatabaseS3(DatabaseFactory & factory);
#endif

#if USE_HDFS
void registerDatabaseHDFS(DatabaseFactory & factory);
#endif

#if USE_AVRO && USE_PARQUET
void registerDatabaseDataLake(DatabaseFactory & factory);
#endif

void registerDatabaseBackup(DatabaseFactory & factory);

void registerDatabases()
{
    auto & factory = DatabaseFactory::instance();
    registerDatabaseAtomic(factory);
    registerDatabaseOrdinary(factory);
    registerDatabaseDictionary(factory);
    registerDatabaseMemory(factory);
    registerDatabaseLazy(factory);
    registerDatabaseFilesystem(factory);
    registerDatabaseReplicated(factory);

#if USE_MYSQL
    registerDatabaseMySQL(factory);
#endif

#if USE_LIBPQXX
    registerDatabasePostgreSQL(factory);
    registerDatabaseMaterializedPostgreSQL(factory);
#endif

#if USE_SQLITE
    registerDatabaseSQLite(factory);
#endif

#if USE_AWS_S3
    registerDatabaseS3(factory);
#endif

#if USE_HDFS
    registerDatabaseHDFS(factory);
#endif

#if USE_AVRO && USE_PARQUET
    registerDatabaseDataLake(factory);
#endif

    registerDatabaseBackup(factory);
}
}
