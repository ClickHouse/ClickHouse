#pragma once


#if !defined(ARCADIA_BUILD)
    #include "config_core.h"
# endif

#if USE_MYSQL

#include <Core/Defines.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <Databases/MySQL/DatabaseMaterializeMySQL.h>
#include <mysqlxx/Connection.h>

namespace DB {

String checkVariableAndGetVersion(const mysqlxx::Pool::Entry & connection);

Context createQueryContext(const Context & global_context);

BlockIO tryToExecuteQuery(const String & query_to_execute, Context & query_context, const String & database, const String & comment);

DatabaseMaterializeMySQL & getDatabase(const String & database_name);

BlockOutputStreamPtr getTableOutput(const String & database_name, const String & table_name, Context & query_context, bool insert_materialized = false);

}

#endif
