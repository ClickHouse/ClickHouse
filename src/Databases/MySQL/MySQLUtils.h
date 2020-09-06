#pragma once

#include <Core/Defines.h>
#include <mysqlxx/Connection.h>

namespace DB {

String checkVariableAndGetVersion(const mysqlxx::Pool::Entry & connection);

Context createQueryContext(const Context & global_context);

BlockIO tryToExecuteQuery(const String & query_to_execute, Context & query_context, const String & database, const String & comment);

DatabaseMaterializeMySQL & getDatabase(const String & database_name);

BlockOutputStreamPtr getTableOutput(const String & database_name, const String & table_name, Context & query_context, bool insert_materialized = false);

}
