#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Storages/StoragePostgreSQL.h>


namespace DB
{

std::shared_ptr<NamesAndTypesList> fetchPostgreSQLTableStructure(
    postgres::ConnectionHolderPtr connection, const String & postgres_table_name, bool use_nulls);

}

#endif
