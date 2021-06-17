#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Storages/PostgreSQL/ConnectionHolder.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

std::shared_ptr<NamesAndTypesList> fetchPostgreSQLTableStructure(
    postgres::ConnectionHolderPtr connection_holder, const String & postgres_table_name, bool use_nulls);

}

#endif
