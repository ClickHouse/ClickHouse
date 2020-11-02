#pragma once

#include "config_core.h"
#if USE_MYSQL

#include <mysqlxx/Pool.h>

#include <common/types.h>
#include <Core/MultiEnum.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>

#include <map>
#include <vector>

namespace DB
{

std::map<String, NamesAndTypesList> fetchTablesColumnsList(
        mysqlxx::Pool & pool,
        const String & database_name,
        const std::vector<String> & tables_name,
        bool external_table_functions_use_nulls,
        MultiEnum<MySQLDataTypesSupport> type_support);

}

#endif
