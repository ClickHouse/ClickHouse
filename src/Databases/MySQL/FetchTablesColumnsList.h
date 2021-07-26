#pragma once

#include "config_core.h"
#if USE_MYSQL

#include <mysqlxx/PoolWithFailover.h>

#include <common/types.h>
#include <Core/MultiEnum.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>

#include <map>
#include <vector>
#include <Core/Settings.h>

namespace DB
{

std::map<String, NamesAndTypesList> fetchTablesColumnsList(
        mysqlxx::PoolWithFailover & pool,
        const String & database_name,
        const std::vector<String> & tables_name,
        const Settings & settings,
        MultiEnum<MySQLDataTypesSupport> type_support);

}

#endif
