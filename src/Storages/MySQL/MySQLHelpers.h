#pragma once
#include "config_core.h"

#if USE_MYSQL
#include <Interpreters/Context_fwd.h>

namespace mysqlxx { class PoolWithFailover; }

namespace DB
{
struct StorageMySQLConfiguration;
struct MySQLSettings;

mysqlxx::PoolWithFailover
createMySQLPoolWithFailover(const StorageMySQLConfiguration & configuration, const MySQLSettings & mysql_settings);

}

#endif
