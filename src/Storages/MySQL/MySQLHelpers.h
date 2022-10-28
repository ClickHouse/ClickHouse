#pragma once
#include "config_core.h"

#if USE_MYSQL
#include <Interpreters/Context_fwd.h>

namespace mysqlxx { class PoolWithFailover; }

namespace DB
{
struct StorageMySQLConfiguration;

template <typename T> mysqlxx::PoolWithFailover
createMySQLPoolWithFailover(const StorageMySQLConfiguration & configuration, const T & mysql_settings);

}

#endif
