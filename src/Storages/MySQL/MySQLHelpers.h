#pragma once
#include "config.h"

#if USE_MYSQL
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageMySQL.h>

namespace mysqlxx { class PoolWithFailover; }

namespace DB
{

mysqlxx::PoolWithFailover createMySQLPoolWithFailover(
    const StorageMySQL::Configuration & configuration, const MySQLSettings & mysql_settings);

mysqlxx::PoolWithFailover createMySQLPoolWithFailover(
    const std::string & database,
    const StorageMySQL::Configuration::Addresses & addresses,
    const std::string & username,
    const std::string & password,
    const std::string & ssl_ca_path,
    MySQLSSLMode ssl_mode,
    const MySQLSettings & mysql_settings);
}

#endif
