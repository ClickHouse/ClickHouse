#include "MySQLHelpers.h"

#if USE_MYSQL
#include <mysqlxx/PoolWithFailover.h>
#include <Storages/MySQL/MySQLSettings.h>

namespace DB
{

namespace MySQLSetting
{
    extern const MySQLSettingsUInt64 connection_max_tries;
    extern const MySQLSettingsUInt64 connection_pool_size;
    extern const MySQLSettingsUInt64 connection_wait_timeout;
    extern const MySQLSettingsUInt64 connect_timeout;
    extern const MySQLSettingsUInt64 read_write_timeout;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

mysqlxx::PoolWithFailover createMySQLPoolWithFailover(const StorageMySQL::Configuration & configuration, const MySQLSettings & mysql_settings)
{
    return createMySQLPoolWithFailover(
        configuration.database, configuration.addresses,
        configuration.username, configuration.password, mysql_settings);
}

mysqlxx::PoolWithFailover createMySQLPoolWithFailover(
    const std::string & database,
    const StorageMySQL::Configuration::Addresses & addresses,
    const std::string & username,
    const std::string & password,
    const MySQLSettings & mysql_settings)
{
    if (!mysql_settings[MySQLSetting::connection_pool_size])
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Connection pool cannot have zero size");

    return mysqlxx::PoolWithFailover(
        database, addresses, username, password,
        MYSQLXX_POOL_WITH_FAILOVER_DEFAULT_START_CONNECTIONS,
        static_cast<unsigned>(mysql_settings[MySQLSetting::connection_pool_size]),
        mysql_settings[MySQLSetting::connection_max_tries],
        mysql_settings[MySQLSetting::connection_wait_timeout],
        mysql_settings[MySQLSetting::connect_timeout],
        mysql_settings[MySQLSetting::read_write_timeout]);
}

}

#endif
