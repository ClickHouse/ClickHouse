#include "MySQLDictionarySource.h"


#if USE_MYSQL
#    include <mysqlxx/PoolFactory.h>
#endif

#include <Poco/Util/AbstractConfiguration.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"
#include <Core/Settings.h>
#include <Common/RemoteHostFilter.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/LocalDateTime.h>
#include <Common/parseRemoteDescription.h>
#include <Common/logger_useful.h>
#include "readInvalidateQuery.h"


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 external_storage_connect_timeout_sec;
    extern const SettingsUInt64 external_storage_rw_timeout_sec;
    extern const SettingsUInt64 glob_expansion_max_elements;
}

[[maybe_unused]]
static const size_t default_num_tries_on_connection_loss = 3;

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNSUPPORTED_METHOD;
}

static const ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> dictionary_allowed_keys = {
    "host", "port", "user", "password",
    "db", "database", "table", "schema",
    "update_field", "invalidate_query", "priority",
    "update_lag", "dont_check_update_time",
    "query", "where", "name" /* name_collection */, "socket",
    "share_connection", "fail_on_connection_loss", "close_connection",
    "ssl_ca", "ssl_cert", "ssl_key",
    "enable_local_infile", "opt_reconnect",
    "connect_timeout", "mysql_connect_timeout",
    "mysql_rw_timeout", "rw_timeout"};

void registerDictionarySourceMysql(DictionarySourceFactory & factory)
{
    auto create_table_source = [=]([[maybe_unused]] const DictionaryStructure & dict_struct,
                                   [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                                   [[maybe_unused]] const std::string & config_prefix,
                                   [[maybe_unused]] Block & sample_block,
                                   [[maybe_unused]] ContextPtr global_context,
                                   const std::string & /* default_database */,
                                   [[maybe_unused]] bool created_from_ddl) -> DictionarySourcePtr {
#if USE_MYSQL
        StreamSettings mysql_input_stream_settings(
            global_context->getSettingsRef(),
            config.getBool(config_prefix + ".mysql.close_connection", false) || config.getBool(config_prefix + ".mysql.share_connection", false),
            false,
            config.getBool(config_prefix + ".mysql.fail_on_connection_loss", false) ? 1 : default_num_tries_on_connection_loss);

        auto settings_config_prefix = config_prefix + ".mysql";
        std::shared_ptr<mysqlxx::PoolWithFailover> pool;
        MySQLSettings mysql_settings;

        std::optional<MySQLDictionarySource::Configuration> dictionary_configuration;
        auto named_collection = created_from_ddl ? tryGetNamedCollectionWithOverrides(config, settings_config_prefix, global_context) : nullptr;
        if (named_collection)
        {
            auto allowed_arguments{dictionary_allowed_keys};
            for (const auto & setting : mysql_settings.all())
                allowed_arguments.insert(setting.getName());
            validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(*named_collection, {}, allowed_arguments);

            StorageMySQL::Configuration::Addresses addresses;
            const auto addresses_expr = named_collection->getOrDefault<String>("addresses_expr", "");
            if (addresses_expr.empty())
            {
                const auto host = named_collection->getAnyOrDefault<String>({"host", "hostname"}, "");
                const auto port = static_cast<UInt16>(named_collection->get<UInt64>("port"));
                addresses = {std::make_pair(host, port)};
            }
            else
            {
                size_t max_addresses = global_context->getSettingsRef()[Setting::glob_expansion_max_elements];
                addresses = parseRemoteDescriptionForExternalDatabase(addresses_expr, max_addresses, 3306);
            }

            for (auto & address : addresses)
                global_context->getRemoteHostFilter().checkHostAndPort(address.first, toString(address.second));

            dictionary_configuration.emplace(MySQLDictionarySource::Configuration{
                .db = named_collection->getAnyOrDefault<String>({"database", "db"}, ""),
                .table = named_collection->getOrDefault<String>("table", ""),
                .query = named_collection->getOrDefault<String>("query", ""),
                .where = named_collection->getOrDefault<String>("where", ""),
                .invalidate_query = named_collection->getOrDefault<String>("invalidate_query", ""),
                .update_field = named_collection->getOrDefault<String>("update_field", ""),
                .update_lag = named_collection->getOrDefault<UInt64>("update_lag", 1),
                .dont_check_update_time = named_collection->getOrDefault<bool>("dont_check_update_time", false),
            });

            const auto & settings = global_context->getSettingsRef();
            if (!mysql_settings.isChanged("connect_timeout"))
                mysql_settings.connect_timeout = settings[Setting::external_storage_connect_timeout_sec];
            if (!mysql_settings.isChanged("read_write_timeout"))
                mysql_settings.read_write_timeout = settings[Setting::external_storage_rw_timeout_sec];

            for (const auto & setting : mysql_settings.all())
            {
                const auto & setting_name = setting.getName();
                if (named_collection->has(setting_name))
                    mysql_settings.set(setting_name, named_collection->get<String>(setting_name));
            }

            pool = std::make_shared<mysqlxx::PoolWithFailover>(
                createMySQLPoolWithFailover(
                    dictionary_configuration->db,
                    addresses,
                    named_collection->getAnyOrDefault<String>({"user", "username"}, ""),
                    named_collection->getOrDefault<String>("password", ""),
                    mysql_settings));
        }
        else
        {
            dictionary_configuration.emplace(MySQLDictionarySource::Configuration{
                .db = config.getString(settings_config_prefix + ".db", ""),
                .table = config.getString(settings_config_prefix + ".table", ""),
                .query = config.getString(settings_config_prefix + ".query", ""),
                .where = config.getString(settings_config_prefix + ".where", ""),
                .invalidate_query = config.getString(settings_config_prefix + ".invalidate_query", ""),
                .update_field = config.getString(settings_config_prefix + ".update_field", ""),
                .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
                .dont_check_update_time = config.getBool(settings_config_prefix + ".dont_check_update_time", false)
            });

            pool = std::make_shared<mysqlxx::PoolWithFailover>(
                mysqlxx::PoolFactory::instance().get(config, settings_config_prefix));
        }

        if (dictionary_configuration->query.empty() && dictionary_configuration->table.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MySQL dictionary source configuration must contain table or query field");

        return std::make_unique<MySQLDictionarySource>(dict_struct, *dictionary_configuration, std::move(pool), sample_block, mysql_input_stream_settings);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary source of type `mysql` is disabled because ClickHouse was built without mysql support.");
#endif
    };

    factory.registerSource("mysql", create_table_source);
}

}


#if USE_MYSQL

namespace DB
{


MySQLDictionarySource::MySQLDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    mysqlxx::PoolWithFailoverPtr pool_,
    const Block & sample_block_,
    const StreamSettings & settings_)
    : log(getLogger("MySQLDictionarySource"))
    , update_time(std::chrono::system_clock::from_time_t(0))
    , dict_struct(dict_struct_)
    , configuration(configuration_)
    , pool(std::move(pool_))
    , sample_block(sample_block_)
    , query_builder(dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks)
    , load_all_query(query_builder.composeLoadAllQuery())
    , settings(settings_)
{
}

/// copy-constructor is provided in order to support cloneability
MySQLDictionarySource::MySQLDictionarySource(const MySQLDictionarySource & other)
    : log(getLogger("MySQLDictionarySource"))
    , update_time(other.update_time)
    , dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , pool(other.pool)
    , sample_block(other.sample_block)
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks}
    , load_all_query{other.load_all_query}
    , last_modification{other.last_modification}
    , invalidate_query_response{other.invalidate_query_response}
    , settings(other.settings)
{
}

std::string MySQLDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - configuration.update_lag;
        std::string str_time = DateLUT::instance().timeToString(hr_time);
        update_time = std::chrono::system_clock::now();
        return query_builder.composeUpdateQuery(configuration.update_field, str_time);
    }
    else
    {
        update_time = std::chrono::system_clock::now();
        return load_all_query;
    }
}

QueryPipeline MySQLDictionarySource::loadFromQuery(const String & query)
{
    return QueryPipeline(std::make_shared<MySQLWithFailoverSource>(
            pool, query, sample_block, settings));
}

QueryPipeline MySQLDictionarySource::loadAll()
{
    auto connection = pool->get();
    last_modification = getLastModification(connection, false);

    LOG_TRACE(log, fmt::runtime(load_all_query));
    return loadFromQuery(load_all_query);
}

QueryPipeline MySQLDictionarySource::loadUpdatedAll()
{
    auto connection = pool->get();
    last_modification = getLastModification(connection, false);

    std::string load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, fmt::runtime(load_update_query));
    return loadFromQuery(load_update_query);
}

QueryPipeline MySQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return loadFromQuery(query);
}

QueryPipeline MySQLDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return loadFromQuery(query);
}

bool MySQLDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        if (response == invalidate_query_response)
            return false;

        invalidate_query_response = response;
        return true;
    }

    if (configuration.dont_check_update_time)
        return true;

    auto connection = pool->get();
    return getLastModification(connection, true) > last_modification;
}

bool MySQLDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool MySQLDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}

DictionarySourcePtr MySQLDictionarySource::clone() const
{
    return std::make_shared<MySQLDictionarySource>(*this);
}

std::string MySQLDictionarySource::toString() const
{
    const auto & where = configuration.where;
    return "MySQL: " + configuration.db + '.' + configuration.table + (where.empty() ? "" : ", where: " + where);
}

std::string MySQLDictionarySource::quoteForLike(const std::string & value)
{
    std::string tmp;
    tmp.reserve(value.size());

    for (auto c : value)
    {
        if (c == '%' || c == '_' || c == '\\')
            tmp.push_back('\\');
        tmp.push_back(c);
    }

    WriteBufferFromOwnString out;
    writeQuoted(tmp, out);
    return out.str();
}

LocalDateTime MySQLDictionarySource::getLastModification(mysqlxx::Pool::Entry & connection, bool allow_connection_closure) const
{
    LocalDateTime modification_time{std::time(nullptr)};

    if (configuration.dont_check_update_time)
        return modification_time;

    try
    {
        auto query = connection->query("SHOW TABLE STATUS LIKE " + quoteForLike(configuration.table));

        LOG_TRACE(log, fmt::runtime(query.str()));

        auto result = query.use();

        size_t fetched_rows = 0;
        if (auto row = result.fetch())
        {
            ++fetched_rows;
            static const auto UPDATE_TIME_IDX = 12;
            const auto & update_time_value = row[UPDATE_TIME_IDX];

            if (!update_time_value.isNull())
            {
                modification_time = update_time_value.getDateTime();
                LOG_TRACE(log, "Got modification time: {}", update_time_value.getString());
            }

            /// fetch remaining rows to avoid "commands out of sync" error
            while (result.fetch())
                ++fetched_rows;
        }

        if (settings.auto_close && allow_connection_closure)
        {
            connection.disconnect();
        }

        if (0 == fetched_rows)
            LOG_ERROR(log, "Cannot find table in SHOW TABLE STATUS result.");

        if (fetched_rows > 1)
            LOG_ERROR(log, "Found more than one table in SHOW TABLE STATUS result.");
    }
    catch (...)
    {
        tryLogCurrentException("MySQLDictionarySource");
    }
    /// we suppose failure to get modification time is not an error, therefore return current time
    return modification_time;
}

std::string MySQLDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block invalidate_sample_block;
    ColumnPtr column(ColumnString::create());
    invalidate_sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));
    return readInvalidateQuery(QueryPipeline(std::make_unique<MySQLSource>(pool->get(), request, invalidate_sample_block, settings)));
}

}

#endif
