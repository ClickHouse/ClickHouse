#include <Interpreters/getEngineDefinitionFromConfig.h>
#include <Interpreters/SystemLogStorage.h>
#include <Common/quoteString.h>
#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <base/find_symbols.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_CONFIG_PARAMETER;
}

String getEngineDefinitionFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, String default_partition_by, String default_order_by, const std::set<String> & engine_constraints, const String & validation_query_description)
{
    String engine;
    if (config.has(config_prefix + ".engine"))
    {
        if (config.has(config_prefix + ".partition_by"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, PARTITION BY parameters should "
                            "be specified directly inside 'engine' and 'partition_by' setting doesn't make sense");
        if (config.has(config_prefix + ".ttl"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, TTL parameters should "
                            "be specified directly inside 'engine' and 'ttl' setting doesn't make sense");
        if (config.has(config_prefix + ".order_by"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, ORDER BY parameters should "
                            "be specified directly inside 'engine' and 'order_by' setting doesn't make sense");
        if (config.has(config_prefix + ".storage_policy"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, SETTINGS storage_policy = '...' should "
                            "be specified directly inside 'engine' and 'storage_policy' setting doesn't make sense");
        if (config.has(config_prefix + ".settings"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, SETTINGS parameters should "
                            "be specified directly inside 'engine' and 'settings' setting doesn't make sense");

        engine = config.getString(config_prefix + ".engine");
    }
    else
    {
        /// ENGINE expr is necessary.
        engine = "ENGINE = MergeTree";

        /// PARTITION expr is not necessary.
        String partition_by = config.getString(config_prefix + ".partition_by", default_partition_by);
        if (!partition_by.empty())
            engine += " PARTITION BY (" + partition_by + ")";

        /// TTL expr is not necessary.
        String ttl = config.getString(config_prefix + ".ttl", "");
        if (!ttl.empty())
            engine += " TTL " + ttl;

        /// ORDER BY expr is necessary.
        String order_by = config.getString(config_prefix + ".order_by", default_order_by);
        engine += " ORDER BY (" + order_by + ")";

        /// SETTINGS expr is not necessary.
        ///   https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#settings
        ///
        /// STORAGE POLICY expr is retained for backward compatible.
        String storage_policy = config.getString(config_prefix + ".storage_policy", "");
        String settings = config.getString(config_prefix + ".settings", "");
        if (!storage_policy.empty() || !settings.empty())
        {
            engine += " SETTINGS";
            /// If 'storage_policy' is repeated, the 'settings' configuration is preferred.
            if (!storage_policy.empty())
                engine += " storage_policy = " + quoteString(storage_policy);
            if (!settings.empty())
                engine += (storage_policy.empty() ? " " : ", ") + settings;
        }
    }

    if (!engine_constraints.empty())
    {
        std::vector<std::string> tokens;
        splitInto<' ', '\t', '\r', '\n', '='>(tokens, engine, true);
        if (tokens.size() < 2)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Ill-formed engine definition: {}", engine);
        const auto& engine_name = tokens[1];
        if (!engine_constraints.contains(engine_name))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Invalid table engine: {}", engine_name);
    }

    /// Validate engine definition syntax to prevent some configuration errors.
    validateEngineDefinition(engine, validation_query_description);

    return engine;
}

}
