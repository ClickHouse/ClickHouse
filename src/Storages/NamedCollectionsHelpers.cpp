#include <Storages/NamedCollectionsHelpers.h>
#include <Access/ContextAccess.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/assert_cast.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    std::optional<std::string> getCollectionName(ASTs asts)
    {
        if (asts.empty())
            return std::nullopt;

        const auto * identifier = asts[0]->as<ASTIdentifier>();
        if (!identifier)
            return std::nullopt;

        return identifier->name();
    }

    std::optional<std::pair<std::string, std::variant<Field, ASTPtr>>>
    getKeyValueFromASTImpl(ASTPtr ast, bool fallback_to_ast_value, ContextPtr context)
    {
        const auto * function = ast->as<ASTFunction>();
        if (!function || function->name != "equals")
            return std::nullopt;

        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
        const auto & function_args = function_args_expr->children;

        if (function_args.size() != 2)
            return std::nullopt;

        auto literal_key = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[0], context);
        auto key = checkAndGetLiteralArgument<String>(literal_key, "key");

        ASTPtr literal_value;
        try
        {
            if (key == "database" || key == "db")
                literal_value = evaluateConstantExpressionForDatabaseName(function_args[1], context);
            else
                literal_value = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
        }
        catch (...)
        {
            if (fallback_to_ast_value)
                return std::pair{key, function_args[1]};
            throw;
        }

        auto value = literal_value->as<ASTLiteral>()->value;

        /// A named collection value is stored as text, and an aggregate state has no text
        /// representation: fieldToString() on it raises a LOGICAL_ERROR (an abort under
        /// debug/sanitizers). The value comes from the query, so this is a user error.
        if (value.getType() == Field::Types::AggregateFunctionState)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Value of key '{}' cannot be an aggregate function state", key);

        return std::pair{key, Field(value)};
    }

    /// `XMLConfiguration` accepts indexes, escapes and redundant dots in paths. Compare the
    /// underlying key names so these spellings cannot disguise an override as a new key.
    String normalizeKey(std::string_view key)
    {
        String normalized;
        for (size_t i = 0; i < key.size(); ++i)
        {
            if (key[i] == '[')
            {
                auto end = key.find(']', i);
                if (end == std::string_view::npos)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid named collection key '{}'", key);
                i = end;
            }
            else if (key[i] == '\\' && i + 1 < key.size())
                normalized += key[++i];
            else if (key[i] != '.' || (!normalized.empty() && normalized.back() != '.'))
                normalized += key[i];
        }
        if (!normalized.empty() && normalized.back() == '.')
            normalized.pop_back();
        return normalized;
    }

    /// The librdkafka property that a `Kafka` key sets, and whether the key is a flat `kafka_*` table setting.
    /// `KafkaConfigLoader` sets the properties from nested collection keys (`kafka.<property>`, `kafka.consumer.<property>`
    /// and `kafka.producer.<property>`, with `_` converted to `.`), and the non-empty flat settings overwrite them afterwards.
    struct KafkaProperty
    {
        String name;
        bool is_flat_setting;
    };

    std::optional<KafkaProperty> getKafkaProperty(std::string_view key)
    {
        static constexpr auto flat_settings = std::to_array<std::pair<std::string_view, std::string_view>>({
            {"kafka_security_protocol", "security.protocol"},
            {"kafka_sasl_mechanism", "sasl.mechanism"},
            {"kafka_sasl_username", "sasl.username"},
            {"kafka_sasl_password", "sasl.password"},
            {"kafka_compression_codec", "compression.codec"},
            {"kafka_compression_level", "compression.level"},
            {"kafka_autodetect_client_rack", "client.rack"},
        });

        for (const auto & [setting, property] : flat_settings)
        {
            if (key == setting)
                return KafkaProperty{String(property), true};
        }

        for (const std::string_view prefix : {"kafka.consumer.", "kafka.producer.", "kafka."})
        {
            if (!key.starts_with(prefix))
                continue;

            KafkaProperty property{String(key.substr(prefix.size())), false};
            /// `log_level` is the only librdkafka property with an underscore.
            if (property.name != "log_level")
                std::ranges::replace(property.name, '_', '.');
            return property;
        }
        return std::nullopt;
    }

    bool areEquivalentKeys(std::string_view key, std::string_view other)
    {
        if (NamedCollectionValidateKey<ExternalDatabaseEqualKeysSet>{key} == NamedCollectionValidateKey<ExternalDatabaseEqualKeysSet>{other}
            || NamedCollectionValidateKey<MongoDBEqualKeysSet>{key} == NamedCollectionValidateKey<MongoDBEqualKeysSet>{other})
            return true;

        static constexpr auto equal_keys = std::to_array<std::pair<std::string_view, std::string_view>>({
            {"ssl_ca_pem", "ssl_ca"},
            {"ssl_cert_pem", "ssl_cert"},
            {"ssl_key_pem", "ssl_key"},
            {"sslrootcert_pem", "sslrootcert"},
            {"sslcert_pem", "sslcert"},
            {"sslkey_pem", "sslkey"},
            {"nats_credentials", "nats_credential_file"},
            {"nats_url", "nats_server_list"},
            {"rabbitmq_host_port", "rabbitmq_address"},
            {"http_method", "method"},
            {"compression_method", "compression"},
            {"storage_account_url", "connection_string"},
            {"user", "credentials.user"},
            {"password", "credentials.password"},
        });

        for (const auto & [first, second] : equal_keys)
        {
            if ((key == first && other == second) || (key == second && other == first))
                return true;
        }

        /// A flat `Kafka` setting and a nested collection key that set the same librdkafka property replace each other.
        const auto kafka_property = getKafkaProperty(key);
        const auto other_kafka_property = getKafkaProperty(other);
        if (kafka_property && other_kafka_property && kafka_property->name == other_kafka_property->name
            && (kafka_property->is_flat_setting || other_kafka_property->is_flat_setting))
            return true;

        /// Configuration values include the text of their descendants. Replacing a subtree,
        /// or adding a child of an alias, can therefore replace a stored value as well.
        const auto key_root = key.substr(0, key.find('.'));
        const auto other_root = other.substr(0, other.find('.'));
        if (key_root != key || other_root != other)
            return areEquivalentKeys(key_root, other_root);
        return false;
    }

    /// Throws if `key` replaces a stored key that is `NOT OVERRIDABLE`, unless a replayed definition replaces a stored `'auto'`.
    /// Returns whether `key` replaces a stored key that requires the privilege `SHOW NAMED COLLECTIONS SECRETS`.
    bool checkOverrideLockAndFindStoredKey(const NamedCollection & collection, const std::string & key, bool is_replayed_definition)
    {
        bool overrides_stored_key = false;
        const auto normalized_key = normalizeKey(key);
        for (const auto & stored_key : collection.getKeys())
        {
            if (!areEquivalentKeys(normalized_key, normalizeKey(stored_key)))
                continue;

            /// ClickHouse appends the inferred `format` and `structure` to the arguments and parses them again.
            /// Replacing the stored value `'auto'` neither hides a stored value nor redirects credentials, so it is not an override.
            const bool replaces_auto = (stored_key == "format" || stored_key == "structure")
                && collection.getOrDefault<String>(stored_key, "") == "auto";

            if (!collection.isOverridable(stored_key, /* default_value= */ true) && !(replaces_auto && is_replayed_definition))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed for '{}'", stored_key);

            if (replaces_auto)
                continue;

            overrides_stored_key = true;
        }
        return overrides_stored_key;
    }
}

void checkNamedCollectionOverrideLock(const NamedCollection & collection, const std::string & key)
{
    checkOverrideLockAndFindStoredKey(collection, key, /* is_replayed_definition= */ false);
}

void checkNamedCollectionOverride(const NamedCollection & collection, const std::string & key, ContextPtr context, bool is_replayed_definition)
{
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Checking an override of named collection key '{}' requires a context", key);
    if (checkOverrideLockAndFindStoredKey(collection, key, is_replayed_definition))
        context->checkAccess(AccessType::SHOW_NAMED_COLLECTIONS_SECRETS, collection.getName());
}

void checkNamedCollectionOverridesInDictionarySource(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto collection_name = config.getString(config_prefix + ".name", "");
    if (collection_name.empty())
        return;

    NamedCollectionFactory::instance().loadIfNot();

    /// A missing collection is reported when the dictionary source is created.
    auto collection = NamedCollectionFactory::instance().tryGet(collection_name);
    if (!collection)
        return;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
    {
        /// The 'name' key identifies the named collection itself and is not a data key to override.
        if (key == "name")
            continue;

        checkNamedCollectionOverride(*collection, key, context);
    }
}

std::pair<String, Field> getKeyValueFromAST(ASTPtr ast, ContextPtr context)
{
    auto res = getKeyValueFromASTImpl(ast, true, context);

    if (!res || !std::holds_alternative<Field>(res->second))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to get key value from ast '{}'", ast->formatForErrorMessage());

    return {res->first, std::get<Field>(res->second)};
}

std::map<String, Field> getParamsMapFromAST(ASTs asts, ContextPtr context)
{
    std::map<String, Field> params;
    for (const auto & ast : asts)
    {
        auto [key, value] = getKeyValueFromAST(ast, context);
        bool inserted = params.emplace(key, value).second;
        if (!inserted)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicated key '{}' in params", key);
    }

    return params;
}

MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(
    ASTs asts,
    ContextPtr context,
    bool throw_unknown_collection,
    VectorWithMemoryTracking<std::pair<std::string, ASTPtr>> * complex_args,
    const StorageID * dependent_table_id,
    const ASTSetQuery * settings,
    bool is_replayed_definition)
{
    if (asts.empty())
        return nullptr;

    NamedCollectionFactory::instance().loadIfNot();

    auto collection_name = getCollectionName(asts);
    if (!collection_name.has_value())
        return nullptr;

    context->checkAccess(AccessType::NAMED_COLLECTION, *collection_name);

    NamedCollectionPtr collection;
    if (throw_unknown_collection)
        collection = NamedCollectionFactory::instance().get(*collection_name);
    else
        collection = NamedCollectionFactory::instance().tryGet(*collection_name);

    if (!collection)
        return nullptr;

    if (settings)
    {
        for (const auto & change : settings->changes)
            checkNamedCollectionOverride(*collection, change.name, context);
    }

    auto collection_copy = collection->duplicate();

    if (asts.size() == 1)
    {
        if (dependent_table_id)
            NamedCollectionFactory::instance().addDependency(*collection_name, *dependent_table_id);
        return collection_copy;
    }

    for (auto it = std::next(asts.begin()); it != asts.end(); ++it)
    {
        auto value_override = getKeyValueFromASTImpl(*it, /* fallback_to_ast_value */ complex_args != nullptr, context);

        if (!value_override)
        {
            const auto * function = (*it)->as<ASTFunction>();
            if (!function)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value argument or function");
            checkNamedCollectionOverride(*collection, function->name, context);
            continue;
        }
        checkNamedCollectionOverride(*collection, value_override->first, context, is_replayed_definition);

        if (const ASTPtr * value = std::get_if<ASTPtr>(&value_override->second))
        {
            complex_args->emplace_back(value_override->first, *value);
            continue;
        }

        const auto & [key, value] = *value_override;
        /// Marked before the value is written: the mark remembers the stored value the override
        /// replaces, so consumers can tell an override that drops a collection-provided value
        /// from one that never had anything to drop (see `StorageMySQL::getSSLParams`).
        collection_copy->markQueryOverridden(key);
        collection_copy->setOrUpdate<String>(key, fieldToString(std::get<Field>(value)), {});
    }

    if (dependent_table_id)
        NamedCollectionFactory::instance().addDependency(*collection_name, *dependent_table_id);

    return collection_copy;
}

MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto collection_name = config.getString(config_prefix + ".name", "");
    if (collection_name.empty())
        return nullptr;

    context->checkAccess(AccessType::NAMED_COLLECTION, collection_name);

    const auto & collection = NamedCollectionFactory::instance().get(collection_name);
    auto collection_copy = collection->duplicate();

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
    {
        /// The 'name' key identifies the named collection itself and is not a data key to override.
        if (key == "name")
            continue;

        checkNamedCollectionOverrideLock(*collection, key);

        /// The keys of a dictionary created with a DDL query come from the query, so mark them the
        /// same way as the AST-based overload above: `StorageMySQL::getSSLParams` distinguishes a
        /// credential supplied at the point of use from one defined in the collection itself.
        /// Marked before the value is written so the mark remembers the replaced stored value.
        collection_copy->markQueryOverridden(key);
        collection_copy->setOrUpdate<String>(key, config.getString(config_prefix + '.' + key), {});
    }

    /// Register the dictionary that uses this named collection as a dependency,
    /// so that DROP NAMED COLLECTION is blocked while the dictionary exists.
    /// config_prefix is "<dict_root>.source.<type>" (e.g. "dictionary.source.clickhouse"),
    /// where the dictionary root is always the first component.
    auto dot = config_prefix.find('.');
    if (dot == std::string::npos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected config_prefix to have dotted components, got: {}", config_prefix);
    auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix.substr(0, dot));
    NamedCollectionFactory::instance().addDependency(collection_name, dict_id);

    return collection_copy;
}

HTTPHeaderEntries getHeadersFromNamedCollection(const NamedCollection & collection)
{
    HTTPHeaderEntries headers;
    auto keys = collection.getKeys(0, "headers");
    for (const auto & key : keys)
        headers.emplace_back(collection.get<String>(key + ".name"), collection.get<String>(key + ".value"));
    return headers;
}

}
