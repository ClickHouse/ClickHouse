#include <Storages/NamedCollectionsHelpers.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/assert_cast.h>

#include <Poco/String.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <unordered_set>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_named_collection_override_by_default;
}

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

    /// A credential can be given either as a path to a file (`ssl_ca`, `nats_credential_file`) or as
    /// the contents of that file (`ssl_ca_pem`, `nats_credentials`) - two spellings of one setting.
    /// Only the contents form is accepted from a query (see `StorageMySQL::getSSLParams`,
    /// `StoragePostgreSQL::getSSLParams` and `resolveCredentialSource` in `StorageNATS.cpp`), where it
    /// replaces the path inherited from the collection.
    /// Returns the path key a contents key replaces, if the key is a contents key.
    std::optional<std::string> credentialsPathKeyFor(const std::string & key)
    {
        static constexpr std::pair<std::string_view, std::string_view> credentials_keys[] = {
            {"ssl_ca_pem", "ssl_ca"},
            {"ssl_cert_pem", "ssl_cert"},
            {"ssl_key_pem", "ssl_key"},
            {"sslrootcert_pem", "sslrootcert"},
            {"sslcert_pem", "sslcert"},
            {"sslkey_pem", "sslkey"},
            {"nats_credentials", "nats_credential_file"},
        };

        for (const auto & [contents_key, path_key] : credentials_keys)
        {
            if (key == contents_key)
                return std::string(path_key);
        }

        return std::nullopt;
    }

    /// Keys that decide which host a request is sent to. `port` is deliberately absent: it selects a
    /// service on a host the operator already chose, and overriding it next to a pinned `host` is an
    /// established way to complete a collection (see `tests/integration/test_storage_mysql`). A port
    /// written as part of a URL is still compared, because there the whole destination is one value.
    bool isRedirectKey(const std::string & key)
    {
        static const std::unordered_set<std::string_view> redirect_keys = {
            "address", "addresses_expr", "connection_settings", "connection_string", "datasource",
            "endpoint", "host", "hostname", "http_proxy_urls", "storage_account_url", "uri", "url",
        };
        return redirect_keys.contains(key);
    }

    /// Keys holding a secret that stays attached to the request when some other key is overridden.
    /// Only secrets belong here. A key that merely identifies the caller, such as `user`, is not one:
    /// listing it would forbid redirects that leak nothing.
    bool hasCredentials(const NamedCollection & collection)
    {
        return collection.hasAny({
            "access_key_id", "access_token", "account_key", "api_key", "connection_string",
            "credentials.password", "oauth_token", "password", "secret", "secret_access_key",
            "session_token", "ssl_key",
        });
    }

    /// The host:port a redirect value points at. A value that does not parse as a URI is compared
    /// whole, so an unrecognised shape can only ever make the comparison stricter, never laxer.
    std::string redirectTarget(const std::string & value)
    {
        try
        {
            const Poco::URI uri(value);
            if (!uri.getHost().empty())
                return Poco::toLower(uri.getHost()) + ":" + std::to_string(uri.getPort());
        }
        catch (const Poco::Exception &) /// NOLINT(bugprone-empty-catch)
        {
        }
        return Poco::toLower(value);
    }

    /// Whether overriding a redirect key sends the request to a different destination rather than
    /// just to another path of the one the collection already configures. An override of a key the
    /// collection does not define is always a redirect: an alias such as `addresses_expr` replaces
    /// the `host` the collection pins, and there is no stored value of that key to compare against.
    /// `new_value` is empty for a complex (non-literal) argument, which cannot be compared.
    bool isRedirectingOverride(
        const NamedCollection & collection, const std::string & key, const std::optional<std::string> & new_value)
    {
        if (!new_value || !collection.has(key))
            return true;
        return redirectTarget(collection.get<String>(key)) != redirectTarget(*new_value);
    }

    /// Whether the collection carries credentials and also pins the destination they are sent to.
    /// Such a collection expresses the operator's intent that its secrets only ever reach that
    /// destination. A collection with no credentials has nothing to leak, and one that pins no
    /// destination expects the query to supply it, so neither is guarded.
    bool pinsCredentialedDestination(const NamedCollection & collection)
    {
        if (!hasCredentials(collection))
            return false;
        const auto keys = collection.getKeys();
        return std::any_of(keys.begin(), keys.end(), [](const auto & key) { return isRedirectKey(key); });
    }

    struct ForbiddenOverride
    {
        /// The key whose permission forbids the override, which is not always the overridden one.
        std::string key;
        /// Set when the override is forbidden because it would redirect credentials to another host.
        bool redirects_credentials = false;
    };

    /// Whether an override of `key` at the point of use is permitted by the collection.
    /// Returns the key that forbids it - which is not always `key` itself - or nullopt when allowed.
    ///
    /// A contents form (`ssl_ca_pem`) is not a brand-new key when the collection defines the
    /// corresponding path (`ssl_ca`): it replaces it, so the permission is taken from the key it
    /// replaces. Passing the contents is the only way to supply such a credential from SQL at all - a
    /// path is refused there unconditionally - so the permission is the one the operator states
    /// explicitly with `<ssl_ca overridable="false">` rather than the value of
    /// `allow_named_collection_override_by_default`. `StorageMySQL::getSSLParams` re-checks the very
    /// same condition when the replacement happens; without this the two disagree, and the documented
    /// SQL-safe form is rejected on exactly the installations that need it.
    ///
    /// `guard_redirects` says the collection pins a destination for the credentials it carries. A
    /// query may then still repoint a redirect key within that destination, but not away from it:
    /// otherwise `NAMED_COLLECTION` usage alone would be enough to have the server send the
    /// operator's secrets to a host of the user's choosing, which is how they leak without
    /// `SHOW NAMED COLLECTIONS SECRETS`. The operator can still allow it per key with `OVERRIDABLE`,
    /// so the guard only changes what an unmarked key defaults to.
    std::optional<ForbiddenOverride> findOverrideForbiddingKey(
        const NamedCollection & collection,
        const std::string & key,
        bool default_value,
        const std::optional<std::string> & new_value,
        bool guard_redirects)
    {
        /// Only when the override would otherwise be allowed: with `default_value` already false the
        /// check below refuses an unmarked key anyway, and saying it was refused as a redirect would
        /// misattribute the reason.
        if (default_value && guard_redirects && isRedirectKey(key)
            && !collection.isOverridable(key, /* default_value= */ false) && isRedirectingOverride(collection, key, new_value))
            return ForbiddenOverride{key, /* redirects_credentials= */ true};

        if (!collection.has(key))
        {
            auto path_key = credentialsPathKeyFor(key);
            if (path_key && collection.has(*path_key))
            {
                /// The locked key is the path, so name it rather than the contents form the query used.
                if (collection.isOverridable(*path_key, /* default_value= */ true))
                    return std::nullopt;
                return ForbiddenOverride{*path_key};
            }
        }

        if (collection.isOverridable(key, default_value))
            return std::nullopt;
        return ForbiddenOverride{key};
    }

    [[noreturn]] void throwOverrideNotAllowed(const ForbiddenOverride & forbidden)
    {
        if (forbidden.redirects_credentials)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Override not allowed for '{}': it would send the credentials stored in the named collection to a "
                "different destination. Only another path of the configured destination can be given here. An "
                "administrator can allow this by declaring the key as OVERRIDABLE in the named collection.",
                forbidden.key);
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed for '{}'", forbidden.key);
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
    const StorageID * dependent_table_id)
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

    auto collection_copy = collection->duplicate();

    if (asts.size() == 1)
    {
        if (dependent_table_id)
            NamedCollectionFactory::instance().addDependency(*collection_name, *dependent_table_id);
        return collection_copy;
    }

    const auto allow_override_by_default = context->getSettingsRef()[Setting::allow_named_collection_override_by_default];
    /// Determined before any override is applied, so it describes the stored collection rather than
    /// a partially overridden copy of it.
    const bool guard_redirects = pinsCredentialedDestination(*collection_copy);

    for (auto it = std::next(asts.begin()); it != asts.end(); ++it)
    {
        auto value_override = getKeyValueFromASTImpl(*it, /* fallback_to_ast_value */ complex_args != nullptr, context);

        if (!value_override)
        {
            if (!(*it)->as<ASTFunction>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value argument or function");
            if (allow_override_by_default)
                continue;
            // if allow_override_by_default is false we don't allow extra arguments
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed because setting allow_override_by_default is disabled");
        }

        const auto & key = value_override->first;
        const Field * value = std::get_if<Field>(&value_override->second);
        std::optional<std::string> new_value;
        if (value)
            new_value = fieldToString(*value);

        if (auto forbidden = findOverrideForbiddingKey(*collection_copy, key, allow_override_by_default, new_value, guard_redirects))
            throwOverrideNotAllowed(*forbidden);

        if (!value)
        {
            complex_args->emplace_back(key, std::get<ASTPtr>(value_override->second));
            continue;
        }

        /// Marked before the value is written: the mark remembers the stored value the override
        /// replaces, so consumers can tell an override that drops a collection-provided value
        /// from one that never had anything to drop (see `StorageMySQL::getSSLParams`).
        collection_copy->markQueryOverridden(key);
        collection_copy->setOrUpdate<String>(key, *new_value, {});
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
    const auto allow_override_by_default = context->getSettingsRef()[Setting::allow_named_collection_override_by_default];
    /// Determined before any override is applied, so it describes the stored collection rather than
    /// a partially overridden copy of it.
    const bool guard_redirects = pinsCredentialedDestination(*collection_copy);

    for (const auto & key : keys)
    {
        /// The 'name' key identifies the named collection itself and is not a data key to override.
        if (key == "name")
            continue;

        const auto value = config.getString(config_prefix + '.' + key);

        if (auto forbidden = findOverrideForbiddingKey(*collection_copy, key, allow_override_by_default, value, guard_redirects))
            throwOverrideNotAllowed(*forbidden);

        /// The keys of a dictionary created with a DDL query come from the query, so mark them the
        /// same way as the AST-based overload above: `StorageMySQL::getSSLParams` distinguishes a
        /// credential supplied at the point of use from one defined in the collection itself.
        /// Marked before the value is written so the mark remembers the replaced stored value.
        collection_copy->markQueryOverridden(key);
        collection_copy->setOrUpdate<String>(key, value, {});
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
