#include "NamedCollections.h"

#include <base/find_symbols.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <ranges>


namespace DB
{

static constexpr auto NAMED_COLLECTIONS_CONFIG_PREFIX = "named_collections";

namespace ErrorCodes
{
    extern const int UNKNOWN_NAMED_COLLECTION;
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
}

namespace
{
    std::string getCollectionPrefix(const std::string & collection_name)
    {
        return fmt::format("{}.{}", NAMED_COLLECTIONS_CONFIG_PREFIX, collection_name);
    }

    /// Enumerate keys paths of the config recursively.
    /// E.g. if `enumerate_paths` = {"root.key1"} and config like
    /// <root>
    ///     <key0></key0>
    ///     <key1>
    ///         <key2></key2>
    ///         <key3>
    ///            <key4></key4>
    ///         </key3>
    ///     </key1>
    /// </root>
    /// the `result` will contain two strings: "root.key1.key2" and "root.key1.key3.key4"
    void collectKeys(
        const Poco::Util::AbstractConfiguration & config,
        std::queue<std::string> & enumerate_paths,
        std::set<std::string> & result)
    {
        if (enumerate_paths.empty())
            return;

        auto initial_paths = std::move(enumerate_paths);
        enumerate_paths = {};
        while (!initial_paths.empty())
        {
            auto path = initial_paths.front();
            initial_paths.pop();

            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(path, keys);

            if (keys.empty())
            {
                result.insert(path);
            }
            else
            {
                for (const auto & key : keys)
                    enumerate_paths.emplace(path + '.' + key);
            }
        }

        collectKeys(config, enumerate_paths, result);
    }
}

NamedCollectionFactory & NamedCollectionFactory::instance()
{
    static NamedCollectionFactory instance;
    return instance;
}

void NamedCollectionFactory::initialize(
    const Poco::Util::AbstractConfiguration & server_config)
{
    std::lock_guard lock(mutex);
    if (is_initialized)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Named collection factory already initialized");
    }

    config = &server_config;
    is_initialized = true;
}

void NamedCollectionFactory::assertInitialized(
    std::lock_guard<std::mutex> & /* lock */) const
{
    if (!is_initialized)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Named collection factory must be initialized before being used");
    }
}

bool NamedCollectionFactory::exists(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    return existsUnlocked(collection_name, lock);
}

bool NamedCollectionFactory::existsUnlocked(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & lock) const
{
    assertInitialized(lock);
    /// Named collections can be added via SQL command or via config.
    /// Named collections from config are loaded on first access,
    /// therefore it might not be in `named_collections` map yet.
    return named_collections.contains(collection_name)
        || config->has(getCollectionPrefix(collection_name));
}

NamedCollectionPtr NamedCollectionFactory::get(
    const std::string & collection_name,
    const NamedCollectionInfo & collection_info) const
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);

    if (!existsUnlocked(collection_name, lock))
    {
        throw Exception(
            ErrorCodes::UNKNOWN_NAMED_COLLECTION,
            "There is no named collection `{}`",
            collection_name);
    }

    return getImpl(collection_name, collection_info, lock);
}

NamedCollectionPtr NamedCollectionFactory::tryGet(
    const std::string & collection_name,
    const NamedCollectionInfo & collection_info) const
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);

    if (!existsUnlocked(collection_name, lock))
        return nullptr;

    return getImpl(collection_name, collection_info, lock);
}

NamedCollectionPtr NamedCollectionFactory::getImpl(
    const std::string & collection_name,
    const NamedCollectionInfo & collection_info,
    std::lock_guard<std::mutex> & /* lock */) const
{
    auto it = named_collections.find(collection_name);
    if (it == named_collections.end())
    {
        it = named_collections.emplace(
            collection_name,
            std::make_unique<NamedCollection>(
                *config, collection_name, collection_info)).first;
    }
    return it->second;
}

void NamedCollectionFactory::add(
    const std::string & collection_name,
    NamedCollectionPtr collection)
{
    std::lock_guard lock(mutex);
    auto [it, inserted] = named_collections.emplace(collection_name, collection);
    if (!inserted)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
            "A named collection `{}` already exists",
            collection_name);
    }
}

void NamedCollectionFactory::remove(const std::string & collection_name)
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);

    if (!existsUnlocked(collection_name, lock))
    {
        throw Exception(
            ErrorCodes::UNKNOWN_NAMED_COLLECTION,
            "There is no named collection `{}`",
            collection_name);
    }

    if (config->has(collection_name))
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Collection {} is defined in config and cannot be removed",
            collection_name);
    }

    [[maybe_unused]] auto removed = named_collections.erase(collection_name);
    assert(removed);
}

NamedCollectionFactory::NamedCollections NamedCollectionFactory::getAll() const
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);

    NamedCollections result(named_collections);

    Poco::Util::AbstractConfiguration::Keys config_collections_names;
    config->keys(NAMED_COLLECTIONS_CONFIG_PREFIX, config_collections_names);

    for (const auto & name : config_collections_names)
    {
        if (result.contains(name))
            continue;

        const auto collection_prefix = getCollectionPrefix(name);
        std::queue<std::string> enumerate_input;
        std::set<std::string> enumerate_result;

        enumerate_input.push(collection_prefix);
        collectKeys(*config, enumerate_input, enumerate_result);

        NamedCollectionInfo collection_info;

        /// Collection does not have any keys.
        /// (`enumerate_result` == <collection_path>).
        const bool collection_is_empty = enumerate_result.size() == 1;
        if (!collection_is_empty)
        {
            for (const auto & path : enumerate_result)
            {
                collection_info.emplace(
                    /// Skip collection prefix and add +1 to avoid '.' in the beginning.
                    path.substr(std::strlen(collection_prefix.data()) + 1),
                    NamedCollectionValueInfo{});
            }
        }

        result.emplace(
            name, std::make_unique<NamedCollection>(*config, name, collection_info));
    }

    return result;
}

struct NamedCollection::Impl
{
private:
    using IConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::XMLConfiguration>;

    ///  Named collection configuration
    ///  <collection1>
    ///      ...
    ///  </collection1>
    ConfigurationPtr config;
    /// Information about the values of keys. Key is a path to the
    /// value represented as a dot concatenated list of keys.
    const CollectionInfo collection_info;

public:
    Impl(const Poco::Util::AbstractConfiguration & config_,
         const std::string & collection_name_,
         const NamedCollectionInfo & collection_info_)
        : config(createEmptyConfiguration(collection_name_))
        , collection_info(collection_info_)
    {
        auto collection_path = getCollectionPrefix(collection_name_);
        for (const auto & [key, value_info] : collection_info)
            copyConfigValue(
                config_, collection_path + '.' + key, *config, key, value_info.type);
    }

    Value get(const Key & key) const
    {
        auto value_info = collection_info.at(key);
        return getConfigValue(*config, key, value_info.type, value_info.is_required);
    }

    void set(const Key & key, const Value & value)
    {
        setConfigValue(*config, key, value);
    }

    /// Get a string representation of the collection structure.
    /// Used for debugging and tests.
    std::string toString() const
    {
        /// Convert a collection config like
        /// <collection>
        ///     <key0>value0</key0>
        ///     <key1>
        ///         <key2>value2</key2>
        ///         <key3>
        ///            <key4>value3</key4>
        ///         </key3>
        ///     </key1>
        /// </collection>
        /// to a string:
        /// "key0: value0
        ///  key1:
        ///  	key2: value2
        ///  	key3:
        ///  		key4: value3"
        WriteBufferFromOwnString wb;
        for (const auto & [key, value_info] : collection_info)
        {
            Strings key_parts;
            splitInto<'.'>(key_parts, key);
            size_t tab_cnt = 0;

            for (auto it = key_parts.begin(); it != key_parts.end(); ++it)
            {
                if (it != key_parts.begin())
                    wb << '\n' << std::string(tab_cnt++, '\t');
                wb << *it << ':';
            }
            wb << '\t' << convertFieldToString(get(key)) << '\n';
        }
        return wb.str();
    }

private:
    static void validate(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & collection_path,
        const NamedCollectionInfo & collection_info_)
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(collection_path, config_keys);
        checkKeys(config_keys, collection_info_);
    }

    static void checkKeys(
        const Poco::Util::AbstractConfiguration::Keys & config_keys,
        const NamedCollectionInfo & collection_info)

    {
        auto get_suggestion = [&](bool only_required_keys)
        {
            std::string suggestion;
            for (const auto & [key, info] : collection_info)
            {
                if (only_required_keys && info.is_required)
                    continue;

                if (!suggestion.empty())
                    suggestion += ", ";

                suggestion += key;
            }
            return suggestion;
        };

        std::set<NamedCollection::Key> required_keys;
        for (const auto & [key, info] : collection_info)
        {
            if (info.is_required)
                required_keys.insert(key);
        }

        for (const auto & key : config_keys)
        {
            if (!collection_info.contains(key))
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unknown key `{}`, expected one of: {}",
                    key, get_suggestion(false));
            }
            required_keys.erase(key);
        }

        if (!required_keys.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Keys `{}` are required, but was not found in config. List of required keys: {}",
                fmt::join(required_keys, ", "), get_suggestion(true));
        }
    }

    static ConfigurationPtr createEmptyConfiguration(const std::string & root_name)
    {
        using DocumentPtr = Poco::AutoPtr<Poco::XML::Document>;
        DocumentPtr xml_document(new Poco::XML::Document());
        xml_document->appendChild(xml_document->createElement(root_name));
        ConfigurationPtr config(new Poco::Util::XMLConfiguration(xml_document));
        return config;
    }

    using ConfigValueType = Field::Types::Which;
    static void copyConfigValue(
        const Poco::Util::AbstractConfiguration & from_config,
        const std::string & from_path,
        Poco::Util::AbstractConfiguration & to_config,
        const std::string & to_path,
        ConfigValueType type)
    {
        using Type = Field::Types::Which;
        switch (type)
        {
            case Type::String:
                to_config.setString(to_path, from_config.getString(from_path));
                break;
            case Type::UInt64:
                to_config.setUInt64(to_path, from_config.getUInt64(from_path));
                break;
            case Type::Int64:
                to_config.setInt64(to_path, from_config.getInt64(from_path));
                break;
            case Type::Float64:
                to_config.setDouble(to_path, from_config.getDouble(from_path));
                break;
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type");
        }
    }

    static void setConfigValue(
        Poco::Util::AbstractConfiguration & config,
        const std::string & path,
        const Field & value)
    {
        using Type = Field::Types::Which;
        switch (value.getType())
        {
            case Type::String:
                config.setString(path, value.safeGet<String>());
                break;
            case Type::UInt64:
                config.setUInt64(path, value.safeGet<UInt64>());
                break;
            case Type::Int64:
                config.setInt64(path, value.safeGet<Int64>());
                break;
            case Type::Float64:
                config.setDouble(path, value.safeGet<Float64>());
                break;
            default:
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Unsupported type");
        }
    }

    static Field getConfigValue(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & path,
        ConfigValueType type,
        bool throw_not_found,
        std::optional<Field> default_value = std::nullopt)
    {
        const bool has_value = config.has(path);
        if (!has_value)
        {
            if (throw_not_found)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Failed to find key `{}` in config, but this key is required",
                    path);
            }
            else if (!default_value)
                return Null{};
        }

        Field value;

        using Type = Field::Types::Which;
        switch (type)
        {
            case Type::String:
                value = has_value ? config.getString(path) : default_value->get<String>();
                break;
            case Type::UInt64:
                value = has_value ? config.getUInt64(path) : default_value->get<UInt64>();
                break;
            case Type::Int64:
                value = has_value ? config.getInt64(path) : default_value->get<Int64>();
                break;
            case Type::Float64:
                value = has_value ? config.getDouble(path) : default_value->get<Float64>();
                break;
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type");
        }
        return value;
    }
};

NamedCollection::NamedCollection(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & collection_path,
    const CollectionInfo & collection_info)
    : pimpl(std::make_unique<Impl>(config, collection_path, collection_info))
{
}

NamedCollection::Value NamedCollection::get(const Key & key) const
{
    return pimpl->get(key);
}

std::string NamedCollection::toString() const
{
    return pimpl->toString();
}

}
