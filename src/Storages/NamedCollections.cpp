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

namespace ErrorCodes
{
    extern const int UNKNOWN_NAMED_COLLECTION;
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr auto NAMED_COLLECTIONS_CONFIG_PREFIX = "named_collections";

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
        std::queue<std::string> enumerate_paths,
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

void NamedCollectionFactory::initialize(const Poco::Util::AbstractConfiguration & config_)
{
    std::lock_guard lock(mutex);
    if (is_initialized)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Named collection factory already initialized");
    }

    config = &config_;
    is_initialized = true;
}

void NamedCollectionFactory::reload(const Poco::Util::AbstractConfiguration & config_)
{
    std::lock_guard lock(mutex);
    config = &config_;
    loaded_named_collections.clear();
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
    return loaded_named_collections.contains(collection_name)
        || config->has(getCollectionPrefix(collection_name));
}

NamedCollectionPtr NamedCollectionFactory::get(const std::string & collection_name) const
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

    return getImpl(collection_name, lock);
}

NamedCollectionPtr NamedCollectionFactory::tryGet(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);

    if (!existsUnlocked(collection_name, lock))
        return nullptr;

    return getImpl(collection_name, lock);
}

NamedCollectionPtr NamedCollectionFactory::getImpl(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & /* lock */) const
{
    auto it = loaded_named_collections.find(collection_name);
    if (it == loaded_named_collections.end())
    {
        it = loaded_named_collections.emplace(
            collection_name,
            NamedCollection::create(*config, collection_name)).first;
    }
    return it->second;
}

void NamedCollectionFactory::add(
    const std::string & collection_name,
    NamedCollectionPtr collection)
{
    std::lock_guard lock(mutex);
    auto [it, inserted] = loaded_named_collections.emplace(collection_name, collection);
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

    [[maybe_unused]] auto removed = loaded_named_collections.erase(collection_name);
    assert(removed);
}

NamedCollectionFactory::NamedCollections NamedCollectionFactory::getAll() const
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);

    NamedCollections result(loaded_named_collections);

    Poco::Util::AbstractConfiguration::Keys config_collections_names;
    config->keys(NAMED_COLLECTIONS_CONFIG_PREFIX, config_collections_names);

    for (const auto & collection_name : config_collections_names)
    {
        if (result.contains(collection_name))
            continue;

        result.emplace(collection_name, NamedCollection::create(*config, collection_name));
    }

    return result;
}

class NamedCollection::Impl
{
private:
    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    ///  Named collection configuration
    ///  <collection1>
    ///      ...
    ///  </collection1>
    ConfigurationPtr config;
    Keys keys;

public:
    Impl(const Poco::Util::AbstractConfiguration & config_,
         const std::string & collection_name_,
         const Keys & keys_)
        : config(createEmptyConfiguration(collection_name_))
        , keys(keys_)
    {
        auto collection_path = getCollectionPrefix(collection_name_);
        for (const auto & key : keys)
            copyConfigValue<String>(config_, collection_path + '.' + key, *config, key);
    }

    template <typename T> T get(const Key & key) const
    {
        return getConfigValue<T>(*config, key);
    }

    template <typename T> T getOrDefault(const Key & key, const T & default_value) const
    {
        return getConfigValueOrDefault<T>(*config, key, &default_value);
    }

    template <typename T> void set(const Key & key, const T & value, bool update_if_exists)
    {
        setConfigValue<T>(*config, key, value, update_if_exists);
        if (!keys.contains(key))
            keys.insert(key);
    }

    void remove(const Key & key)
    {
        removeConfigValue(*config, key);
        [[maybe_unused]] auto removed = keys.erase(key);
        assert(removed);
    }

    Keys getKeys() const
    {
        return keys;
    }

    ImplPtr copy() const
    {
        return std::make_unique<Impl>(*this);
    }

    std::string dumpStructure() const
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
        ///     key2: value2
        ///     key3:
        ///        key4: value3"
        WriteBufferFromOwnString wb;
        Strings prev_key_parts;
        for (const auto & key : keys)
        {
            Strings key_parts;
            splitInto<'.'>(key_parts, key);
            size_t tab_cnt = 0;

            auto it = key_parts.begin();
            auto prev_key_parts_it = prev_key_parts.begin();
            while (it != key_parts.end()
                   && prev_key_parts_it != prev_key_parts.end()
                   && *it == *prev_key_parts_it)
            {
                ++it;
                ++prev_key_parts_it;
                ++tab_cnt;
            }

            auto start_it = it;
            for (; it != key_parts.end(); ++it)
            {
                if (it != start_it)
                    wb << '\n';
                wb << std::string(tab_cnt++, '\t');
                wb << *it << ':';
            }
            wb << '\t' << get<String>(key) << '\n';
            prev_key_parts = key_parts;
        }
        return wb.str();
    }

private:
    template <typename T> static T getConfigValue(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & path)
    {
        return getConfigValueOrDefault<T>(config, path);
    }

    template <typename T> static T getConfigValueOrDefault(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & path,
        const T * default_value = nullptr)
    {
        if (!config.has(path))
        {
            if (!default_value)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}`", path);
            return *default_value;
        }

        if constexpr (std::is_same_v<T, String>)
            return config.getString(path);
        else if constexpr (std::is_same_v<T, UInt64>)
            return config.getUInt64(path);
        else if constexpr (std::is_same_v<T, Int64>)
            return config.getInt64(path);
        else if constexpr (std::is_same_v<T, Float64>)
            return config.getDouble(path);
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Unsupported type in getConfigValueOrDefault(). "
                "Supported types are String, UInt64, Int64, Float64");
    }

    template<typename T> static void setConfigValue(
        Poco::Util::AbstractConfiguration & config,
        const std::string & path,
        const T & value,
        bool update = false)
    {
        if (!update && config.has(path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key `{}` already exists", path);

        if constexpr (std::is_same_v<T, String>)
            config.setString(path, value);
        else if constexpr (std::is_same_v<T, UInt64>)
            config.setUInt64(path, value);
        else if constexpr (std::is_same_v<T, Int64>)
            config.setInt64(path, value);
        else if constexpr (std::is_same_v<T, Float64>)
            config.setDouble(path, value);
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Unsupported type in setConfigValue(). "
                "Supported types are String, UInt64, Int64, Float64");
    }

    template <typename T> static void copyConfigValue(
        const Poco::Util::AbstractConfiguration & from_config,
        const std::string & from_path,
        Poco::Util::AbstractConfiguration & to_config,
        const std::string & to_path)
    {
        if (!from_config.has(from_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}`", from_path);

        if (to_config.has(to_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key `{}` already exists", to_path);

        if constexpr (std::is_same_v<T, String>)
            to_config.setString(to_path, from_config.getString(from_path));
        else if constexpr (std::is_same_v<T, std::string>)
            to_config.setString(to_path, from_config.getString(from_path));
        else if constexpr (std::is_same_v<T, UInt64>)
            to_config.setUInt64(to_path, from_config.getUInt64(from_path));
        else if constexpr (std::is_same_v<T, Int64>)
            to_config.setInt64(to_path, from_config.getInt64(from_path));
        else if constexpr (std::is_same_v<T, Float64>)
            to_config.setDouble(to_path, from_config.getDouble(from_path));
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Unsupported type in copyConfigValue(). "
                "Supported types are String, UInt64, Int64, Float64");
    }

    static void removeConfigValue(
        Poco::Util::AbstractConfiguration & config,
        const std::string & path)
    {
        if (!config.has(path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}`", path);
        config.remove(path);
    }

    static ConfigurationPtr createEmptyConfiguration(const std::string & root_name)
    {
        using DocumentPtr = Poco::AutoPtr<Poco::XML::Document>;
        DocumentPtr xml_document(new Poco::XML::Document());
        xml_document->appendChild(xml_document->createElement(root_name));
        ConfigurationPtr config(new Poco::Util::XMLConfiguration(xml_document));
        return config;
    }
};

NamedCollection::NamedCollection(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & collection_path,
    const Keys & keys)
    : NamedCollection(std::make_unique<Impl>(config, collection_path, keys))
{
}

NamedCollection::NamedCollection(ImplPtr pimpl_)
    : pimpl(std::move(pimpl_))
{
}

NamedCollectionPtr NamedCollection::create(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & collection_name)
{
    const auto collection_prefix = getCollectionPrefix(collection_name);
    std::queue<std::string> enumerate_input;
    std::set<std::string> enumerate_result;

    enumerate_input.push(collection_prefix);
    collectKeys(config, std::move(enumerate_input), enumerate_result);

    /// Collection does not have any keys.
    /// (`enumerate_result` == <collection_path>).
    const bool collection_is_empty = enumerate_result.size() == 1;
    std::set<std::string> keys;
    if (!collection_is_empty)
    {
        /// Skip collection prefix and add +1 to avoid '.' in the beginning.
        for (const auto & path : enumerate_result)
            keys.emplace(path.substr(collection_prefix.size() + 1));
    }
    return std::make_unique<NamedCollection>(config, collection_name, keys);
}

template <typename T> T NamedCollection::get(const Key & key) const
{
    return pimpl->get<T>(key);
}

template <typename T> T NamedCollection::getOrDefault(const Key & key, const T & default_value) const
{
    return pimpl->getOrDefault<T>(key, default_value);
}

template <typename T> void NamedCollection::set(const Key & key, const T & value, bool update_if_exists)
{
    pimpl->set<T>(key, value, update_if_exists);
}

void NamedCollection::remove(const Key & key)
{
    pimpl->remove(key);
}

std::shared_ptr<NamedCollection> NamedCollection::duplicate() const
{
    return std::make_shared<NamedCollection>(pimpl->copy());
}

NamedCollection::Keys NamedCollection::getKeys() const
{
    return pimpl->getKeys();
}

std::string NamedCollection::dumpStructure() const
{
    return pimpl->dumpStructure();
}

template String NamedCollection::get<String>(const NamedCollection::Key & key) const;
template UInt64 NamedCollection::get<UInt64>(const NamedCollection::Key & key) const;
template Int64 NamedCollection::get<Int64>(const NamedCollection::Key & key) const;
template Float64 NamedCollection::get<Float64>(const NamedCollection::Key & key) const;

template String NamedCollection::getOrDefault<String>(const NamedCollection::Key & key, const String & default_value) const;
template UInt64 NamedCollection::getOrDefault<UInt64>(const NamedCollection::Key & key, const UInt64 & default_value) const;
template Int64 NamedCollection::getOrDefault<Int64>(const NamedCollection::Key & key, const Int64 & default_value) const;
template Float64 NamedCollection::getOrDefault<Float64>(const NamedCollection::Key & key, const Float64 & default_value) const;

template void NamedCollection::set<String>(const NamedCollection::Key & key, const String & value, bool update_if_exists);
template void NamedCollection::set<UInt64>(const NamedCollection::Key & key, const UInt64 & value, bool update_if_exists);
template void NamedCollection::set<Int64>(const NamedCollection::Key & key, const Int64 & value, bool update_if_exists);
template void NamedCollection::set<Float64>(const NamedCollection::Key & key, const Float64 & value, bool update_if_exists);

}
