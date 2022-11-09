#include "NamedCollections.h"

#include <Common/assert_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <ranges>


namespace DB
{

static constexpr auto NAMED_COLLECTIONS_CONFIG_PREFIX = "named_collections.";

namespace ErrorCodes
{
    extern const int UNKNOWN_NAMED_COLLECTION;
}

namespace
{
    std::string getCollectionPrefix(const std::string & collection_name)
    {
        return NAMED_COLLECTIONS_CONFIG_PREFIX + collection_name;
    }
}

NamedCollectionFactory & NamedCollectionFactory::instance()
{
    static NamedCollectionFactory instance;
    return instance;
}

void NamedCollectionFactory::initialize(const Poco::Util::AbstractConfiguration & server_config)
{
    std::lock_guard lock(mutex);
    if (is_initialized)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Named collection factory is already initialzied");
    }

    config = &server_config;
    is_initialized = true;
}

void NamedCollectionFactory::assertInitialized(std::lock_guard<std::mutex> & /* lock */) const
{
    if (!is_initialized)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Named collection factory must be initialized before used");
    }
}

bool NamedCollectionFactory::exists(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);
    return config->has(getCollectionPrefix(collection_name));
}

NamedCollectionPtr NamedCollectionFactory::get(
    const std::string & collection_name,
    const NamedCollectionInfo & collection_info) const
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);

    if (!exists(collection_name))
        throw Exception(
            ErrorCodes::UNKNOWN_NAMED_COLLECTION,
            "There is no named collection `{}` in config",
            collection_name);

    return getImpl(collection_name, collection_info, lock);
}

NamedCollectionPtr NamedCollectionFactory::tryGet(
    const std::string & collection_name,
    const NamedCollectionInfo & collection_info) const
{
    std::lock_guard lock(mutex);
    assertInitialized(lock);

    if (!exists(collection_name))
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
        const auto collection_prefix = getCollectionPrefix(collection_name);
        const auto collection_view = config->createView(collection_prefix);

        auto collection = std::make_unique<NamedCollection>();
        collection->initialize(*collection_view, collection_info);
        it = named_collections.emplace(collection_name, std::move(collection)).first;
    }
    return it->second;
}

struct NamedCollection::Impl
{
    std::unordered_map<Key, Value> collection;

    ImplPtr copy() const
    {
        auto impl = std::make_unique<Impl>();
        impl->collection = collection;
        return impl;
    }

    Value get(const Key & key) const
    {
        auto it = collection.find(key);
        if (it == collection.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no key: {}", key);
        return it->second;
    }

    void replace(const Key & key, const Value & value)
    {
        auto it = collection.find(key);
        if (it == collection.end())
            collection.emplace(key, value);
        else
            it->second = value;
    }

    void initialize(
        const Poco::Util::AbstractConfiguration & config,
        const NamedCollectionInfo & collection_info)
    {
        for (const auto & [key, key_info] : collection_info)
        {
            const auto & default_value = key_info.default_value;
            const bool has_value = config.has(key);

            if (!default_value && !has_value)
                continue;

            Field value;
            switch (key_info.type)
            {
                case Field::Types::Which::String:
                    value = has_value ? config.getString(key) : default_value->get<String>();
                    break;
                case Field::Types::Which::UInt64:
                    value = has_value ? config.getUInt64(key) : default_value->get<UInt64>();
                    break;
                case Field::Types::Which::Int64:
                    value = has_value ? config.getInt64(key) : default_value->get<Int64>();
                    break;
                case Field::Types::Which::Float64:
                    value = has_value ? config.getDouble(key) : default_value->get<Float64>();
                    break;
                default:
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Unsupported type: {}", toString(key_info.type));
            }

            collection.emplace(key, value);
        }
    }

    static void validate(
        const Poco::Util::AbstractConfiguration & config,
        const NamedCollectionInfo & collection_info)
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys("", config_keys);

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
};

NamedCollection::NamedCollection()
{
    pimpl =  std::make_unique<Impl>();
}

NamedCollection::NamedCollection(ImplPtr pimpl_)
{
    pimpl = std::move(pimpl_);
}

NamedCollection::Value NamedCollection::get(const Key & key) const
{
    return pimpl->get(key);
}

std::shared_ptr<NamedCollection> NamedCollection::copy() const
{
    return std::make_shared<NamedCollection>(pimpl->copy());
}

void NamedCollection::validate(
    const Poco::Util::AbstractConfiguration & config,
    const NamedCollectionInfo & collection_info) const
{
    pimpl->validate(config, collection_info);
}

void NamedCollection::initialize(
    const Poco::Util::AbstractConfiguration & config,
    const NamedCollectionInfo & collection_info)
{
    pimpl->initialize(config, collection_info);
}

void NamedCollection::replace(const Key & key, const Value & value)
{
    pimpl->replace(key, value);
}

}
