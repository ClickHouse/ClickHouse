#include "NamedCollections.h"

#include <Interpreters/Context.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_IS_IMMUTABLE;
    extern const int BAD_ARGUMENTS;
}

namespace Configuration = NamedCollectionConfiguration;

class NamedCollection::Impl
{
private:
    ConfigurationPtr config;
    Keys keys;

    Impl(ConfigurationPtr config_, const Keys & keys_) : config(config_) , keys(keys_) {}

public:
    static ImplPtr create(
         const Poco::Util::AbstractConfiguration & config,
         const std::string & collection_name,
         const std::string & collection_path,
         const Keys & keys)
    {
        auto collection_config = NamedCollectionConfiguration::createEmptyConfiguration(collection_name);
        for (const auto & key : keys)
            Configuration::copyConfigValue<String>(
                config, collection_path + '.' + key, *collection_config, key);

        return std::unique_ptr<Impl>(new Impl(collection_config, keys));
    }

    bool has(const Key & key) const
    {
        return Configuration::hasConfigValue(*config, key);
    }

    template <typename T> T get(const Key & key) const
    {
        return Configuration::getConfigValue<T>(*config, key);
    }

    template <typename T> T getOrDefault(const Key & key, const T & default_value) const
    {
        return Configuration::getConfigValueOrDefault<T>(*config, key, &default_value);
    }

    template <typename T>
    void set(const Key & key, const T & value, bool update_if_exists, const std::optional<bool> is_overridable)
    {
        Configuration::setConfigValue<T>(*config, key, value, update_if_exists, is_overridable);
        if (!keys.contains(key))
            keys.insert(key);
    }

    bool isOverridable(const Key & key, const bool default_value)
    {
        const auto is_overridable = Configuration::isOverridable(*config, key);
        if (is_overridable)
            return *is_overridable;
        return default_value;
    }

    ImplPtr createCopy(const std::string & collection_name_) const
    {
        return create(*config, collection_name_, "", keys);
    }

    void remove(const Key & key)
    {
        Configuration::removeConfigValue(*config, key);
        [[maybe_unused]] auto removed = keys.erase(key);
        assert(removed);
    }

    Keys getKeys(ssize_t depth, const std::string & prefix) const
    {
        std::queue<std::string> enumerate_input;

        if (prefix.empty())
        {
            if (depth == -1)
            {
                /// Return all keys with full depth.
                return keys;
            }
        }
        else
        {
            if (!Configuration::hasConfigValue(*config, prefix))
                return {};

            enumerate_input.push(prefix);
        }

        Keys result;
        Configuration::listKeys(*config, enumerate_input, result, depth);
        return result;
    }

    Keys::const_iterator begin() const
    {
        return keys.begin();
    }

    Keys::const_iterator end() const
    {
        return keys.end();
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
};

NamedCollection::NamedCollection(
    ImplPtr pimpl_,
    const std::string & collection_name_,
    SourceId source_id_,
    bool is_mutable_)
    : pimpl(std::move(pimpl_))
    , collection_name(collection_name_)
    , source_id(source_id_)
    , is_mutable(is_mutable_)
{
}

MutableNamedCollectionPtr NamedCollection::create(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & collection_name,
    const std::string & collection_path,
    const Keys & keys,
    SourceId source_id,
    bool is_mutable)
{
    auto impl = Impl::create(config, collection_name, collection_path, keys);
    return std::unique_ptr<NamedCollection>(
        new NamedCollection(std::move(impl), collection_name, source_id, is_mutable));
}

bool NamedCollection::has(const Key & key) const
{
    std::lock_guard lock(mutex);
    return pimpl->has(key);
}

bool NamedCollection::hasAny(const std::initializer_list<Key> & keys) const
{
    std::lock_guard lock(mutex);
    for (const auto & key : keys)
        if (pimpl->has(key))
            return true;
    return false;
}

template <typename T> T NamedCollection::get(const Key & key) const
{
    std::lock_guard lock(mutex);
    return pimpl->get<T>(key);
}

template <typename T> T NamedCollection::getOrDefault(const Key & key, const T & default_value) const
{
    std::lock_guard lock(mutex);
    return pimpl->getOrDefault<T>(key, default_value);
}

template <typename T> T NamedCollection::getAny(const std::initializer_list<Key> & keys) const
{
    std::lock_guard lock(mutex);
    for (const auto & key : keys)
    {
        if (pimpl->has(key))
            return pimpl->get<T>(key);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such keys: {}", fmt::join(keys, ", "));
}

template <typename T> T NamedCollection::getAnyOrDefault(const std::initializer_list<Key> & keys, const T & default_value) const
{
    std::lock_guard lock(mutex);
    for (const auto & key : keys)
    {
        if (pimpl->has(key))
            return pimpl->get<T>(key);
    }
    return default_value;
}

template <typename T, bool Locked>
void NamedCollection::set(const Key & key, const T & value, const std::optional<bool> is_overridable)
{
    assertMutable();
    std::unique_lock lock(mutex, std::defer_lock);
    if constexpr (!Locked)
        lock.lock();
    pimpl->set<T>(key, value, false, is_overridable);
}

template <typename T, bool Locked>
void NamedCollection::setOrUpdate(const Key & key, const T & value, const std::optional<bool> is_overridable)
{
    assertMutable();
    std::unique_lock lock(mutex, std::defer_lock);
    if constexpr (!Locked)
        lock.lock();
    pimpl->set<T>(key, value, true, is_overridable);
}

bool NamedCollection::isOverridable(const Key & key, bool default_value) const
{
    std::lock_guard lock(mutex);
    return pimpl->isOverridable(key, default_value);
}

template <bool Locked> void NamedCollection::remove(const Key & key)
{
    assertMutable();
    std::unique_lock lock(mutex, std::defer_lock);
    if constexpr (!Locked)
        lock.lock();
    pimpl->remove(key);
}

void NamedCollection::assertMutable() const
{
    if (!is_mutable)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_IS_IMMUTABLE,
            "Cannot change named collection because it is immutable");
    }
}

MutableNamedCollectionPtr NamedCollection::duplicate() const
{
    std::lock_guard lock(mutex);
    auto impl = pimpl->createCopy(collection_name);
    return std::unique_ptr<NamedCollection>(
        new NamedCollection(
            std::move(impl), collection_name, SourceId::NONE, true));
}

NamedCollection::Keys NamedCollection::getKeys(ssize_t depth, const std::string & prefix) const
{
    std::lock_guard lock(mutex);
    return pimpl->getKeys(depth, prefix);
}

template <bool Locked> NamedCollection::const_iterator NamedCollection::begin() const
{
    std::unique_lock lock(mutex, std::defer_lock);
    if constexpr (!Locked)
        lock.lock();
    return pimpl->begin();
}

template <bool Locked> NamedCollection::const_iterator NamedCollection::end() const
{
    std::unique_lock lock(mutex, std::defer_lock);
    if constexpr (!Locked)
        lock.lock();
    return pimpl->end();
}

std::string NamedCollection::dumpStructure() const
{
    std::lock_guard lock(mutex);
    return pimpl->dumpStructure();
}

std::unique_lock<std::mutex> NamedCollection::lock()
{
    return std::unique_lock(mutex);
}

template String NamedCollection::get<String>(const NamedCollection::Key & key) const;
template UInt64 NamedCollection::get<UInt64>(const NamedCollection::Key & key) const;
template Int64 NamedCollection::get<Int64>(const NamedCollection::Key & key) const;
template Float64 NamedCollection::get<Float64>(const NamedCollection::Key & key) const;
template bool NamedCollection::get<bool>(const NamedCollection::Key & key) const;

template String NamedCollection::getOrDefault<String>(const NamedCollection::Key & key, const String & default_value) const;
template UInt64 NamedCollection::getOrDefault<UInt64>(const NamedCollection::Key & key, const UInt64 & default_value) const;
template Int64 NamedCollection::getOrDefault<Int64>(const NamedCollection::Key & key, const Int64 & default_value) const;
template Float64 NamedCollection::getOrDefault<Float64>(const NamedCollection::Key & key, const Float64 & default_value) const;
template bool NamedCollection::getOrDefault<bool>(const NamedCollection::Key & key, const bool & default_value) const;

template String NamedCollection::getAny<String>(const std::initializer_list<NamedCollection::Key> & key) const;
template UInt64 NamedCollection::getAny<UInt64>(const std::initializer_list<NamedCollection::Key> & key) const;
template Int64 NamedCollection::getAny<Int64>(const std::initializer_list<NamedCollection::Key> & key) const;
template Float64 NamedCollection::getAny<Float64>(const std::initializer_list<NamedCollection::Key> & key) const;
template bool NamedCollection::getAny<bool>(const std::initializer_list<NamedCollection::Key> & key) const;

template String NamedCollection::getAnyOrDefault<String>(const std::initializer_list<NamedCollection::Key> & key, const String & default_value) const;
template UInt64 NamedCollection::getAnyOrDefault<UInt64>(const std::initializer_list<NamedCollection::Key> & key, const UInt64 & default_value) const;
template Int64 NamedCollection::getAnyOrDefault<Int64>(const std::initializer_list<NamedCollection::Key> & key, const Int64 & default_value) const;
template Float64 NamedCollection::getAnyOrDefault<Float64>(const std::initializer_list<NamedCollection::Key> & key, const Float64 & default_value) const;
template bool NamedCollection::getAnyOrDefault<bool>(const std::initializer_list<NamedCollection::Key> & key, const bool & default_value) const;

template void
NamedCollection::set<String, true>(const NamedCollection::Key & key, const String & value, const std::optional<bool> is_overridable);
template void
NamedCollection::set<String, false>(const NamedCollection::Key & key, const String & value, const std::optional<bool> is_overridable);
template void
NamedCollection::set<UInt64, true>(const NamedCollection::Key & key, const UInt64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::set<UInt64, false>(const NamedCollection::Key & key, const UInt64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::set<Int64, true>(const NamedCollection::Key & key, const Int64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::set<Int64, false>(const NamedCollection::Key & key, const Int64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::set<Float64, true>(const NamedCollection::Key & key, const Float64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::set<Float64, false>(const NamedCollection::Key & key, const Float64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::set<bool, false>(const NamedCollection::Key & key, const bool & value, const std::optional<bool> is_overridable);

template void NamedCollection::setOrUpdate<String, true>(
    const NamedCollection::Key & key, const String & value, const std::optional<bool> is_overridable);
template void NamedCollection::setOrUpdate<String, false>(
    const NamedCollection::Key & key, const String & value, const std::optional<bool> is_overridable);
template void NamedCollection::setOrUpdate<UInt64, true>(
    const NamedCollection::Key & key, const UInt64 & value, const std::optional<bool> is_overridable);
template void NamedCollection::setOrUpdate<UInt64, false>(
    const NamedCollection::Key & key, const UInt64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::setOrUpdate<Int64, true>(const NamedCollection::Key & key, const Int64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::setOrUpdate<Int64, false>(const NamedCollection::Key & key, const Int64 & value, const std::optional<bool> is_overridable);
template void NamedCollection::setOrUpdate<Float64, true>(
    const NamedCollection::Key & key, const Float64 & value, const std::optional<bool> is_overridable);
template void NamedCollection::setOrUpdate<Float64, false>(
    const NamedCollection::Key & key, const Float64 & value, const std::optional<bool> is_overridable);
template void
NamedCollection::setOrUpdate<bool, false>(const NamedCollection::Key & key, const bool & value, const std::optional<bool> is_overridable);

template void NamedCollection::remove<true>(const Key & key);
template void NamedCollection::remove<false>(const Key & key);

template NamedCollection::const_iterator NamedCollection::begin<true>() const;
template NamedCollection::const_iterator NamedCollection::begin<false>() const;
template NamedCollection::const_iterator NamedCollection::end<true>() const;
template NamedCollection::const_iterator NamedCollection::end<false>() const;
}
