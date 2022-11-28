#pragma once

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

class NamedCollection;
using NamedCollectionPtr = std::shared_ptr<const NamedCollection>;

/**
 * Class to represent arbitrary-structured named collection object.
 * It can be defined via config or via SQL command.
 * <named_collections>
 *     <collection1>
 *         ...
 *     </collection1>
 *     ...
 * </named_collections>
 */
class NamedCollection
{
private:
    class Impl;
    using ImplPtr = std::unique_ptr<Impl>;

    ImplPtr pimpl;

public:
    using Key = std::string;
    using Keys = std::set<Key>;

    static NamedCollectionPtr create(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & collection_name);

    NamedCollection(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & collection_path,
        const Keys & keys);

    explicit NamedCollection(ImplPtr pimpl_);

    template <typename T> T get(const Key & key) const;

    template <typename T> T getOrDefault(const Key & key, const T & default_value) const;

    template <typename T> void set(const Key & key, const T & value, bool update_if_exists = false);

    void remove(const Key & key);

    std::shared_ptr<NamedCollection> duplicate() const;

    Keys getKeys() const;

    std::string dumpStructure() const;
};

/**
 * A factory of immutable named collections.
 */
class NamedCollectionFactory : boost::noncopyable
{
public:
    static NamedCollectionFactory & instance();

    void initialize(const Poco::Util::AbstractConfiguration & config_);

    void reload(const Poco::Util::AbstractConfiguration & config_);

    bool exists(const std::string & collection_name) const;

    NamedCollectionPtr get(const std::string & collection_name) const;

    NamedCollectionPtr tryGet(const std::string & collection_name) const;

    void add(
        const std::string & collection_name,
        NamedCollectionPtr collection);

    void remove(const std::string & collection_name);

    using NamedCollections = std::unordered_map<std::string, NamedCollectionPtr>;
    NamedCollections getAll() const;

private:
    void assertInitialized(std::lock_guard<std::mutex> & lock) const;

    NamedCollectionPtr getImpl(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    bool existsUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    mutable NamedCollections loaded_named_collections;

    const Poco::Util::AbstractConfiguration * config;

    bool is_initialized = false;
    mutable std::mutex mutex;
};

}
