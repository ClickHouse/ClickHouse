#pragma once
#include <Interpreters/Context.h>
#include <Storages/NamedCollections_fwd.h>
#include <Storages/NamedCollectionUtils.h>

namespace Poco { namespace Util { class AbstractConfiguration; } }

namespace DB
{

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
public:
    using Key = std::string;
    using Keys = std::set<Key>;
    using SourceId = NamedCollectionUtils::SourceId;

    static MutableNamedCollectionPtr create(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & collection_name,
        const std::string & collection_path,
        const Keys & keys,
        SourceId source_id_,
        bool is_mutable_);

    template <typename T> T get(const Key & key) const;

    template <typename T> T getOrDefault(const Key & key, const T & default_value) const;

    std::unique_lock<std::mutex> lock();

    template <typename T, bool locked = false> void set(const Key & key, const T & value);

    template <typename T, bool locked = false> void setOrUpdate(const Key & key, const T & value);

    template <bool locked = false> void remove(const Key & key);

    MutableNamedCollectionPtr duplicate() const;

    Keys getKeys() const;

    std::string dumpStructure() const;

    bool isMutable() const { return is_mutable; }

    SourceId getSourceId() const { return source_id; }

private:
    class Impl;
    using ImplPtr = std::unique_ptr<Impl>;

    NamedCollection(
        ImplPtr pimpl_,
        const std::string & collection_name,
        SourceId source_id,
        bool is_mutable);

    void assertMutable() const;

    ImplPtr pimpl;
    const std::string collection_name;
    const SourceId source_id;
    const bool is_mutable;
    mutable std::mutex mutex;
};

/**
 * A factory of immutable named collections.
 */
class NamedCollectionFactory : boost::noncopyable
{
public:
    static NamedCollectionFactory & instance();

    bool exists(const std::string & collection_name) const;

    NamedCollectionPtr get(const std::string & collection_name) const;

    NamedCollectionPtr tryGet(const std::string & collection_name) const;

    MutableNamedCollectionPtr getMutable(const std::string & collection_name) const;

    void add(const std::string & collection_name, MutableNamedCollectionPtr collection);

    void add(NamedCollectionsMap collections);

    void update(NamedCollectionsMap collections);

    void remove(const std::string & collection_name);

    void removeIfExists(const std::string & collection_name);

    void removeById(NamedCollectionUtils::SourceId id);

    NamedCollectionsMap getAll() const;

private:
    bool existsUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    MutableNamedCollectionPtr tryGetUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock) const;

    void addUnlocked(
        const std::string & collection_name,
        MutableNamedCollectionPtr collection,
        std::lock_guard<std::mutex> & lock);

    bool removeIfExistsUnlocked(
        const std::string & collection_name,
        std::lock_guard<std::mutex> & lock);

    mutable NamedCollectionsMap loaded_named_collections;

    mutable std::mutex mutex;
    bool is_initialized = false;
};


}
