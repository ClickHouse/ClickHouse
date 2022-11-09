#pragma once

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

class NamedCollection;
using NamedCollectionPtr = std::shared_ptr<const NamedCollection>;
struct NamedCollectionValueInfo;
using NamedCollectionInfo = std::unordered_map<std::string, NamedCollectionValueInfo>;

/**
 * A factory of immutable named collections.
 * Named collections are defined in server config as arbitrary
 * structure configurations:
 * <named_collections>
 *     <collection1>
 *         ...
 *     </collection1>
 *     ...
 * </named_collections>
 * In order to get a named collection, you need to know it's name
 * and expected structure of the collection defined as NamedCollectionInfo.
 */
class NamedCollectionFactory : boost::noncopyable
{
public:
    void initialize(const Poco::Util::AbstractConfiguration & server_config);

    bool exists(const std::string & collection_name) const;

    NamedCollectionPtr get(
        const std::string & collection_name,
        const NamedCollectionInfo & collection_info) const;

    NamedCollectionPtr tryGet(
        const std::string & collection_name,
        const NamedCollectionInfo & collection_info) const;

    static NamedCollectionFactory & instance();

private:
    NamedCollectionPtr getImpl(
        const std::string & collection_name,
        const NamedCollectionInfo & collection_info,
        std::lock_guard<std::mutex> & lock) const;

    using NamedCollections = std::unordered_map<std::string, NamedCollectionPtr>;
    mutable NamedCollections named_collections;

private:
    const Poco::Util::AbstractConfiguration * config;

    void assertInitialized(std::lock_guard<std::mutex> & lock) const;

    bool is_initialized = false;
    mutable std::mutex mutex;
};


class NamedCollection
{
friend class NamedCollectionFactory;

private:
    struct Impl;
    using ImplPtr = std::unique_ptr<Impl>;

    ImplPtr pimpl;

public:
    using Key = std::string;
    using Value = Field;

    Value get(const Key & key) const;

    void replace(const Key & key, const Value & value);

    /// Copy current named collection to allow modification as
    /// NamedConnectionFactory returns immutable collections.
    std::shared_ptr<NamedCollection> copy() const;

    NamedCollection();
    explicit NamedCollection(ImplPtr pimpl_);

protected:
    /// Initialize from config. `config` must be a view to the required collection,
    /// e.g. the root of `config` is the root of collection.
    void initialize(
        const Poco::Util::AbstractConfiguration & config,
        const NamedCollectionInfo & collection_info);

    /// Validate named collection in config.
    /// Throws exception if named collection keys in config are not the same as
    /// expected (contains unknown keys or misses required keys)
    void validate(
        const Poco::Util::AbstractConfiguration & config,
        const NamedCollectionInfo & collection_info) const;
};


/**
 * Named collection info which allows to parse config.
 * Contains a mapping key_path -> value_info.
 */
struct NamedCollectionValueInfo
{
    /// Type of the value. One of: String, UInt64, Int64, Double.
    Field::Types::Which type;
    /// Optional default value for the case if there is no such key in config.
    std::optional<NamedCollection::Value> default_value;
    /// Is this value required or optional? Throw exception if the value is
    /// required, but is not specified in config.
    bool is_required = true;
};

}
