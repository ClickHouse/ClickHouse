#pragma once
#include <Interpreters/Context.h>
#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>


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
    using Keys = std::set<Key, std::less<>>;
    enum class SourceId : uint8_t
    {
        /// None source_id is possible only if the object is a
        /// duplicate of some named collection. See `duplicate` method.
        NONE = 0,
        CONFIG = 1,
        SQL = 2,
    };

    bool has(const Key & key) const;

    bool hasAny(const std::initializer_list<Key> & keys) const;

    template <typename T> T get(const Key & key) const;

    template <typename T> T getOrDefault(const Key & key, const T & default_value) const;

    template <typename T> T getAny(const std::initializer_list<Key> & keys) const;

    template <typename T> T getAnyOrDefault(const std::initializer_list<Key> & keys, const T & default_value) const;

    std::unique_lock<std::mutex> lock();

    template <typename T, bool locked = false>
    void set(const Key & key, const T & value, std::optional<bool> is_overridable);

    template <typename T, bool locked = false>
    void setOrUpdate(const Key & key, const T & value, std::optional<bool> is_overridable);

    bool isOverridable(const Key & key, bool default_value) const;

    template <bool locked = false> void remove(const Key & key);

    /// Creates mutable, with NONE source id full copy.
    MutableNamedCollectionPtr duplicate() const;

    Keys getKeys(ssize_t depth = -1, const std::string & prefix = "") const;

    using iterator = typename Keys::iterator;
    using const_iterator = typename Keys::const_iterator;

    template <bool locked = false> const_iterator begin() const;

    template <bool locked = false> const_iterator end() const;

    std::string dumpStructure() const;

    bool isMutable() const { return is_mutable; }

    virtual SourceId getSourceId() const { return SourceId::NONE; }

    virtual String getCreateStatement(bool /*show_secrects*/) { return  {}; }

    virtual void update(const ASTAlterNamedCollectionQuery & query);

    virtual ~NamedCollection();

protected:
    class Impl;
    using ImplPtr = std::unique_ptr<Impl>;
    NamedCollection(
        ImplPtr pimpl_,
        const std::string & collection_name,
        bool is_mutable_
    );

    void assertMutable() const;


    ImplPtr pimpl;
    const std::string collection_name;
    const bool is_mutable;
    mutable std::mutex mutex;
};

class NamedCollectionFromSQL final : public NamedCollection
{
public:
    static MutableNamedCollectionPtr create(const ASTCreateNamedCollectionQuery & query);

    String getCreateStatement(bool show_secrects) override;

    void update(const ASTAlterNamedCollectionQuery & query) override;

    NamedCollection::SourceId getSourceId() const override { return SourceId::SQL; }

private:
    explicit NamedCollectionFromSQL(const ASTCreateNamedCollectionQuery & query_);

    ASTCreateNamedCollectionQuery create_query_ptr;
};

class NamedCollectionFromConfig final : public NamedCollection
{
public:

    static MutableNamedCollectionPtr create(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & collection_name,
        const std::string & collection_path,
        const Keys & keys);

    String getCreateStatement(bool /*show_secrects*/) override { return {}; }

    void update(const ASTAlterNamedCollectionQuery & /*query*/) override { NamedCollection::assertMutable(); }

    NamedCollection::SourceId getSourceId() const override { return SourceId::CONFIG; }

private:

    NamedCollectionFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & collection_name,
        const std::string & collection_path,
        const Keys & keys);
};

}
