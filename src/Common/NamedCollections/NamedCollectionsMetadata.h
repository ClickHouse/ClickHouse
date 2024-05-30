#pragma once
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/NamedCollections/NamedCollections.h>

namespace DB
{

class NamedCollectionsMetadata : private WithContext
{
public:
    static std::unique_ptr<NamedCollectionsMetadata> create(const ContextPtr & context);

    ~NamedCollectionsMetadata() = default;

    NamedCollectionsMap getAll() const;

    MutableNamedCollectionPtr get(const std::string & collection_name) const;

    MutableNamedCollectionPtr create(const ASTCreateNamedCollectionQuery & query);

    void remove(const std::string & collection_name);

    bool removeIfExists(const std::string & collection_name);

    void update(const ASTAlterNamedCollectionQuery & query);

    class INamedCollectionsStorage;
    NamedCollectionsMetadata(std::shared_ptr<INamedCollectionsStorage> storage_, ContextPtr context_)
        : WithContext(context_)
        , storage(std::move(storage_)) {}
    /// FIXME: It should be a protected constructor, but I failed make create() method a proper friend.

private:
    class LocalStorage;
    class ZooKeeperStorage;

    std::shared_ptr<INamedCollectionsStorage> storage;

    std::vector<std::string> listCollections() const;

    ASTCreateNamedCollectionQuery readCreateQuery(const std::string & collection_name) const;

    void writeCreateQuery(const ASTCreateNamedCollectionQuery & query, bool replace = false);
};


}
