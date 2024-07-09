#pragma once
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

class NamedCollectionsMetadataStorage : private WithContext
{
public:
    static std::unique_ptr<NamedCollectionsMetadataStorage> create(const ContextPtr & context);

    NamedCollectionsMap getAll() const;

    MutableNamedCollectionPtr get(const std::string & collection_name) const;

    MutableNamedCollectionPtr create(const ASTCreateNamedCollectionQuery & query);

    void remove(const std::string & collection_name);

    bool removeIfExists(const std::string & collection_name);

    void update(const ASTAlterNamedCollectionQuery & query);

    void shutdown();

    /// Return true if update was made
    bool waitUpdate();

    bool supportsPeriodicUpdate() const;

private:
    class INamedCollectionsStorage;
    class LocalStorage;
    class ZooKeeperStorage;

    std::shared_ptr<INamedCollectionsStorage> storage;

    NamedCollectionsMetadataStorage(std::shared_ptr<INamedCollectionsStorage> storage_, ContextPtr context_);

    std::vector<std::string> listCollections() const;

    ASTCreateNamedCollectionQuery readCreateQuery(const std::string & collection_name) const;

    void writeCreateQuery(const ASTCreateNamedCollectionQuery & query, bool replace = false);
};


}
