#pragma once
#include <Databases/IDatabase.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>

namespace DB
{

template <int Length>
using StringLiteral = const char(&)[Length];

template<typename StorageT, int CommentSize, bool with_description, typename... StorageArgs>
void attachImpl(ContextPtr context, IDatabase & system_database, const String & table_name, StringLiteral<CommentSize> comment, StorageArgs && ... args)
{
    static_assert(CommentSize > 15, "The comment for a system table is too short or empty");
    assert(system_database.getDatabaseName() == DatabaseCatalog::SYSTEM_DATABASE);

    auto table_id = StorageID::createEmpty();
    if (system_database.getUUID() == UUIDHelpers::Nil)
    {
        /// Attach to Ordinary database.
        table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name);
        if constexpr (with_description)
            system_database.attachTable(context, table_name, std::make_shared<StorageT>(table_id, StorageT::getColumnsDescription(), std::forward<StorageArgs>(args)...));
        else
            system_database.attachTable(context, table_name, std::make_shared<StorageT>(table_id, std::forward<StorageArgs>(args)...));
    }
    else
    {
        /// Attach to Atomic database.
        /// NOTE: UUIDs are not persistent, but it's ok since no data are stored on disk for these storages
        /// and path is actually not used
        table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name, UUIDHelpers::generateV4());
        DatabaseCatalog::instance().addUUIDMapping(table_id.uuid);
        String path = "store/" + DatabaseCatalog::getPathForUUID(table_id.uuid);
        if constexpr (with_description)
            system_database.attachTable(context, table_name, std::make_shared<StorageT>(table_id, StorageT::getColumnsDescription(), std::forward<StorageArgs>(args)...), path);
        else
            system_database.attachTable(context, table_name, std::make_shared<StorageT>(table_id, std::forward<StorageArgs>(args)...), path);
    }

    /// Set the comment
    auto table = DatabaseCatalog::instance().getTable(table_id, context);
    assert(table);
    auto metadata = table->getInMemoryMetadata();
    metadata.comment = comment;
    table->setInMemoryMetadata(metadata);
}


template<typename StorageT, int CommentSize, typename... StorageArgs>
void attach(ContextPtr context, IDatabase & system_database, const String & table_name, StringLiteral<CommentSize> comment, StorageArgs && ... args)
{
    attachImpl<StorageT, CommentSize, true>(context, system_database, table_name, comment, std::forward<StorageArgs>(args)...);
}

template<typename StorageT, int CommentSize, typename... StorageArgs>
void attachNoDescription(ContextPtr context, IDatabase & system_database, const String & table_name, StringLiteral<CommentSize> comment, StorageArgs && ... args)
{
    attachImpl<StorageT, CommentSize, false>(context, system_database, table_name, comment, std::forward<StorageArgs>(args)...);
}

}
