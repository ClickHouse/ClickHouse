#pragma once
#include <Databases/IDatabase.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Core/UUID.h>

namespace DB
{

template <int Length>
using StringLiteral = const char(&)[Length];

/// All documentation comments passed through these helpers are defined in `attachSystemTables.cpp`. Documentation
/// constants defined elsewhere override this mapping after attachment.
extern const char * const ATTACHED_SYSTEM_TABLE_DOCUMENTATION_SOURCE;

template<typename StorageT, bool with_description, typename... StorageArgs>
void attachImpl(ContextPtr context, IDatabase & system_database, const String & table_name, std::string_view comment, StorageArgs && ... args)
{
    chassert(comment.size() > 15);
    chassert(system_database.getDatabaseName() == DatabaseCatalog::SYSTEM_DATABASE);

    auto table_id = StorageID::createEmpty();
    String path;
    if (system_database.getUUID() == UUIDHelpers::Nil)
    {
        /// Attach to Ordinary database.
        table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name);
    }
    else
    {
        /// Attach to Atomic database.
        /// NOTE: UUIDs are not persistent, but it's ok since no data are stored on disk for these storages
        /// and path is actually not used
        table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name, UUIDHelpers::generateV4());
        DatabaseCatalog::instance().addUUIDMapping(table_id.uuid);
        path = DatabaseCatalog::getStoreDirPath(table_id.uuid);
    }

    std::shared_ptr<StorageT> storage;
    if constexpr (with_description)
        storage = std::make_shared<StorageT>(table_id, StorageT::getColumnsDescription(), std::forward<StorageArgs>(args)...);
    else
        storage = std::make_shared<StorageT>(table_id, std::forward<StorageArgs>(args)...);

    /// Set the comment on the storage before attaching it, so that we neither look the table back up in
    /// `DatabaseCatalog` nor copy its whole metadata (including the full `ColumnsDescription`) an extra time.
    storage->setInMemoryMetadataComment(String(comment));

    system_database.attachTable(context, table_name, storage, path);
    registerSystemTableDocumentationSource(table_name, ATTACHED_SYSTEM_TABLE_DOCUMENTATION_SOURCE, comment);
}


template<typename StorageT, int CommentSize, typename... StorageArgs>
void attach(ContextPtr context, IDatabase & system_database, const String & table_name, StringLiteral<CommentSize> comment, StorageArgs && ... args)
{
    static_assert(CommentSize > 15, "The comment for a system table is too short or empty");
    attachImpl<StorageT, true>(context, system_database, table_name, comment, std::forward<StorageArgs>(args)...);
}

template<typename StorageT, int CommentSize, typename... StorageArgs>
void attachNoDescription(ContextPtr context, IDatabase & system_database, const String & table_name, StringLiteral<CommentSize> comment, StorageArgs && ... args)
{
    static_assert(CommentSize > 15, "The comment for a system table is too short or empty");
    attachImpl<StorageT, false>(context, system_database, table_name, comment, std::forward<StorageArgs>(args)...);
}

/// Overloads for a comment which is not a string literal at the call site, e.g. a documentation constant defined
/// next to the storage because the documentation has to be available even where the table is not attached.
template<typename StorageT, typename... StorageArgs>
void attach(ContextPtr context, IDatabase & system_database, const String & table_name, std::string_view comment, StorageArgs && ... args)
{
    attachImpl<StorageT, true>(context, system_database, table_name, comment, std::forward<StorageArgs>(args)...);
}

template<typename StorageT, typename... StorageArgs>
void attachNoDescription(ContextPtr context, IDatabase & system_database, const String & table_name, std::string_view comment, StorageArgs && ... args)
{
    attachImpl<StorageT, false>(context, system_database, table_name, comment, std::forward<StorageArgs>(args)...);
}

}
