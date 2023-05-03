#include <filesystem>
#include <fstream>
#include <Access/AccessEntityConvertor.h>
#include <Access/AccessEntityIO.h>
#include <Access/DiskAccessStorage.h>
#include <Access/SqlDrivenFDBAccessStorage.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <base/logger_useful.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/FoundationDB/FoundationDBCommon.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

SqlDrivenFDBAccessStorage::SqlDrivenFDBAccessStorage(
    const DB::String & local_directory_path_, const std::function<FoundationDBPtr()> & get_fdb_function_, bool readonly_)
    : SqlDrivenFDBAccessStorage(STORAGE_TYPE, local_directory_path_, get_fdb_function_, readonly_)
{
}

SqlDrivenFDBAccessStorage::SqlDrivenFDBAccessStorage(
    const DB::String & storage_name_,
    const DB::String & local_directory_path_,
    const std::function<FoundationDBPtr()> & get_fdb_function_,
    bool readonly_)
    : FDBAccessStorage(storage_name_, get_fdb_function_())
{
    local_directory_path = DiskAccessStorage::makeDirectoryPathCanonical(local_directory_path_);
    readonly = readonly_;
    scope = AccessEntityScope::SQL_DRIVEN;

    if (meta_store->isFirstBoot())
    {
        /// Node Startups firstly. Need to migrate Metadata from Local Disk into Foundationdb.
        try
        {
            migrateMetadataFromLocalToFDB();
        }
        catch (Exception &)
        {
            throw;
        }
    }
    else
    {
        /// Node doesn't belong to Startup firstly. Need to pull Metadata (Just Lists) from FoundationDB into Memory.
        readLists();
    }
}

SqlDrivenFDBAccessStorage::~SqlDrivenFDBAccessStorage() = default;

void SqlDrivenFDBAccessStorage::migrateMetadataFromLocalToFDB()
{
    LOG_WARNING(getLogger(), "Migrating access entities in local disk directory {}", local_directory_path);

    if (!std::filesystem::exists(local_directory_path) || !std::filesystem::is_directory(local_directory_path))
        /// To pass migration feature in functional test. There should be execute existence of directory.
        return;

    /// Clear old access entities in fdb.
    tryClearEntitiesOnFDB();

    /// parse *.sql which in local directory into memory and add Metadata (aka. access entities) into foundationdb.
    rebuildListsFromDisk();
}

void SqlDrivenFDBAccessStorage::clear()
{
    entries_by_id.clear();
    for (auto type : collections::range(AccessEntityType::MAX))
        entries_by_name_and_type[static_cast<size_t>(type)].clear();
}

/// Reads and parses all the "<id>.sql" files from a specified directory,
/// and build access entities in memory
/// finally saves the files "users_list", "roles_list", etc. to foundationdb.
void SqlDrivenFDBAccessStorage::rebuildListsFromDisk()
{
    LOG_WARNING(getLogger(), "Recovering lists in local directory {} and migrating into foundationdb.", local_directory_path);
    clear();

    for (const auto & local_directory_entry : std::filesystem::directory_iterator(local_directory_path))
    {
        if (!local_directory_entry.is_regular_file())
            continue;
        const auto & local_path = local_directory_entry.path();
        if (local_path.extension() != ".sql")
            continue;

        UUID id;
        if (!DiskAccessStorage::tryParseUUID(local_path.stem(), id))
            continue;

        const auto access_entity_local_file_path = DiskAccessStorage::getEntityFilePath(local_directory_path, id);
        auto entity = DiskAccessStorage::tryReadEntityFile(access_entity_local_file_path, *getLogger());
        if (!entity)
            continue;

        const String & name = entity->getName();
        auto type = entity->getType();
        auto & entry = entries_by_id[id];
        entry.id = id;
        entry.name = name;
        entry.entity = entity;
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        entries_by_name[entry.name] = &entry;

        /// Insert access entity to FoundationDB.
        tryInsertEntityToFDB(id, *entity);
    }

    /// The list files was successfully written, we don't need the 'need_rebuild_lists.mark' file any longer.
    std::filesystem::remove(DiskAccessStorage::getNeedRebuildListsMarkFilePath(local_directory_path));
}

void SqlDrivenFDBAccessStorage::readLists()
{
    clear();

    bool ok = true;
    for (auto type : collections::range(AccessEntityType::MAX))
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];

        try
        {
            for (const auto & [id, name] : tryReadListsByTypeFromFDB(type))
            {
                auto & entry = entries_by_id[id];
                entry.id = id;
                entry.type = type;
                entry.name = name;
                entries_by_name[entry.name] = &entry;
            }
        }
        catch (...)
        {
            ok = false;
            break;
        }
    }

    if (!ok)
        clear();
}

/// Reads a map of name of access entity to UUID for access entities of some type from KV-pair of FoundationDB.
std::vector<std::pair<UUID, std::string>> SqlDrivenFDBAccessStorage::tryReadListsByTypeFromFDB(const AccessEntityType & type) const
{
    try
    {
        return meta_store->getAccessEntityListsByType(scope, type);
    }
    catch (FoundationDBException & e)
    {
        e.addMessage(fmt::format("while read lists from FoundationDB happens error!"));
        throw;
    }
}

AccessEntityPtr SqlDrivenFDBAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return nullptr;
    }

    const auto & entry = it->second;
    if (!entry.entity)
        entry.entity = tryReadEntityFromFDB(id);
    return entry.entity;
}

std::optional<String> SqlDrivenFDBAccessStorage::readNameImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return std::nullopt;
    }
    return it->second.name;
}

std::optional<UUID> SqlDrivenFDBAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    UUID id = generateRandomID();
    /// Check that we can insert
    if (readonly)
        throwReadonlyCannotInsert(new_entity->getType(), new_entity->getName());

    std::lock_guard lock{mutex};
    if (insertNoLock(
            id, new_entity, replace_if_exists, throw_if_exists, notifications, [this](const UUID & id_, const IAccessEntity & entity_) {
                tryInsertEntityToFDB(id_, entity_);
            }))
        return id;

    return std::nullopt;
}

bool SqlDrivenFDBAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    auto entity = read(id, throw_if_not_exists);
    if (!entity)
        return false;
    if (readonly)
        throwReadonlyCannotRemove(entity->getType(), entity->getName());

    std::lock_guard lock{mutex};
    return removeNoLock(id, throw_if_not_exists, notifications, [this](const UUID & id_, const AccessEntityType & type_) {
        tryDeleteEntityOnFDB(id_, type_);
    });
}

bool SqlDrivenFDBAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    Notifications notifications;
    SCOPE_EXIT({ notify(notifications); });

    auto entity = read(id, throw_if_not_exists);
    if (!entity)
        return false;
    if (readonly)
        throwReadonlyCannotUpdate(entity->getType(), entity->getName());

    std::lock_guard lock{mutex};
    return updateNoLock(id, update_func, throw_if_not_exists, notifications, [this](const UUID & id_, const IAccessEntity & entity_) {
        tryUpdateEntityOnFDB(id_, entity_);
    });
}

bool SqlDrivenFDBAccessStorage::isPathEqual(const String & local_directory_path_) const
{
    return getPath() == DiskAccessStorage::makeDirectoryPathCanonical(local_directory_path_);
}
}
