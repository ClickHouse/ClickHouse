#include <Access/DiskAccessStorage.h>
#include <Access/AccessEntityIO.h>
#include <Access/AccessChangesNotifier.h>
#include <Access/MemoryAccessStorage.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Access/InterpreterCreateUserQuery.h>
#include <Interpreters/Access/InterpreterShowGrantsQuery.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/range/adaptor/map.hpp>
#include <base/range.h>
#include <filesystem>
#include <fstream>
#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}


namespace
{
    /// Reads a file containing ATTACH queries and then parses it to build an access entity.
    AccessEntityPtr readEntityFile(const String & file_path)
    {
        /// Read the file.
        ReadBufferFromFile in{file_path};
        String file_contents;
        readStringUntilEOF(file_contents, in);

        /// Parse the file contents.
        return deserializeAccessEntity(file_contents, file_path);
    }


    AccessEntityPtr tryReadEntityFile(const String & file_path, LoggerPtr log)
    {
        try
        {
            return readEntityFile(file_path);
        }
        catch (...)
        {
            tryLogCurrentException(log);
            return nullptr;
        }
    }

    /// Writes ATTACH queries for building a specified access entity to a file.
    void writeEntityFile(const String & file_path, const IAccessEntity & entity)
    {
        String file_contents = serializeAccessEntity(entity);

        /// First we save *.tmp file and then we rename if everything's ok.
        auto tmp_file_path = std::filesystem::path{file_path}.replace_extension(".tmp");
        bool succeeded = false;
        SCOPE_EXIT(
        {
            if (!succeeded)
                (void)std::filesystem::remove(tmp_file_path);
        });

        /// Write the file.
        WriteBufferFromFile out{tmp_file_path.string()};
        out.write(file_contents.data(), file_contents.size());
        out.close();

        /// Rename.
        std::filesystem::rename(tmp_file_path, file_path);
        succeeded = true;
    }


    /// Converts a path to an absolute path and append it with a separator.
    String makeDirectoryPathCanonical(const String & directory_path)
    {
        auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
        if (canonical_directory_path.has_filename())
            canonical_directory_path += std::filesystem::path::preferred_separator;
        return canonical_directory_path;
    }


    /// Calculates the path to a file named <id>.sql for saving an access entity.
    String getEntityFilePath(const String & directory_path, const UUID & id)
    {
        return directory_path + toString(id) + ".sql";
    }


    /// Reads a map of name of access entity to UUID for access entities of some type from a file.
    std::vector<std::pair<UUID, String>> readListFile(const String & file_path)
    {
        ReadBufferFromFile in(file_path);

        size_t num;
        readVarUInt(num, in);
        std::vector<std::pair<UUID, String>> id_name_pairs;
        id_name_pairs.reserve(num);

        for (size_t i = 0; i != num; ++i)
        {
            String name;
            readStringBinary(name, in);
            UUID id;
            readUUIDText(id, in);
            id_name_pairs.emplace_back(id, std::move(name));
        }

        return id_name_pairs;
    }


    /// Writes a map of name of access entity to UUID for access entities of some type to a file.
    void writeListFile(const String & file_path, const std::vector<std::pair<UUID, std::string_view>> & id_name_pairs)
    {
        WriteBufferFromFile out(file_path);
        writeVarUInt(id_name_pairs.size(), out);
        for (const auto & [id, name] : id_name_pairs)
        {
            writeStringBinary(name, out);
            writeUUIDText(id, out);
        }
        out.close();
    }


    /// Calculates the path for storing a map of name of access entity to UUID for access entities of some type.
    String getListFilePath(const String & directory_path, AccessEntityType type)
    {
        String file_name = AccessEntityTypeInfo::get(type).plural_raw_name;
        boost::to_lower(file_name);
        return directory_path + file_name + ".list";
    }


    /// Calculates the path to a temporary file which existence means that list files are corrupted
    /// and need to be rebuild.
    String getNeedRebuildListsMarkFilePath(const String & directory_path)
    {
        return directory_path + "need_rebuild_lists.mark";
    }


    bool tryParseUUID(const String & str, UUID & id)
    {
        return tryParse(id, str);
    }
}


DiskAccessStorage::DiskAccessStorage(const String & storage_name_, const String & directory_path_, AccessChangesNotifier & changes_notifier_, bool readonly_, bool allow_backup_)
    : IAccessStorage(storage_name_), memory_storage(storage_name_, changes_notifier_, /* allow_backup_= */ true)
{
    directory_path = makeDirectoryPathCanonical(directory_path_);
    readonly = readonly_;
    backup_allowed = allow_backup_;

    std::error_code create_dir_error_code;
    std::filesystem::create_directories(directory_path, create_dir_error_code);

    if (!std::filesystem::exists(directory_path) || !std::filesystem::is_directory(directory_path) || create_dir_error_code)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Couldn't create directory {} reason: '{}'",
                        directory_path, create_dir_error_code.message());

    bool should_rebuild_lists = std::filesystem::exists(getNeedRebuildListsMarkFilePath(directory_path));
    if (!should_rebuild_lists)
    {
        if (!readLists())
            should_rebuild_lists = true;
    }

    if (should_rebuild_lists)
    {
        LOG_WARNING(getLogger(), "Recovering lists in directory {}", directory_path);
        reloadAllAndRebuildLists();
    }
}


DiskAccessStorage::~DiskAccessStorage()
{
    try
    {
        DiskAccessStorage::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void DiskAccessStorage::shutdown()
{
    stopListsWritingThread();

    {
        std::lock_guard lock{mutex};
        writeLists();
    }
}


String DiskAccessStorage::getStorageParamsJSON() const
{
    std::lock_guard lock{mutex};
    Poco::JSON::Object json;
    json.set("path", directory_path);
    bool readonly_loaded = readonly;
    if (readonly_loaded)
        json.set("readonly", Poco::Dynamic::Var{true});
    std::ostringstream oss;         // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}


bool DiskAccessStorage::isPathEqual(const String & directory_path_) const
{
    return getPath() == makeDirectoryPathCanonical(directory_path_);
}


bool DiskAccessStorage::readLists()
{
    std::vector<std::pair<UUID, AccessEntityPtr>> ids_entities;

    for (auto type : collections::range(AccessEntityType::MAX))
    {
        auto file_path = getListFilePath(directory_path, type);
        if (!std::filesystem::exists(file_path))
        {
            LOG_WARNING(getLogger(), "File {} doesn't exist", file_path);
            return false;
        }

        try
        {
            for (auto & [id, name] : readListFile(file_path))
                ids_entities.emplace_back(id, std::make_shared<EntityOnDisk>(std::move(name), type));
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Could not read " + file_path);
            return false;
        }
    }

    memory_storage.removeAllExcept({});
    /// This entities are not fully loaded yet, do not send notifications to AccessChangesNotifier
    memory_storage.setAll(ids_entities, /* notify= */ false);

    return true;
}


void DiskAccessStorage::writeLists()
{
    if (failed_to_write_lists)
        return; /// We don't try to write list files after the first fail.
                /// The next restart of the server will invoke rebuilding of the list files.

    if (types_of_lists_to_write.empty())
        return;

    for (const auto & type : types_of_lists_to_write)
    {
        auto file_path = getListFilePath(directory_path, type);
        try
        {
            std::vector<std::pair<UUID, std::string_view>> id_name_pairs;
            std::vector<std::pair<UUID, std::shared_ptr<const IAccessEntity>>> all_entities = memory_storage.readAllWithIDs(type);
            id_name_pairs.reserve(all_entities.size());
            for (const auto & [id, entity] : all_entities)
                id_name_pairs.emplace_back(id, entity->getName());
            writeListFile(file_path, id_name_pairs);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Could not write " + file_path);
            failed_to_write_lists = true;
            types_of_lists_to_write.clear();
            return;
        }
    }

    /// The list files were successfully written.
    if (!has_stale_files_on_disk)
        (void)std::filesystem::remove(getNeedRebuildListsMarkFilePath(directory_path));
    types_of_lists_to_write.clear();
}


void DiskAccessStorage::scheduleWriteLists(AccessEntityType type)
{
    if (failed_to_write_lists)
        return; /// We don't try to write list files after the first fail.
                /// The next restart of the server will invoke rebuilding of the list files.

    types_of_lists_to_write.insert(type);

    if (lists_writing_thread_is_waiting)
        return; /// If the lists' writing thread is still waiting we can update `types_of_lists_to_write` easily,
                /// without restarting that thread.

    if (lists_writing_thread && lists_writing_thread->joinable())
        lists_writing_thread->join();

    /// Create the 'need_rebuild_lists.mark' file.
    /// This file will be used later to find out if writing lists is successful or not.
    std::ofstream out{getNeedRebuildListsMarkFilePath(directory_path)};
    out.close();

    lists_writing_thread = std::make_unique<ThreadFromGlobalPool>(&DiskAccessStorage::listsWritingThreadFunc, this);
    lists_writing_thread_is_waiting = true;
}


void DiskAccessStorage::listsWritingThreadFunc()
{
    std::unique_lock lock{mutex};

    {
        /// It's better not to write the lists files too often, that's why we need
        /// the following timeout.
        const auto timeout = std::chrono::minutes(1);
        SCOPE_EXIT({ lists_writing_thread_is_waiting = false; });
        if (lists_writing_thread_should_exit.wait_for(lock, timeout) != std::cv_status::timeout)
            return; /// The destructor requires us to exit.
    }

    writeLists();
}


void DiskAccessStorage::stopListsWritingThread()
{
    if (lists_writing_thread && lists_writing_thread->joinable())
    {
        lists_writing_thread_should_exit.notify_one();
        lists_writing_thread->join();
    }
}


/// Reads and parses all the "<id>.sql" files from a specified directory
/// and then saves the files "users.list", "roles.list", etc. to the same directory.
void DiskAccessStorage::reloadAllAndRebuildLists()
{
    struct LoadedEntity
    {
        UUID id;
        AccessEntityPtr entity;
        std::filesystem::path path;
        std::filesystem::file_time_type mtime;
    };
    std::map<std::pair<AccessEntityType, String>, LoadedEntity> loaded_entities;
    std::vector<std::filesystem::path> files_to_remove;

    /// Iterate through the access directory: load <uuid>.sql files, find stale files for removal.
    for (const auto & directory_entry : std::filesystem::directory_iterator(directory_path))
    {
        if (!directory_entry.is_regular_file())
            continue;

        const auto & path = directory_entry.path();
        UUID id;
        if (!tryParseUUID(path.stem(), id))
            continue; /// Not an access-entity file (e.g. `users.list`, `need_rebuild_lists.mark`).

        if (path.extension() == ".tmp")
        {
            /// writeEntityFile() created a tmp file but failed to rename it, so we try to remove it here.
            files_to_remove.push_back(path);
            continue;
        }

        if (path.extension() != ".sql")
            continue;

        const auto access_entity_file_path = getEntityFilePath(directory_path, id);
        auto entity = tryReadEntityFile(access_entity_file_path, getLogger());
        if (!entity)
            continue; /// Unparsable file; we leave it on disk for inspection.

        std::error_code mtime_ec;
        auto mtime = std::filesystem::last_write_time(path, mtime_ec);
        if (mtime_ec)
            LOG_WARNING(getLogger(), "Failed to stat {}: {}", path.string(), mtime_ec.message());

        auto key = std::make_pair(entity->getType(), entity->getName());
        auto it = loaded_entities.find(key);

        if (it == loaded_entities.end())
        {
            loaded_entities.emplace(key, LoadedEntity{id, entity, path, mtime});
        }
        else
        {
            /// Two files <uuid_1>.sql and <uuid_2>.sql claim the same name and type.
            /// Such duplicates can appear after `insertNoLock` crashes between renaming the new
            /// `<id>.tmp` and deleting `<old_id>.sql`.
            /// Here we keep the newer file by mtime and remove the older file.
            if (mtime > it->second.mtime)
            {
                LOG_WARNING(getLogger(), "Duplicate {} {} on disk; keeping newer file {}, removing older {}",
                    AccessEntityTypeInfo::get(entity->getType()).name, entity->getName(),
                    path.string(), it->second.path.string());
                files_to_remove.push_back(it->second.path);
                it->second = LoadedEntity{id, entity, path, mtime};
            }
            else
            {
                LOG_WARNING(getLogger(), "Duplicate {} {} on disk; keeping newer file {}, removing older {}",
                    AccessEntityTypeInfo::get(entity->getType()).name, entity->getName(),
                    it->second.path.string(), path.string());
                files_to_remove.push_back(path);
            }
        }
    }

    std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
    all_entities.reserve(loaded_entities.size());
    for (auto & [_, loaded] : loaded_entities)
        all_entities.emplace_back(loaded.id, loaded.entity);

    memory_storage.setAll(all_entities);

    /// Mark every entity type as needing its `.list` file rewritten so the next `writeLists`
    /// regenerates the full index.
    for (auto type : collections::range(AccessEntityType::MAX))
        types_of_lists_to_write.insert(type);

    /// Write the lists and keep the rebuild marker for now.
    failed_to_write_lists = false;
    has_stale_files_on_disk = true;
    writeLists();

    /// Now that the canonical `.list` files are persisted, it's safe to delete the orphan tmp
    /// files and older duplicate `.sql` files.
    if (!failed_to_write_lists)
    {
        bool stale_files_removed = true;
        for (const auto & p : files_to_remove)
        {
            std::error_code ec;
            std::filesystem::remove(p, ec);
            if (ec)
            {
                LOG_WARNING(getLogger(), "Failed to remove file {}: {}", p.string(), ec.message());
                stale_files_removed = false;
            }
        }

        if (stale_files_removed)
        {
            /// Disk is now consistent: list files are persisted, orphan tmps and older
            /// duplicates are gone. Clear the flag and drop the marker that writeLists()
            /// preserved while we did the cleanup.
            (void)std::filesystem::remove(getNeedRebuildListsMarkFilePath(directory_path));
            has_stale_files_on_disk = false;
        }
    }
}


void DiskAccessStorage::reload(ReloadMode reload_mode)
{
    if (reload_mode != ReloadMode::ALL)
        return;

    std::lock_guard lock{mutex};
    reloadAllAndRebuildLists();
}


std::optional<UUID> DiskAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::lock_guard lock{mutex};
    return memory_storage.find(type, name);
}


std::vector<UUID> DiskAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::lock_guard lock{mutex};
    return memory_storage.findAll(type);
}

bool DiskAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return memory_storage.exists(id);
}


bool DiskAccessStorage::isNotLoadedFromDisk(const AccessEntityPtr & entity)
{
    return static_cast<bool>(std::dynamic_pointer_cast<const EntityOnDisk>(entity));
}


AccessEntityPtr DiskAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock{mutex};
    auto entity = memory_storage.read(id, /* throw_if_not_exists= */ false);
    if (!entity)
    {
        if (throw_if_not_exists)
            throwNotFound(id, getStorageName());
        else
            return nullptr;
    }

    if (isNotLoadedFromDisk(entity))
        entity = readAccessEntityFromDisk(id);

    /// Will replace existing EntityOnDisk with actual entity
    memory_storage.insert(id, entity, /* replace_if_exists= */ true, /* throw_if_exists= */ false, /* conflicting_id= */ nullptr);

    return entity;
}


std::optional<std::pair<String, AccessEntityType>> DiskAccessStorage::readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::lock_guard lock{mutex};
    auto entry = memory_storage.read(id, throw_if_not_exists);
    if (!entry)
        return std::nullopt;

    return std::make_pair(entry->getName(), entry->getType());
}


bool DiskAccessStorage::insertImpl(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id)
{
    std::lock_guard lock{mutex};
    return insertNoLock(id, new_entity, replace_if_exists, throw_if_exists, conflicting_id, /* write_on_disk = */ true);
}


bool DiskAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id, bool write_on_disk)
{
    const AccessEntityType type = new_entity->getType();
    const String & name = new_entity->getName();

    if (readonly)
        throwReadonlyCannotInsert(type, name);

    /// Step 1: Validate against memory_storage without mutating it.
    auto id_by_name = memory_storage.find(type, name);
    bool name_collision = id_by_name.has_value();
    if (name_collision && !replace_if_exists)
    {
        if (throw_if_exists)
            throwNameCollisionCannotInsert(type, name, getStorageName());
        if (conflicting_id)
            *conflicting_id = *id_by_name;
        return false;
    }

    bool id_collision = memory_storage.exists(id);
    if (id_collision && !replace_if_exists)
    {
        if (throw_if_exists)
        {
            auto existing = memory_storage.read(id, /* throw_if_not_exists= */ true);
            throwIDCollisionCannotInsert(id, type, name,
                existing->getType(), existing->getName(), getStorageName());
        }
        if (conflicting_id)
            *conflicting_id = id;
        return false;
    }

    std::optional<UUID> old_id_to_delete;
    if (name_collision && (id_by_name != id))
        old_id_to_delete = *id_by_name;

    /// Step 2: Modify files first.
    if (write_on_disk)
    {
        scheduleWriteLists(type);

        /// Write <id>.tmp and atomically rename it to <id>.sql
        writeAccessEntityToDisk(id, *new_entity);

        if (old_id_to_delete.has_value())
        {
            /// Remove conflicting entity <old_id>.sql (same name and type, different id).
            try
            {
                deleteAccessEntityOnDisk(*old_id_to_delete);
            }
            catch (...)
            {
                /// Failed to remove <old_id>.sql.
                /// However new entity <id>.sql has been already written on disk,
                /// so the operation can only be considered successful at this point.
                /// We can't atomically rename <id>.tmp to <id>.sql and delete <old_id>.sql at the same time,
                /// so best effort is to keep both <id>.sql and <old_id>.sql for now,
                /// and let reloadAllAndRebuildLists() resolve the conflict based on mtime on the next start.
                tryLogCurrentException(getLogger(),
                    "Failed to remove stale entity file for " + toString(*old_id_to_delete)
                    + " after replacing with " + toString(id));
                /// Keep `need_rebuild_lists.mark` so the next startup rebuilds and dedups by mtime.
                has_stale_files_on_disk = true;
            }
        }
    }

    /// Step 3: We modify memory_storage only if disk operation succeeded.
    if (!memory_storage.insert(id, new_entity, replace_if_exists,
                               /* throw_if_exists= */ false, conflicting_id))
    {
        /// We have already validated the operation against memory_storage at step 1,
        /// so this call of memory_storage.insert() must be successful.
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Failed to insert entity {} into memory_storage after committing disk write", toString(id));
    }

    return true;
}


bool DiskAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    std::lock_guard lock{mutex};
    return removeNoLock(id, throw_if_not_exists, /* write_on_disk= */ true);
}


bool DiskAccessStorage::removeNoLock(const UUID & id, bool throw_if_not_exists, bool write_on_disk)
{
    /// Step 1: Validate against memory_storage without mutating it.
    AccessEntityPtr entity = memory_storage.read(id, /* throw_if_not_exists= */ false);
    if (!entity)
    {
        if (throw_if_not_exists)
            throwNotFound(id, getStorageName());
        return false;
    }
    AccessEntityType type = entity->getType();

    if (readonly)
        throwReadonlyCannotRemove(type, entity->getName());

    /// Step 2: Modify files first.
    if (write_on_disk)
    {
        scheduleWriteLists(type);
        deleteAccessEntityOnDisk(id);
    }

    /// Step 3: We modify memory_storage only if disk operation succeeded.
    if (!memory_storage.remove(id, /* throw_if_not_exists= */ false))
    {
        /// We have already validated the operation against memory_storage at step 1,
        /// so this call of memory_storage.remove() must be successful.
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Entity {} disappeared from memory_storage during remove", toString(id));
    }

    return true;
}


bool DiskAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    std::lock_guard lock{mutex};
    return updateNoLock(id, update_func, throw_if_not_exists, /* write_on_disk= */ true);
}


bool DiskAccessStorage::updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists, bool write_on_disk)
{
    /// Step 1: Validate against memory_storage without mutating it.
    AccessEntityPtr old_entity = memory_storage.read(id, /* throw_if_not_exists= */ false);
    if (!old_entity)
    {
        if (throw_if_not_exists)
            throwNotFound(id, getStorageName());
        return false;
    }

    if (readonly)
        throwReadonlyCannotUpdate(old_entity->getType(), old_entity->getName());

    /// Materialize the placeholder before invoking update_func.
    if (isNotLoadedFromDisk(old_entity))
    {
        old_entity = readAccessEntityFromDisk(id);
        memory_storage.insert(id, old_entity, /* replace_if_exists= */ true, /* throw_if_exists= */ false, /* conflicting_id= */ nullptr);
    }

    AccessEntityPtr new_entity = update_func(old_entity, id);

    if (!new_entity->isTypeOf(old_entity->getType()))
        throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

    if (*new_entity == *old_entity)
        return true;   /// no-op; do not touch disk

    const bool name_changed = (new_entity->getName() != old_entity->getName());
    if (name_changed)
    {
        auto collision = memory_storage.find(new_entity->getType(), new_entity->getName());
        if (collision.has_value() && *collision != id)
            throwNameCollisionCannotRename(old_entity->getType(),
                old_entity->getName(), new_entity->getName(), getStorageName());
    }

    /// Step 2: Modify files first.
    if (write_on_disk)
    {
        if (name_changed)
            scheduleWriteLists(new_entity->getType());
        writeAccessEntityToDisk(id, *new_entity);   /// tmp + atomic rename overwrites <id>.sql
    }

    /// Step 3: We modify memory_storage only if disk operation succeeded.
    if (!memory_storage.update(id,
            [&](const AccessEntityPtr &, const UUID &) { return new_entity; },
            /* throw_if_not_exists= */ false))
    {
        /// We have already validated the operation against memory_storage at step 1,
        /// so this call of memory_storage.update() must be successful.
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Entity {} disappeared from memory_storage during update", toString(id));
    }

    return true;
}


AccessEntityPtr DiskAccessStorage::readAccessEntityFromDisk(const UUID & id) const
{
    return readEntityFile(getEntityFilePath(directory_path, id));
}


void DiskAccessStorage::writeAccessEntityToDisk(const UUID & id, const IAccessEntity & entity) const
{
    writeEntityFile(getEntityFilePath(directory_path, id), entity);
}


void DiskAccessStorage::deleteAccessEntityOnDisk(const UUID & id) const
{
    auto file_path = getEntityFilePath(directory_path, id);
    if (!std::filesystem::remove(file_path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Couldn't delete {}", file_path);
}

}
