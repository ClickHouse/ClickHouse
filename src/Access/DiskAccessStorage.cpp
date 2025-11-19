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

    /// The list files was successfully written, we don't need the 'need_rebuild_lists.mark' file any longer.
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
    std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;

    for (const auto & directory_entry : std::filesystem::directory_iterator(directory_path))
    {
        if (!directory_entry.is_regular_file())
            continue;
        const auto & path = directory_entry.path();
        if (path.extension() != ".sql")
            continue;

        UUID id;
        if (!tryParseUUID(path.stem(), id))
            continue;

        const auto access_entity_file_path = getEntityFilePath(directory_path, id);
        auto entity = tryReadEntityFile(access_entity_file_path, getLogger());
        if (!entity)
            continue;

        all_entities.emplace_back(id, entity);
    }

    memory_storage.setAll(all_entities);

    for (auto type : collections::range(AccessEntityType::MAX))
        types_of_lists_to_write.insert(type);

    failed_to_write_lists = false; /// Try again writing lists.
    writeLists();
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
    /// Check that we can insert.
    if (readonly)
        throwReadonlyCannotInsert(new_entity->getType(), new_entity->getName());

    /// In case of name collision old file should be removed.
    if (replace_if_exists && write_on_disk)
    {
        std::optional<UUID> collision_id = memory_storage.find(new_entity->getType(), new_entity->getName());
        if (collision_id.has_value())
        {
            scheduleWriteLists(new_entity->getType());
            deleteAccessEntityOnDisk(collision_id.value());
        }
    }

    /// Do insertion.
    if (!memory_storage.insert(id, new_entity, replace_if_exists, throw_if_exists, conflicting_id))
        return false;

    /// Also rewrites existing file in case of id collision.
    if (write_on_disk)
    {
        scheduleWriteLists(new_entity->getType());
        writeAccessEntityToDisk(id, *new_entity);
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
    AccessEntityPtr entity = memory_storage.read(id, throw_if_not_exists);
    if (!entity)
    {
        if (throw_if_not_exists)
            throwNotFound(id, getStorageName());
        else
            return false;
    }
    AccessEntityType type = entity->getType();

    if (readonly)
        throwReadonlyCannotRemove(type, entity->getName());

    /// Do removing.
    memory_storage.remove(id, /* throw_if_not_exists= */ false);

    if (write_on_disk)
    {
        scheduleWriteLists(type);
        deleteAccessEntityOnDisk(id);
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
    AccessEntityPtr old_entity = memory_storage.read(id, throw_if_not_exists);
    if (!old_entity)
    {
        if (throw_if_not_exists)
            throwNotFound(id, getStorageName());
        else
            return false;
    }

    if (readonly)
        throwReadonlyCannotUpdate(old_entity->getType(), old_entity->getName());

    if (isNotLoadedFromDisk(old_entity))
    {
        old_entity = readAccessEntityFromDisk(id);
        memory_storage.insert(id, old_entity, /* replace_if_exists= */ true, /* throw_if_exists= */ false, /* conflicting_id= */ nullptr);
    }

    if (!memory_storage.update(id, update_func, throw_if_not_exists))
        return false;

    AccessEntityPtr new_entity = memory_storage.read(id, throw_if_not_exists);
    if (write_on_disk)
    {
        if (old_entity->getName() != new_entity->getName())
            scheduleWriteLists(new_entity->getType());
         writeAccessEntityToDisk(id, *new_entity);
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
