#include <Access/DiskAccessStorage.h>
#include <Access/AccessEntityIO.h>
#include <Access/AccessChangesNotifier.h>
#include <Backups/RestorerFromBackup.h>
#include <Backups/RestoreSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Access/InterpreterCreateUserQuery.h>
#include <Interpreters/Access/InterpreterShowGrantsQuery.h>
#include <Common/logger_useful.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/range/adaptor/map.hpp>
#include <base/range.h>
#include <filesystem>
#include <fstream>


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


    AccessEntityPtr tryReadEntityFile(const String & file_path, Poco::Logger & log)
    {
        try
        {
            return readEntityFile(file_path);
        }
        catch (...)
        {
            tryLogCurrentException(&log);
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
                std::filesystem::remove(tmp_file_path);
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
    : IAccessStorage(storage_name_), changes_notifier(changes_notifier_)
{
    directory_path = makeDirectoryPathCanonical(directory_path_);
    readonly = readonly_;
    backup_allowed = allow_backup_;

    std::error_code create_dir_error_code;
    std::filesystem::create_directories(directory_path, create_dir_error_code);

    if (!std::filesystem::exists(directory_path) || !std::filesystem::is_directory(directory_path) || create_dir_error_code)
        throw Exception("Couldn't create directory " + directory_path + " reason: '" + create_dir_error_code.message() + "'", ErrorCodes::DIRECTORY_DOESNT_EXIST);

    bool should_rebuild_lists = std::filesystem::exists(getNeedRebuildListsMarkFilePath(directory_path));
    if (!should_rebuild_lists)
    {
        if (!readLists())
            should_rebuild_lists = true;
    }

    if (should_rebuild_lists)
    {
        rebuildLists();
        writeLists();
    }
}


DiskAccessStorage::~DiskAccessStorage()
{
    stopListsWritingThread();

    try
    {
        writeLists();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
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


void DiskAccessStorage::clear()
{
    entries_by_id.clear();
    for (auto type : collections::range(AccessEntityType::MAX))
        entries_by_name_and_type[static_cast<size_t>(type)].clear();
}


bool DiskAccessStorage::readLists()
{
    clear();

    bool ok = true;
    for (auto type : collections::range(AccessEntityType::MAX))
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        auto file_path = getListFilePath(directory_path, type);
        if (!std::filesystem::exists(file_path))
        {
            LOG_WARNING(getLogger(), "File {} doesn't exist", file_path);
            ok = false;
            break;
        }

        try
        {
            for (const auto & [id, name] : readListFile(file_path))
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
            tryLogCurrentException(getLogger(), "Could not read " + file_path);
            ok = false;
            break;
        }
    }

    if (!ok)
        clear();
    return ok;
}


bool DiskAccessStorage::writeLists()
{
    if (failed_to_write_lists)
        return false; /// We don't try to write list files after the first fail.
                      /// The next restart of the server will invoke rebuilding of the list files.

    if (types_of_lists_to_write.empty())
        return true;

    for (const auto & type : types_of_lists_to_write)
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        auto file_path = getListFilePath(directory_path, type);
        try
        {
            std::vector<std::pair<UUID, std::string_view>> id_name_pairs;
            id_name_pairs.reserve(entries_by_name.size());
            for (const auto * entry : entries_by_name | boost::adaptors::map_values)
                id_name_pairs.emplace_back(entry->id, entry->name);
            writeListFile(file_path, id_name_pairs);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger(), "Could not write " + file_path);
            failed_to_write_lists = true;
            types_of_lists_to_write.clear();
            return false;
        }
    }

    /// The list files was successfully written, we don't need the 'need_rebuild_lists.mark' file any longer.
    std::filesystem::remove(getNeedRebuildListsMarkFilePath(directory_path));
    types_of_lists_to_write.clear();
    return true;
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

    if (lists_writing_thread.joinable())
        lists_writing_thread.join();

    /// Create the 'need_rebuild_lists.mark' file.
    /// This file will be used later to find out if writing lists is successful or not.
    std::ofstream out{getNeedRebuildListsMarkFilePath(directory_path)};
    out.close();

    lists_writing_thread = ThreadFromGlobalPool{&DiskAccessStorage::listsWritingThreadFunc, this};
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
    if (lists_writing_thread.joinable())
    {
        lists_writing_thread_should_exit.notify_one();
        lists_writing_thread.join();
    }
}


/// Reads and parses all the "<id>.sql" files from a specified directory
/// and then saves the files "users.list", "roles.list", etc. to the same directory.
bool DiskAccessStorage::rebuildLists()
{
    LOG_WARNING(getLogger(), "Recovering lists in directory {}", directory_path);
    clear();

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
        auto entity = tryReadEntityFile(access_entity_file_path, *getLogger());
        if (!entity)
            continue;

        const String & name = entity->getName();
        auto type = entity->getType();
        auto & entry = entries_by_id[id];
        entry.id = id;
        entry.type = type;
        entry.name = name;
        entry.entity = entity;
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        entries_by_name[entry.name] = &entry;
    }

    for (auto type : collections::range(AccessEntityType::MAX))
        types_of_lists_to_write.insert(type);

    return true;
}


std::optional<UUID> DiskAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::lock_guard lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it = entries_by_name.find(name);
    if (it == entries_by_name.end())
        return {};

    return it->second->id;
}


std::vector<UUID> DiskAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    std::vector<UUID> res;
    res.reserve(entries_by_name.size());
    for (const auto * entry : entries_by_name | boost::adaptors::map_values)
        res.emplace_back(entry->id);
    return res;
}

bool DiskAccessStorage::exists(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries_by_id.contains(id);
}


AccessEntityPtr DiskAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
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
        entry.entity = readAccessEntityFromDisk(id);
    return entry.entity;
}


std::optional<std::pair<String, AccessEntityType>> DiskAccessStorage::readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const
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
    return std::make_pair(it->second.name, it->second.type);
}


std::optional<UUID> DiskAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists)
{
    UUID id = generateRandomID();
    if (insertWithID(id, new_entity, replace_if_exists, throw_if_exists))
        return id;

    return std::nullopt;
}


bool DiskAccessStorage::insertWithID(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists)
{
    std::lock_guard lock{mutex};
    return insertNoLock(id, new_entity, replace_if_exists, throw_if_exists);
}


bool DiskAccessStorage::insertNoLock(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists)
{
    const String & name = new_entity->getName();
    AccessEntityType type = new_entity->getType();

    /// Check that we can insert.
    if (readonly)
        throwReadonlyCannotInsert(type, name);

    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it_by_name = entries_by_name.find(name);
    bool name_collision = (it_by_name != entries_by_name.end());

    if (name_collision && !replace_if_exists)
    {
        if (throw_if_exists)
            throwNameCollisionCannotInsert(type, name);
        else
            return false;
    }

    auto it_by_id = entries_by_id.find(id);
    if (it_by_id != entries_by_id.end())
    {
        const auto & existing_entry = it_by_id->second;
        throwIDCollisionCannotInsert(id, type, name, existing_entry.entity->getType(), existing_entry.entity->getName());
    }

    scheduleWriteLists(type);
    writeAccessEntityToDisk(id, *new_entity);

    if (name_collision && replace_if_exists)
        removeNoLock(it_by_name->second->id, /* throw_if_not_exists = */ false);

    /// Do insertion.
    auto & entry = entries_by_id[id];
    entry.id = id;
    entry.type = type;
    entry.name = name;
    entry.entity = new_entity;
    entries_by_name[entry.name] = &entry;

    changes_notifier.onEntityAdded(id, new_entity);
    return true;
}


bool DiskAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    std::lock_guard lock{mutex};
    return removeNoLock(id, throw_if_not_exists);
}


bool DiskAccessStorage::removeNoLock(const UUID & id, bool throw_if_not_exists)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return false;
    }

    Entry & entry = it->second;
    AccessEntityType type = entry.type;

    if (readonly)
        throwReadonlyCannotRemove(type, entry.name);

    scheduleWriteLists(type);
    deleteAccessEntityOnDisk(id);

    /// Do removing.
    UUID removed_id = id;
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    entries_by_name.erase(entry.name);
    entries_by_id.erase(it);

    changes_notifier.onEntityRemoved(removed_id, type);
    return true;
}


bool DiskAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    std::lock_guard lock{mutex};
    return updateNoLock(id, update_func, throw_if_not_exists);
}


bool DiskAccessStorage::updateNoLock(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return false;
    }

    Entry & entry = it->second;
    if (readonly)
        throwReadonlyCannotUpdate(entry.type, entry.name);

    if (!entry.entity)
        entry.entity = readAccessEntityFromDisk(id);
    auto old_entity = entry.entity;
    auto new_entity = update_func(old_entity);

    if (!new_entity->isTypeOf(old_entity->getType()))
        throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

    if (*new_entity == *old_entity)
        return true;

    const String & new_name = new_entity->getName();
    const String & old_name = old_entity->getName();
    const AccessEntityType type = entry.type;
    auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];

    bool name_changed = (new_name != old_name);
    if (name_changed)
    {
        if (entries_by_name.contains(new_name))
            throwNameCollisionCannotRename(type, old_name, new_name);
        scheduleWriteLists(type);
    }

    writeAccessEntityToDisk(id, *new_entity);
    entry.entity = new_entity;

    if (name_changed)
    {
        entries_by_name.erase(entry.name);
        entry.name = new_name;
        entries_by_name[entry.name] = &entry;
    }

    changes_notifier.onEntityUpdated(id, new_entity);

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
        throw Exception("Couldn't delete " + file_path, ErrorCodes::FILE_DOESNT_EXIST);
}


void DiskAccessStorage::restoreFromBackup(RestorerFromBackup & restorer)
{
    if (!isRestoreAllowed())
        throwRestoreNotAllowed();

    auto entities = restorer.getAccessEntitiesToRestore();
    if (entities.empty())
        return;

    auto create_access = restorer.getRestoreSettings().create_access;
    bool replace_if_exists = (create_access == RestoreAccessCreationMode::kReplace);
    bool throw_if_exists = (create_access == RestoreAccessCreationMode::kCreate);

    restorer.addDataRestoreTask([this, entities = std::move(entities), replace_if_exists, throw_if_exists]
    {
        for (const auto & [id, entity] : entities)
            insertWithID(id, entity, replace_if_exists, throw_if_exists);
    });
}

}
