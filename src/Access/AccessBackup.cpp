#include <Access/AccessBackup.h>
#include <Access/AccessControl.h>
#include <Access/AccessEntityIO.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
#include <Access/RowPolicy.h>
#include <Access/Quota.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackup.h>
#include <Backups/RestoreSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/UUIDGenerator.h>
#include <base/insertAtEnd.h>

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
}


namespace
{
    /// Represents a list of access entities as they're stored in a backup.
    struct AccessEntitiesInBackup
    {
        std::unordered_map<UUID, AccessEntityPtr> entities;
        std::unordered_map<UUID, std::pair<String, AccessEntityType>> dependencies;

        BackupEntryPtr toBackupEntry() const
        {
            WriteBufferFromOwnString buf;

            for (const auto & [id, entity] : entities)
            {
                writeText(id, buf);
                writeChar('\t', buf);
                writeText(entity->getTypeInfo().name, buf);
                writeChar('\t', buf);
                writeText(entity->getName(), buf);
                writeChar('\n', buf);
                writeText(serializeAccessEntity(*entity), buf);
                writeChar('\n', buf);
            }

            if (!dependencies.empty())
            {
                writeText("DEPENDENCIES\n", buf);
                for (const auto & [id, name_and_type] : dependencies)
                {
                    writeText(id, buf);
                    writeChar('\t', buf);
                    writeText(AccessEntityTypeInfo::get(name_and_type.second).name, buf);
                    writeChar('\t', buf);
                    writeText(name_and_type.first, buf);
                    writeChar('\n', buf);
                }
            }

            return std::make_shared<BackupEntryFromMemory>(buf.str());
        }

        static AccessEntitiesInBackup fromBackupEntry(std::unique_ptr<ReadBuffer> buf, const String & file_path)
        {
            try
            {
                AccessEntitiesInBackup res;

                bool dependencies_found = false;

                while (!buf->eof())
                {
                    String line;
                    readStringUntilNewlineInto(line, *buf);
                    buf->ignore();
                    if (line == "DEPENDENCIES")
                    {
                        dependencies_found = true;
                        break;
                    }

                    UUID id = parse<UUID>(line.substr(0, line.find('\t')));
                    line.clear();

                    String queries;
                    while (!buf->eof())
                    {
                        String query;
                        readStringUntilNewlineInto(query, *buf);
                        buf->ignore();
                        if (query.empty())
                            break;
                        if (!queries.empty())
                            queries.append("\n");
                        queries.append(query);
                    }

                    AccessEntityPtr entity = deserializeAccessEntity(queries);
                    res.entities.emplace(id, entity);
                }

                if (dependencies_found)
                {
                    while (!buf->eof())
                    {
                        String id_as_string;
                        readStringInto(id_as_string, *buf);
                        buf->ignore();
                        UUID id = parse<UUID>(id_as_string);

                        String type_as_string;
                        readStringInto(type_as_string, *buf);
                        buf->ignore();
                        AccessEntityType type = AccessEntityTypeInfo::parseType(type_as_string);

                        String name;
                        readStringInto(name, *buf);
                        buf->ignore();

                        if (!res.entities.contains(id))
                            res.dependencies.emplace(id, std::pair{name, type});
                    }
                }

                return res;
            }
            catch (Exception & e)
            {
                e.addMessage("While parsing " + file_path + " from backup");
                throw;
            }
        }
    };

    std::vector<UUID> findDependencies(const std::vector<std::pair<UUID, AccessEntityPtr>> & entities)
    {
        std::vector<UUID> res;
        for (const auto & entity : entities | boost::adaptors::map_values)
            insertAtEnd(res, entity->findDependencies());

        /// Remove duplicates in the list of dependencies (some entities can refer to other entities).
        ::sort(res.begin(), res.end());
        res.erase(std::unique(res.begin(), res.end()), res.end());
        for (const auto & id : entities | boost::adaptors::map_keys)
        {
            auto it = std::lower_bound(res.begin(), res.end(), id);
            if ((it != res.end()) && (*it == id))
                res.erase(it);
        }
        return res;
    }

    std::unordered_map<UUID, std::pair<String, AccessEntityType>> readDependenciesNamesAndTypes(const std::vector<UUID> & dependencies, const AccessControl & access_control)
    {
        std::unordered_map<UUID, std::pair<String, AccessEntityType>> res;
        for (const auto & id : dependencies)
        {
            if (auto name_and_type = access_control.tryReadNameWithType(id))
                res.emplace(id, name_and_type.value());
        }
        return res;
    }

    /// Checks if new entities (which we're going to restore) already exist,
    /// and either skips them or throws an exception depending on the restore settings.
    void checkExistingEntities(std::vector<std::pair<UUID, AccessEntityPtr>> & entities,
                               std::unordered_map<UUID, UUID> & old_to_new_id,
                               const AccessControl & access_control,
                               RestoreAccessCreationMode creation_mode)
    {
        if (creation_mode == RestoreAccessCreationMode::kReplace)
            return;

        auto should_skip = [&](const std::pair<UUID, AccessEntityPtr> & id_and_entity)
        {
            const auto & id = id_and_entity.first;
            const auto & entity = *id_and_entity.second;
            auto existing_id = access_control.find(entity.getType(), entity.getName());
            if (!existing_id)
            {
                return false;
            }
            else if (creation_mode == RestoreAccessCreationMode::kCreateIfNotExists)
            {
                old_to_new_id[id] = *existing_id;
                return true;
            }
            else
            {
                throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "Cannot restore {} because it already exists", entity.formatTypeWithName());
            }
        };

        std::erase_if(entities, should_skip);
    }

    /// If new entities (which we're going to restore) depend on other entities which are not going to be restored or not present in the backup
    /// then we should try to replace those dependencies with already existing entities.
    void resolveDependencies(const std::unordered_map<UUID, std::pair<String, AccessEntityType>> & dependencies,
                             std::unordered_map<UUID, UUID> & old_to_new_ids,
                             const AccessControl & access_control,
                             bool allow_unresolved_dependencies)
    {
        for (const auto & [id, name_and_type] : dependencies)
        {
            std::optional<UUID> new_id;
            if (allow_unresolved_dependencies)
                new_id = access_control.find(name_and_type.second, name_and_type.first);
            else
                new_id = access_control.getID(name_and_type.second, name_and_type.first);
            if (new_id)
                old_to_new_ids.emplace(id, *new_id);
        }
    }

    /// Generates random IDs for the new entities.
    void generateRandomIDs(std::vector<std::pair<UUID, AccessEntityPtr>> & entities, std::unordered_map<UUID, UUID> & old_to_new_ids)
    {
        Poco::UUIDGenerator generator;
        for (auto & [id, entity] : entities)
        {
            UUID new_id;
            generator.createRandom().copyTo(reinterpret_cast<char *>(&new_id));
            old_to_new_ids.emplace(id, new_id);
            id = new_id;
        }
    }

    /// Updates dependencies of the new entities using a specified map.
    void replaceDependencies(std::vector<std::pair<UUID, AccessEntityPtr>> & entities,
                             const std::unordered_map<UUID, UUID> & old_to_new_ids)
    {
        for (auto & entity : entities | boost::adaptors::map_values)
            IAccessEntity::replaceDependencies(entity, old_to_new_ids);
    }

    AccessRightsElements getRequiredAccessToRestore(const std::vector<std::pair<UUID, AccessEntityPtr>> & entities)
    {
        AccessRightsElements res;
        for (const auto & entity : entities | boost::adaptors::map_values)
        {
            auto entity_type = entity->getType();
            switch (entity_type)
            {
                case User::TYPE:
                {
                    const auto & user = typeid_cast<const User &>(*entity);
                    res.emplace_back(AccessType::CREATE_USER);
                    auto elements = user.access.getElements();
                    for (auto & element : elements)
                    {
                        if (element.is_partial_revoke)
                            continue;
                        element.grant_option = true;
                        res.emplace_back(element);
                    }
                    if (!user.granted_roles.isEmpty())
                        res.emplace_back(AccessType::ROLE_ADMIN);
                    break;
                }

                case Role::TYPE:
                {
                    const auto & role = typeid_cast<const Role &>(*entity);
                    res.emplace_back(AccessType::CREATE_ROLE);
                    auto elements = role.access.getElements();
                    for (auto & element : elements)
                    {
                        if (element.is_partial_revoke)
                            continue;
                        element.grant_option = true;
                        res.emplace_back(element);
                    }
                    if (!role.granted_roles.isEmpty())
                        res.emplace_back(AccessType::ROLE_ADMIN);
                    break;
                }

                case SettingsProfile::TYPE:
                {
                    res.emplace_back(AccessType::CREATE_SETTINGS_PROFILE);
                    break;
                }

                case RowPolicy::TYPE:
                {
                    const auto & policy = typeid_cast<const RowPolicy &>(*entity);
                    res.emplace_back(AccessType::CREATE_ROW_POLICY, policy.getDatabase(), policy.getTableName());
                    break;
                }

                case Quota::TYPE:
                {
                    res.emplace_back(AccessType::CREATE_QUOTA);
                    break;
                }

                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown type: {}", toString(entity_type));
            }
        }
        return res;
    }
}


std::pair<String, BackupEntryPtr> makeBackupEntryForAccess(
    const std::vector<std::pair<UUID, AccessEntityPtr>> & access_entities,
    const String & data_path_in_backup,
    size_t counter,
    const AccessControl & access_control)
{
    auto dependencies = readDependenciesNamesAndTypes(findDependencies(access_entities), access_control);
    AccessEntitiesInBackup ab;
    boost::range::copy(access_entities, std::inserter(ab.entities, ab.entities.end()));
    ab.dependencies = std::move(dependencies);
    String filename = fmt::format("access{:02}.txt", counter + 1); /// access01.txt, access02.txt, ...
    String file_path_in_backup = fs::path{data_path_in_backup} / filename;
    return {file_path_in_backup, ab.toBackupEntry()};
}


AccessRestorerFromBackup::AccessRestorerFromBackup(
    const BackupPtr & backup_, const RestoreSettings & restore_settings_)
    : backup(backup_)
    , creation_mode(restore_settings_.create_access)
    , allow_unresolved_dependencies(restore_settings_.allow_unresolved_access_dependencies)
{
}

AccessRestorerFromBackup::~AccessRestorerFromBackup() = default;

void AccessRestorerFromBackup::addDataPath(const String & data_path)
{
    if (!data_paths.emplace(data_path).second)
        return;

    fs::path data_path_in_backup_fs = data_path;
    Strings filenames = backup->listFiles(data_path, /*recursive*/ false);
    if (filenames.empty())
        return;

    for (const String & filename : filenames)
    {
        if (!filename.starts_with("access") || !filename.ends_with(".txt"))
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File name {} doesn't match the wildcard \"access*.txt\"",
                            String{data_path_in_backup_fs / filename});
    }

    ::sort(filenames.begin(), filenames.end());

    for (const String & filename : filenames)
    {
        String filepath_in_backup = data_path_in_backup_fs / filename;
        auto read_buffer_from_backup = backup->readFile(filepath_in_backup);
        auto ab = AccessEntitiesInBackup::fromBackupEntry(std::move(read_buffer_from_backup), filepath_in_backup);

        boost::range::copy(ab.entities, std::back_inserter(entities));
        boost::range::copy(ab.dependencies, std::inserter(dependencies, dependencies.end()));
    }

    for (const auto & id : entities | boost::adaptors::map_keys)
        dependencies.erase(id);
}

AccessRightsElements AccessRestorerFromBackup::getRequiredAccess() const
{
    return getRequiredAccessToRestore(entities);
}

std::vector<std::pair<UUID, AccessEntityPtr>> AccessRestorerFromBackup::getAccessEntities(const AccessControl & access_control) const
{
    auto new_entities = entities;

    std::unordered_map<UUID, UUID> old_to_new_ids;
    checkExistingEntities(new_entities, old_to_new_ids, access_control, creation_mode);
    resolveDependencies(dependencies, old_to_new_ids, access_control, allow_unresolved_dependencies);
    generateRandomIDs(new_entities, old_to_new_ids);
    replaceDependencies(new_entities, old_to_new_ids);

    return new_entities;
}

}
