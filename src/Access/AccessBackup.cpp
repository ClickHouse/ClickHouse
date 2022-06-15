#include <Access/AccessBackup.h>
#include <Access/AccessControl.h>
#include <Access/AccessEntityIO.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackup.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/UUIDGenerator.h>
#include <base/insertAtEnd.h>
#include <boost/range/algorithm/copy.hpp>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

        static AccessEntitiesInBackup fromBackupEntry(const IBackupEntry & backup_entry, const String & file_path)
        {
            try
            {
                AccessEntitiesInBackup res;
                std::unique_ptr<ReadBuffer> buf = backup_entry.getReadBuffer();

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

                    size_t id_endpos = line.find('\t');
                    String id_as_string = line.substr(0, id_endpos);
                    UUID id = parse<UUID>(line);
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
                e.addMessage("While parsing " + file_path);
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
            if (*it == id)
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

    std::unordered_map<UUID, UUID> resolveDependencies(const std::unordered_map<UUID, std::pair<String, AccessEntityType>> & dependencies, const AccessControl & access_control, bool allow_unresolved_dependencies)
    {
        std::unordered_map<UUID, UUID> old_to_new_ids;
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
        return old_to_new_ids;
    }

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

    void replaceDependencies(std::vector<std::pair<UUID, AccessEntityPtr>> & entities, const std::unordered_map<UUID, UUID> & old_to_new_ids)
    {
        for (auto & entity : entities | boost::adaptors::map_values)
        {
            bool need_replace = false;
            for (const auto & dependency : entity->findDependencies())
            {
                if (old_to_new_ids.contains(dependency))
                {
                    need_replace = true;
                    break;
                }
            }

            if (!need_replace)
                continue;

            auto new_entity = entity->clone();
            new_entity->replaceDependencies(old_to_new_ids);
            entity = new_entity;
        }
    }
}

void backupAccessEntities(
    BackupEntriesCollector & backup_entries_collector,
    const String & data_path_in_backup,
    const AccessControl & access_control,
    AccessEntityType type)
{
    auto entities = access_control.readAllForBackup(type, backup_entries_collector.getBackupSettings());
    auto dependencies = readDependenciesNamesAndTypes(findDependencies(entities), access_control);
    AccessEntitiesInBackup ab;
    boost::range::copy(entities, std::inserter(ab.entities, ab.entities.end()));
    ab.dependencies = std::move(dependencies);
    backup_entries_collector.addBackupEntry(fs::path{data_path_in_backup} / "access.txt", ab.toBackupEntry());
}


AccessRestoreTask::AccessRestoreTask(
    const BackupPtr & backup_, const RestoreSettings & restore_settings_, std::shared_ptr<IRestoreCoordination> restore_coordination_)
    : backup(backup_), restore_settings(restore_settings_), restore_coordination(restore_coordination_)
{
}

AccessRestoreTask::~AccessRestoreTask() = default;

void AccessRestoreTask::addDataPath(const String & data_path)
{
    String file_path = fs::path{data_path} / "access.txt";
    auto backup_entry = backup->readFile(file_path);
    auto ab = AccessEntitiesInBackup::fromBackupEntry(*backup_entry, file_path);

    boost::range::copy(ab.entities, std::inserter(entities, entities.end()));
    boost::range::copy(ab.dependencies, std::inserter(dependencies, dependencies.end()));
    for (const auto & id : entities | boost::adaptors::map_keys)
        dependencies.erase(id);
}

void AccessRestoreTask::restore(AccessControl & access_control)
{
    auto old_to_new_ids = resolveDependencies(dependencies, access_control, restore_settings.allow_unresolved_access_dependencies);

    std::vector<std::pair<UUID, AccessEntityPtr>> new_entities;
    boost::range::copy(entities, std::back_inserter(new_entities));
    generateRandomIDs(new_entities, old_to_new_ids);

    replaceDependencies(new_entities, old_to_new_ids);

    access_control.insertFromBackup(new_entities, restore_settings, restore_coordination);
}

}
