#include <Backups/BackupCoordinationRemote.h>
#include <Access/Common/AccessEntityType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Common/hex.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
    extern const int LOGICAL_ERROR;
}

/// zookeeper_path/file_names/file_name->checksum_and_size
/// zookeeper_path/file_infos/checksum_and_size->info
/// zookeeper_path/archive_suffixes
/// zookeeper_path/current_archive_suffix

namespace
{
    using SizeAndChecksum = IBackupCoordination::SizeAndChecksum;
    using FileInfo = IBackupCoordination::FileInfo;
    using PartNameAndChecksum = IBackupCoordination::PartNameAndChecksum;
    using MutationInfo = IBackupCoordination::MutationInfo;

    struct ReplicatedPartNames
    {
        std::vector<PartNameAndChecksum> part_names_and_checksums;
        String table_name_for_logs;

        static String serialize(const std::vector<PartNameAndChecksum> & part_names_and_checksums_, const String & table_name_for_logs_)
        {
            WriteBufferFromOwnString out;
            writeBinary(part_names_and_checksums_.size(), out);
            for (const auto & part_name_and_checksum : part_names_and_checksums_)
            {
                writeBinary(part_name_and_checksum.part_name, out);
                writeBinary(part_name_and_checksum.checksum, out);
            }
            writeBinary(table_name_for_logs_, out);
            return out.str();
        }

        static ReplicatedPartNames deserialize(const String & str)
        {
            ReadBufferFromString in{str};
            ReplicatedPartNames res;
            size_t num;
            readBinary(num, in);
            res.part_names_and_checksums.resize(num);
            for (size_t i = 0; i != num; ++i)
            {
                readBinary(res.part_names_and_checksums[i].part_name, in);
                readBinary(res.part_names_and_checksums[i].checksum, in);
            }
            readBinary(res.table_name_for_logs, in);
            return res;
        }
    };

    struct ReplicatedMutations
    {
        std::vector<MutationInfo> mutations;
        String table_name_for_logs;

        static String serialize(const std::vector<MutationInfo> & mutations_, const String & table_name_for_logs_)
        {
            WriteBufferFromOwnString out;
            writeBinary(mutations_.size(), out);
            for (const auto & mutation : mutations_)
            {
                writeBinary(mutation.id, out);
                writeBinary(mutation.entry, out);
            }
            writeBinary(table_name_for_logs_, out);
            return out.str();
        }

        static ReplicatedMutations deserialize(const String & str)
        {
            ReadBufferFromString in{str};
            ReplicatedMutations res;
            size_t num;
            readBinary(num, in);
            res.mutations.resize(num);
            for (size_t i = 0; i != num; ++i)
            {
                readBinary(res.mutations[i].id, in);
                readBinary(res.mutations[i].entry, in);
            }
            readBinary(res.table_name_for_logs, in);
            return res;
        }
    };

    String serializeFileInfo(const FileInfo & info)
    {
        WriteBufferFromOwnString out;
        writeBinary(info.file_name, out);
        writeBinary(info.size, out);
        writeBinary(info.checksum, out);
        writeBinary(info.base_size, out);
        writeBinary(info.base_checksum, out);
        writeBinary(info.data_file_name, out);
        writeBinary(info.archive_suffix, out);
        writeBinary(info.pos_in_archive, out);
        return out.str();
    }

    FileInfo deserializeFileInfo(const String & str)
    {
        FileInfo info;
        ReadBufferFromString in{str};
        readBinary(info.file_name, in);
        readBinary(info.size, in);
        readBinary(info.checksum, in);
        readBinary(info.base_size, in);
        readBinary(info.base_checksum, in);
        readBinary(info.data_file_name, in);
        readBinary(info.archive_suffix, in);
        readBinary(info.pos_in_archive, in);
        return info;
    }

    String serializeSizeAndChecksum(const SizeAndChecksum & size_and_checksum)
    {
        return getHexUIntLowercase(size_and_checksum.second) + '_' + std::to_string(size_and_checksum.first);
    }

    SizeAndChecksum deserializeSizeAndChecksum(const String & str)
    {
        constexpr size_t num_chars_in_checksum = sizeof(UInt128) * 2;
        if (str.size() <= num_chars_in_checksum)
            throw Exception(
                ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER,
                "Unexpected size of checksum: {}, must be {}",
                str.size(),
                num_chars_in_checksum);
        UInt128 checksum = unhexUInt<UInt128>(str.data());
        UInt64 size = parseFromString<UInt64>(str.substr(num_chars_in_checksum + 1));
        return std::pair{size, checksum};
    }

    size_t extractCounterFromSequentialNodeName(const String & node_name)
    {
        size_t pos_before_counter = node_name.find_last_not_of("0123456789");
        size_t counter_length = node_name.length() - 1 - pos_before_counter;
        auto counter = std::string_view{node_name}.substr(node_name.length() - counter_length);
        return parseFromString<UInt64>(counter);
    }

    String formatArchiveSuffix(size_t counter)
    {
        return fmt::format("{:03}", counter); /// Outputs 001, 002, 003, ...
    }

    /// We try to store data to zookeeper several times due to possible version conflicts.
    constexpr size_t NUM_ATTEMPTS = 10;
}

BackupCoordinationRemote::BackupCoordinationRemote(
    const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, bool remove_zk_nodes_in_destructor_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , remove_zk_nodes_in_destructor(remove_zk_nodes_in_destructor_)
    , stage_sync(zookeeper_path_ + "/stage", get_zookeeper_, &Poco::Logger::get("BackupCoordination"))
{
    createRootNodes();
}

BackupCoordinationRemote::~BackupCoordinationRemote()
{
    try
    {
        if (remove_zk_nodes_in_destructor)
            removeAllNodes();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void BackupCoordinationRemote::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_part_names", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_mutations", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_data_paths", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_access", "");
    zookeeper->createIfNotExists(zookeeper_path + "/file_names", "");
    zookeeper->createIfNotExists(zookeeper_path + "/file_infos", "");
    zookeeper->createIfNotExists(zookeeper_path + "/archive_suffixes", "");
}

void BackupCoordinationRemote::removeAllNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}


void BackupCoordinationRemote::setStage(const String & current_host, const String & new_stage, const String & message)
{
    stage_sync.set(current_host, new_stage, message);
}

void BackupCoordinationRemote::setError(const String & current_host, const Exception & exception)
{
    stage_sync.setError(current_host, exception);
}

Strings BackupCoordinationRemote::waitForStage(const Strings & all_hosts, const String & stage_to_wait)
{
    return stage_sync.wait(all_hosts, stage_to_wait);
}

Strings BackupCoordinationRemote::waitForStage(const Strings & all_hosts, const String & stage_to_wait, std::chrono::milliseconds timeout)
{
    return stage_sync.waitFor(all_hosts, stage_to_wait, timeout);
}


void BackupCoordinationRemote::addReplicatedPartNames(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedPartNames() must not be called after preparing");
    }

    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_part_names/" + escapeForFileName(table_shared_id);
    zookeeper->createIfNotExists(path, "");
    path += "/" + escapeForFileName(replica_name);
    zookeeper->create(path, ReplicatedPartNames::serialize(part_names_and_checksums, table_name_for_logs), zkutil::CreateMode::Persistent);
}

Strings BackupCoordinationRemote::getReplicatedPartNames(const String & table_shared_id, const String & replica_name) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedTables();
    return replicated_tables->getPartNames(table_shared_id, replica_name);
}

void BackupCoordinationRemote::addReplicatedMutations(
    const String & table_shared_id,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<MutationInfo> & mutations)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedMutations() must not be called after preparing");
    }

    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_mutations/" + escapeForFileName(table_shared_id);
    zookeeper->createIfNotExists(path, "");
    path += "/" + escapeForFileName(replica_name);
    zookeeper->create(path, ReplicatedMutations::serialize(mutations, table_name_for_logs), zkutil::CreateMode::Persistent);
}

std::vector<IBackupCoordination::MutationInfo> BackupCoordinationRemote::getReplicatedMutations(const String & table_shared_id, const String & replica_name) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedTables();
    return replicated_tables->getMutations(table_shared_id, replica_name);
}


void BackupCoordinationRemote::addReplicatedDataPath(
    const String & table_shared_id, const String & data_path)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_tables)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedDataPath() must not be called after preparing");
    }

    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_data_paths/" + escapeForFileName(table_shared_id);
    zookeeper->createIfNotExists(path, "");
    path += "/" + escapeForFileName(data_path);
    zookeeper->createIfNotExists(path, "");
}

Strings BackupCoordinationRemote::getReplicatedDataPaths(const String & table_shared_id) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedTables();
    return replicated_tables->getDataPaths(table_shared_id);
}


void BackupCoordinationRemote::prepareReplicatedTables() const
{
    if (replicated_tables)
        return;

    replicated_tables.emplace();
    auto zookeeper = get_zookeeper();

    {
        String path = zookeeper_path + "/repl_part_names";
        for (const String & escaped_table_shared_id : zookeeper->getChildren(path))
        {
            String table_shared_id = unescapeForFileName(escaped_table_shared_id);
            String path2 = path + "/" + escaped_table_shared_id;
            for (const String & escaped_replica_name : zookeeper->getChildren(path2))
            {
                String replica_name = unescapeForFileName(escaped_replica_name);
                auto part_names = ReplicatedPartNames::deserialize(zookeeper->get(path2 + "/" + escaped_replica_name));
                replicated_tables->addPartNames(table_shared_id, part_names.table_name_for_logs, replica_name, part_names.part_names_and_checksums);
            }
        }
    }

    {
        String path = zookeeper_path + "/repl_mutations";
        for (const String & escaped_table_shared_id : zookeeper->getChildren(path))
        {
            String table_shared_id = unescapeForFileName(escaped_table_shared_id);
            String path2 = path + "/" + escaped_table_shared_id;
            for (const String & escaped_replica_name : zookeeper->getChildren(path2))
            {
                String replica_name = unescapeForFileName(escaped_replica_name);
                auto mutations = ReplicatedMutations::deserialize(zookeeper->get(path2 + "/" + escaped_replica_name));
                replicated_tables->addMutations(table_shared_id, mutations.table_name_for_logs, replica_name, mutations.mutations);
            }
        }
    }

    {
        String path = zookeeper_path + "/repl_data_paths";
        for (const String & escaped_table_shared_id : zookeeper->getChildren(path))
        {
            String table_shared_id = unescapeForFileName(escaped_table_shared_id);
            String path2 = path + "/" + escaped_table_shared_id;
            for (const String & escaped_data_path : zookeeper->getChildren(path2))
            {
                String data_path = unescapeForFileName(escaped_data_path);
                replicated_tables->addDataPath(table_shared_id, data_path);
            }
        }
    }
}


void BackupCoordinationRemote::addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id, const String & file_path)
{
    {
        std::lock_guard lock{mutex};
        if (replicated_access)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "addReplicatedAccessFilePath() must not be called after preparing");
    }

    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_access/" + escapeForFileName(access_zk_path);
    zookeeper->createIfNotExists(path, "");
    path += "/" + AccessEntityTypeInfo::get(access_entity_type).name;
    zookeeper->createIfNotExists(path, "");
    path += "/" + host_id;
    zookeeper->createIfNotExists(path, file_path);
}

Strings BackupCoordinationRemote::getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type, const String & host_id) const
{
    std::lock_guard lock{mutex};
    prepareReplicatedAccess();
    return replicated_access->getFilePaths(access_zk_path, access_entity_type, host_id);
}

void BackupCoordinationRemote::prepareReplicatedAccess() const
{
    if (replicated_access)
        return;

    replicated_access.emplace();
    auto zookeeper = get_zookeeper();

    String path = zookeeper_path + "/repl_access";
    for (const String & escaped_access_zk_path : zookeeper->getChildren(path))
    {
        String access_zk_path = unescapeForFileName(escaped_access_zk_path);
        String path2 = path + "/" + escaped_access_zk_path;
        for (const String & type_str : zookeeper->getChildren(path2))
        {
            AccessEntityType type = AccessEntityTypeInfo::parseType(type_str);
            String path3 = path2 + "/" + type_str;
            for (const String & host_id : zookeeper->getChildren(path3))
            {
                String file_path = zookeeper->get(path3 + "/" + host_id);
                replicated_access->addFilePath(access_zk_path, type, host_id, file_path);
            }
        }
    }
}


void BackupCoordinationRemote::addFileInfo(const FileInfo & file_info, bool & is_data_file_required)
{
    auto zookeeper = get_zookeeper();

    String full_path = zookeeper_path + "/file_names/" + escapeForFileName(file_info.file_name);
    String size_and_checksum = serializeSizeAndChecksum(std::pair{file_info.size, file_info.checksum});
    zookeeper->create(full_path, size_and_checksum, zkutil::CreateMode::Persistent);

    if (!file_info.size)
    {
        is_data_file_required = false;
        return;
    }

    full_path = zookeeper_path + "/file_infos/" + size_and_checksum;
    auto code = zookeeper->tryCreate(full_path, serializeFileInfo(file_info), zkutil::CreateMode::Persistent);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
        throw zkutil::KeeperException(code, full_path);

    is_data_file_required = (code == Coordination::Error::ZOK) && (file_info.size > file_info.base_size);
}

void BackupCoordinationRemote::updateFileInfo(const FileInfo & file_info)
{
    if (!file_info.size)
        return; /// we don't keep FileInfos for empty files, nothing to update

    auto zookeeper = get_zookeeper();
    String size_and_checksum = serializeSizeAndChecksum(std::pair{file_info.size, file_info.checksum});
    String full_path = zookeeper_path + "/file_infos/" + size_and_checksum;
    for (size_t attempt = 0; attempt < NUM_ATTEMPTS; ++attempt)
    {
        Coordination::Stat stat;
        auto new_info = deserializeFileInfo(zookeeper->get(full_path, &stat));
        new_info.archive_suffix = file_info.archive_suffix;
        auto code = zookeeper->trySet(full_path, serializeFileInfo(new_info), stat.version);
        if (code == Coordination::Error::ZOK)
            return;
        bool is_last_attempt = (attempt == NUM_ATTEMPTS - 1);
        if ((code != Coordination::Error::ZBADVERSION) || is_last_attempt)
            throw zkutil::KeeperException(code, full_path);
    }
}

std::vector<FileInfo> BackupCoordinationRemote::getAllFileInfos() const
{
    auto zookeeper = get_zookeeper();
    std::vector<FileInfo> file_infos;
    Strings escaped_names = zookeeper->getChildren(zookeeper_path + "/file_names");
    for (const String & escaped_name : escaped_names)
    {
        String size_and_checksum = zookeeper->get(zookeeper_path + "/file_names/" + escaped_name);
        UInt64 size = deserializeSizeAndChecksum(size_and_checksum).first;
        FileInfo file_info;
        if (size) /// we don't keep FileInfos for empty files
            file_info = deserializeFileInfo(zookeeper->get(zookeeper_path + "/file_infos/" + size_and_checksum));
        file_info.file_name = unescapeForFileName(escaped_name);
        file_infos.emplace_back(std::move(file_info));
    }
    return file_infos;
}

Strings BackupCoordinationRemote::listFiles(const String & directory, bool recursive) const
{
    auto zookeeper = get_zookeeper();
    Strings escaped_names = zookeeper->getChildren(zookeeper_path + "/file_names");

    String prefix = directory;
    if (!prefix.empty() && !prefix.ends_with('/'))
        prefix += '/';
    String terminator = recursive ? "" : "/";

    Strings elements;
    std::unordered_set<std::string_view> unique_elements;

    for (const String & escaped_name : escaped_names)
    {
        String name = unescapeForFileName(escaped_name);
        if (!name.starts_with(prefix))
            continue;
        size_t start_pos = prefix.length();
        size_t end_pos = String::npos;
        if (!terminator.empty())
            end_pos = name.find(terminator, start_pos);
        std::string_view new_element = std::string_view{name}.substr(start_pos, end_pos - start_pos);
        if (unique_elements.contains(new_element))
            continue;
        elements.push_back(String{new_element});
        unique_elements.emplace(new_element);
    }

    ::sort(elements.begin(), elements.end());
    return elements;
}

bool BackupCoordinationRemote::hasFiles(const String & directory) const
{
    auto zookeeper = get_zookeeper();
    Strings escaped_names = zookeeper->getChildren(zookeeper_path + "/file_names");

    String prefix = directory;
    if (!prefix.empty() && !prefix.ends_with('/'))
        prefix += '/';

    for (const String & escaped_name : escaped_names)
    {
        String name = unescapeForFileName(escaped_name);
        if (name.starts_with(prefix))
            return true;
    }

    return false;
}

std::optional<FileInfo> BackupCoordinationRemote::getFileInfo(const String & file_name) const
{
    auto zookeeper = get_zookeeper();
    String size_and_checksum;
    if (!zookeeper->tryGet(zookeeper_path + "/file_names/" + escapeForFileName(file_name), size_and_checksum))
        return std::nullopt;
    UInt64 size = deserializeSizeAndChecksum(size_and_checksum).first;
    FileInfo file_info;
    if (size) /// we don't keep FileInfos for empty files
        file_info = deserializeFileInfo(zookeeper->get(zookeeper_path + "/file_infos/" + size_and_checksum));
    file_info.file_name = file_name;
    return file_info;
}

std::optional<FileInfo> BackupCoordinationRemote::getFileInfo(const SizeAndChecksum & size_and_checksum) const
{
    auto zookeeper = get_zookeeper();
    String file_info_str;
    if (!zookeeper->tryGet(zookeeper_path + "/file_infos/" + serializeSizeAndChecksum(size_and_checksum), file_info_str))
        return std::nullopt;
    return deserializeFileInfo(file_info_str);
}

std::optional<SizeAndChecksum> BackupCoordinationRemote::getFileSizeAndChecksum(const String & file_name) const
{
    auto zookeeper = get_zookeeper();
    String size_and_checksum;
    if (!zookeeper->tryGet(zookeeper_path + "/file_names/" + escapeForFileName(file_name), size_and_checksum))
        return std::nullopt;
    return deserializeSizeAndChecksum(size_and_checksum);
}

String BackupCoordinationRemote::getNextArchiveSuffix()
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/archive_suffixes/a";
    String path_created;
    auto code = zookeeper->tryCreate(path, "", zkutil::CreateMode::PersistentSequential, path_created);
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException(code, path);
    return formatArchiveSuffix(extractCounterFromSequentialNodeName(path_created));
}

Strings BackupCoordinationRemote::getAllArchiveSuffixes() const
{
    auto zookeeper = get_zookeeper();
    Strings node_names = zookeeper->getChildren(zookeeper_path + "/archive_suffixes");
    for (auto & node_name : node_names)
        node_name = formatArchiveSuffix(extractCounterFromSequentialNodeName(node_name));
    return node_names;
}

}
