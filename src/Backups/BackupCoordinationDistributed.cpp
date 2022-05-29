#include <Backups/BackupCoordinationDistributed.h>
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

BackupCoordinationDistributed::BackupCoordinationDistributed(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_)
    : zookeeper_path(zookeeper_path_)
    , get_zookeeper(get_zookeeper_)
    , collecting_backup_entries_barrier(zookeeper_path_ + "/collect_bents", get_zookeeper_, "BackupCoordination", "collecting backup entries")
{
    createRootNodes();
}

BackupCoordinationDistributed::~BackupCoordinationDistributed() = default;

void BackupCoordinationDistributed::createRootNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_part_names", "");
    zookeeper->createIfNotExists(zookeeper_path + "/repl_data_paths", "");
    zookeeper->createIfNotExists(zookeeper_path + "/file_names", "");
    zookeeper->createIfNotExists(zookeeper_path + "/file_infos", "");
    zookeeper->createIfNotExists(zookeeper_path + "/archive_suffixes", "");
}

void BackupCoordinationDistributed::removeAllNodes()
{
    auto zookeeper = get_zookeeper();
    zookeeper->removeRecursive(zookeeper_path);
}


void BackupCoordinationDistributed::addReplicatedPartNames(
    const String & table_zk_path,
    const String & table_name_for_logs,
    const String & replica_name,
    const std::vector<PartNameAndChecksum> & part_names_and_checksums)
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_part_names/" + escapeForFileName(table_zk_path);
    zookeeper->createIfNotExists(path, "");
    path += "/" + escapeForFileName(replica_name);
    zookeeper->create(path, ReplicatedPartNames::serialize(part_names_and_checksums, table_name_for_logs), zkutil::CreateMode::Persistent);
}

Strings BackupCoordinationDistributed::getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const
{
    return replicated_part_names->getPartNames(table_zk_path, replica_name);
}


void BackupCoordinationDistributed::addReplicatedDataPath(
    const String & table_zk_path, const String & data_path)
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_data_paths/" + escapeForFileName(table_zk_path);
    zookeeper->createIfNotExists(path, "");
    path += "/";
    zookeeper->create(path, data_path, zkutil::CreateMode::PersistentSequential);
}

Strings BackupCoordinationDistributed::getReplicatedDataPaths(const String & table_zk_path) const
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/repl_data_paths/" + escapeForFileName(table_zk_path);
    Strings children = zookeeper->getChildren(path);
    Strings data_paths;
    data_paths.reserve(children.size());
    for (const String & child : children)
        data_paths.push_back(zookeeper->get(path + "/" + child));
    return data_paths;
}


void BackupCoordinationDistributed::finishCollectingBackupEntries(const String & host_id, const String & error_message)
{
    collecting_backup_entries_barrier.finish(host_id, error_message);
}

void BackupCoordinationDistributed::waitForAllHostsCollectedBackupEntries(const Strings & host_ids, std::chrono::seconds timeout) const
{
    collecting_backup_entries_barrier.waitForAllHostsToFinish(host_ids, timeout);
    prepareReplicatedPartNames();
}

void BackupCoordinationDistributed::prepareReplicatedPartNames() const
{
    auto zookeeper = get_zookeeper();
    replicated_part_names.emplace();

    String path = zookeeper_path + "/repl_part_names";
    for (const String & escaped_table_zk_path : zookeeper->getChildren(path))
    {
        String table_zk_path = unescapeForFileName(escaped_table_zk_path);
        String path2 = path + "/" + escaped_table_zk_path;
        for (const String & escaped_replica_name : zookeeper->getChildren(path2))
        {
            String replica_name = unescapeForFileName(escaped_replica_name);
            auto part_names = ReplicatedPartNames::deserialize(zookeeper->get(path2 + "/" + escaped_replica_name));
            replicated_part_names->addPartNames(table_zk_path, part_names.table_name_for_logs, replica_name, part_names.part_names_and_checksums);
        }
    }

    replicated_part_names->preparePartNames();
}


void BackupCoordinationDistributed::addFileInfo(const FileInfo & file_info, bool & is_data_file_required)
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

void BackupCoordinationDistributed::updateFileInfo(const FileInfo & file_info)
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

std::vector<FileInfo> BackupCoordinationDistributed::getAllFileInfos() const
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

Strings BackupCoordinationDistributed::listFiles(const String & prefix, const String & terminator) const
{
    auto zookeeper = get_zookeeper();
    Strings escaped_names = zookeeper->getChildren(zookeeper_path + "/file_names");

    Strings elements;
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
        if (!elements.empty() && (elements.back() == new_element))
            continue;
        elements.push_back(String{new_element});
    }

    ::sort(elements.begin(), elements.end());
    return elements;
}

std::optional<FileInfo> BackupCoordinationDistributed::getFileInfo(const String & file_name) const
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

std::optional<FileInfo> BackupCoordinationDistributed::getFileInfo(const SizeAndChecksum & size_and_checksum) const
{
    auto zookeeper = get_zookeeper();
    String file_info_str;
    if (!zookeeper->tryGet(zookeeper_path + "/file_infos/" + serializeSizeAndChecksum(size_and_checksum), file_info_str))
        return std::nullopt;
    return deserializeFileInfo(file_info_str);
}

std::optional<SizeAndChecksum> BackupCoordinationDistributed::getFileSizeAndChecksum(const String & file_name) const
{
    auto zookeeper = get_zookeeper();
    String size_and_checksum;
    if (!zookeeper->tryGet(zookeeper_path + "/file_names/" + escapeForFileName(file_name), size_and_checksum))
        return std::nullopt;
    return deserializeSizeAndChecksum(size_and_checksum);
}

String BackupCoordinationDistributed::getNextArchiveSuffix()
{
    auto zookeeper = get_zookeeper();
    String path = zookeeper_path + "/archive_suffixes/a";
    String path_created;
    auto code = zookeeper->tryCreate(path, "", zkutil::CreateMode::PersistentSequential, path_created);
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException(code, path);
    return formatArchiveSuffix(extractCounterFromSequentialNodeName(path_created));
}

Strings BackupCoordinationDistributed::getAllArchiveSuffixes() const
{
    auto zookeeper = get_zookeeper();
    Strings node_names = zookeeper->getChildren(zookeeper_path + "/archive_suffixes");
    for (auto & node_name : node_names)
        node_name = formatArchiveSuffix(extractCounterFromSequentialNodeName(node_name));
    return node_names;
}

void BackupCoordinationDistributed::drop()
{
    removeAllNodes();
}

}
