#include <Storages/StorageZeroCopyReplicatedMergeTree.h>

namespace DB
{

MergeTreeData::MutableDataPartPtr StorageZeroCopyReplicatedMergeTree::executeFetchShared(
const String & source_replica,
const String & new_part_name,
const DiskPtr & disk,
const String & path)
{
    if (source_replica.empty())
    {
        LOG_INFO(log, "No active replica has part {} on shared storage.", new_part_name);
        return nullptr;
    }

    const auto storage_settings_ptr = getSettings();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    try
    {
        return fetchExistsPart(new_part_name, metadata_snapshot, fs::path(zookeeper_path) / "replicas" / source_replica, disk, path);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS)
            e.addMessage("Too busy replica. Will try later.");
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        throw;
    }
}

void StorageZeroCopyReplicatedMergeTree::lockSharedData(
    const IMergeTreeDataPart & part,
    bool replace_existing_lock,
    std::optional<HardlinkedFiles> hardlinked_files) const
{
    LOG_DEBUG(log, "Trying to create zero-copy lock for part {}", part.name);
    auto zookeeper = tryGetZooKeeper();
    if (zookeeper)
        lockSharedData(part, std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), replace_existing_lock, hardlinked_files);
}

void StorageZeroCopyReplicatedMergeTree::getLockSharedDataOps(
    const IMergeTreeDataPart & part,
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    bool replace_existing_lock,
    std::optional<HardlinkedFiles> hardlinked_files,
    Coordination::Requests & requests) const
{
    auto settings = getSettings();

    if (!part.isStoredOnDisk() || !(*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
        return;

    if (!part.getDataPartStorage().supportZeroCopyReplication())
        return;

    if (zookeeper->isNull())
        return;

    String id = part.getUniqueId();
    boost::replace_all(id, "/", "_");

    Strings zc_zookeeper_paths = getZeroCopyPartPath(
        *getSettings(), part.getDataPartStorage().getDiskType(), getTableSharedID(),
        part.name, zookeeper_path);

    String path_to_set_hardlinked_files;
    NameSet hardlinks;

    if (hardlinked_files.has_value() && !hardlinked_files->hardlinks_from_source_part.empty())
    {
        path_to_set_hardlinked_files = getZeroCopyPartPath(
            *getSettings(), part.getDataPartStorage().getDiskType(), hardlinked_files->source_table_shared_id,
            hardlinked_files->source_part_name, zookeeper_path)[0];

        hardlinks = hardlinked_files->hardlinks_from_source_part;
    }

    for (const auto & zc_zookeeper_path : zc_zookeeper_paths)
    {
        String zookeeper_node = fs::path(zc_zookeeper_path) / id / replica_name;

        if (!path_to_set_hardlinked_files.empty() && !hardlinks.empty())
        {
            LOG_DEBUG(log, "Locking shared node {} with hardlinks from the other shared node {}, "
                           "hardlinks: [{}]",
                      zookeeper_node, path_to_set_hardlinked_files,
                      boost::algorithm::join(hardlinks, ","));
        }

        getZeroCopyLockNodeCreateOps(
            zookeeper, zookeeper_node, requests, zkutil::CreateMode::Persistent,
            replace_existing_lock, path_to_set_hardlinked_files, hardlinks);
    }
}


void StorageZeroCopyReplicatedMergeTree::lockSharedData(
    const IMergeTreeDataPart & part,
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    bool replace_existing_lock,
    std::optional<HardlinkedFiles> hardlinked_files) const
{
    auto settings = getSettings();

    if (!part.isStoredOnDisk() || !(*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
        return;

    if (!part.getDataPartStorage().supportZeroCopyReplication())
        return;

    if (zookeeper->isNull())
        return;

    String id = part.getUniqueId();
    boost::replace_all(id, "/", "_");

    Strings zc_zookeeper_paths = getZeroCopyPartPath(
        *getSettings(), part.getDataPartStorage().getDiskType(), getTableSharedID(),
        part.name, zookeeper_path);

    String path_to_set_hardlinked_files;
    NameSet hardlinks;

    if (hardlinked_files.has_value() && !hardlinked_files->hardlinks_from_source_part.empty())
    {
        path_to_set_hardlinked_files = getZeroCopyPartPath(
            *getSettings(), part.getDataPartStorage().getDiskType(), hardlinked_files->source_table_shared_id,
            hardlinked_files->source_part_name, zookeeper_path)[0];

        hardlinks = hardlinked_files->hardlinks_from_source_part;
    }

    for (const auto & zc_zookeeper_path : zc_zookeeper_paths)
    {
        String zookeeper_node = fs::path(zc_zookeeper_path) / id / replica_name;

        LOG_TRACE(log, "Trying to create zookeeper persistent lock {} with hardlinks [{}]", zookeeper_node, fmt::join(hardlinks, ", "));

        createZeroCopyLockNode(
            zookeeper, zookeeper_node, zkutil::CreateMode::Persistent,
            replace_existing_lock, path_to_set_hardlinked_files, hardlinks);

        LOG_TRACE(log, "Zookeeper persistent lock {} created", zookeeper_node);
    }
}

std::pair<bool, NameSet>
StorageZeroCopyReplicatedMergeTree::unlockSharedData(const IMergeTreeDataPart & part) const
{
    return unlockSharedData(part, std::make_shared<ZooKeeperWithFaultInjection>(nullptr));
}

std::pair<bool, NameSet>
StorageZeroCopyReplicatedMergeTree::unlockSharedData(const IMergeTreeDataPart & part, const ZooKeeperWithFaultInjectionPtr & zookeeper) const
{
    auto settings = getSettings();
    if (!(*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
        return std::make_pair(true, NameSet{});

    if (!part.isStoredOnDisk())
    {
        LOG_TRACE(log, "Part {} is not stored on disk, blobs can be removed", part.name);
        return std::make_pair(true, NameSet{});
    }

    if (!part.getDataPartStorage().supportZeroCopyReplication())
    {
        LOG_TRACE(log, "Part {} is not stored on zero-copy replicated disk, blobs can be removed", part.name);
        return std::make_pair(true, NameSet{});
    }

    auto shared_id = getTableSharedID();
    if (shared_id == toString(UUIDHelpers::Nil))
    {
        if (zookeeper->exists(zookeeper_path))
        {
            LOG_WARNING(log, "Not removing shared data for part {} because replica does not have metadata in ZooKeeper, "
                             "but table path exist and other replicas may exist. It may leave some garbage on S3", part.name);
            return std::make_pair(false, NameSet{});
        }
        LOG_TRACE(log, "Part {} blobs can be removed, because table {} completely dropped", part.name, getStorageID().getNameForLogs());
        return std::make_pair(true, NameSet{});
    }

    /// If part is temporary refcount file may be absent
    if (part.getDataPartStorage().existsFile(IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK))
    {
        auto ref_count = part.getDataPartStorage().getRefCount(IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK);
        if (ref_count > 0) /// Keep part shard info for frozen backups
        {
            LOG_TRACE(log, "Part {} has more than zero local references ({}), blobs cannot be removed", part.name, ref_count);
            return std::make_pair(false, NameSet{});
        }

        LOG_TRACE(log, "Part {} local references is zero, will check blobs can be removed in zookeeper", part.name);
    }
    else
    {
        LOG_TRACE(log, "Part {} looks temporary, because {} file doesn't exists, blobs can be removed", part.name, IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK);
        /// Temporary part with some absent file cannot be locked in shared mode
        return std::make_pair(true, NameSet{});
    }

    if (part.getState() == MergeTreeDataPartState::Temporary && part.is_temp)
    {
        /// Part {} is in temporary state and has it_temp flag. it means that it is under construction.
        /// That path hasn't been added to active set, no commit procedure has begun.
        /// The metadata files is about to delete now. Clichouse has to make a decision remove or preserve blobs on remote FS.
        /// In general remote data might be shared and has to be unlocked in the keeper before removing.
        /// However there are some cases when decision is clear without asking keeper:
        /// When the part has been fetched then remote data has to be preserved, part doesn't own it.
        /// When the part has been merged then remote data can be removed, part owns it.
        /// In opposition, when the part has been mutated in generally it hardlinks the files from source part.
        /// Therefore remote data could be shared, it has to be unlocked in the keeper.
        /// In order to track all that cases remove_tmp_policy is used.
        /// Clickhouse set that field as REMOVE_BLOBS or PRESERVE_BLOBS when it sure about the decision without asking keeper.

        if (part.remove_tmp_policy == IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS
            || part.remove_tmp_policy == IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::PRESERVE_BLOBS)
        {
            bool can_remove_blobs = part.remove_tmp_policy == IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;
            LOG_INFO(log, "Looks like CH knows the origin of that part. "
                          "Part {} can be deleted without unlocking shared data in zookeeper. "
                          "Part blobs {}.",
                     part.name,
                     can_remove_blobs ? "will be removed" : "have to be preserved");
            return std::make_pair(can_remove_blobs, NameSet{});
        }
    }

    if (part.rows_count == 0 && part.remove_tmp_policy == IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS_OF_NOT_TEMPORARY)
    {
        /// It's a non-replicated empty part that was created to avoid unexpected parts after DROP_RANGE
        LOG_INFO(log, "Looks like {} is a non-replicated empty part that was created to avoid unexpected parts after DROP_RANGE, "
                      "blobs can be removed", part.name);
        return std::make_pair(true, NameSet{});
    }

    if (has_metadata_in_zookeeper.has_value() && !has_metadata_in_zookeeper)
    {
        if (zookeeper->exists(zookeeper_path))
        {
            LOG_WARNING(log, "Not removing shared data for part {} because replica does not have metadata in ZooKeeper, "
                             "but table path exist and other replicas may exist. It may leave some garbage on S3", part.name);
            return std::make_pair(false, NameSet{});
        }

        /// If table was completely dropped (no meta in zookeeper) we can safely remove parts
        return std::make_pair(true, NameSet{});
    }

    /// We remove parts during table shutdown. If exception happen, restarting thread will be already turned
    /// off and nobody will reconnect our zookeeper connection. In this case we use zookeeper connection from
    /// context.
    if (shutdown_called.load())
        zookeeper->setKeeper(getZooKeeperIfTableShutDown());
    else
        zookeeper->setKeeper(getZooKeeper());

    /// It can happen that we didn't had the connection to zookeeper during table creation, but actually
    /// table is completely dropped, so we can drop it without any additional checks.
    if (!has_metadata_in_zookeeper.has_value() && !zookeeper->exists(zookeeper_path))
        return std::make_pair(true, NameSet{});

    return unlockSharedDataByID(
        part.getUniqueId(), shared_id, part.info, replica_name,
        part.getDataPartStorage().getDiskType(), zookeeper, *getSettings(), log.load(), zookeeper_path, format_version);
}

namespace
{

/// What is going on here?
/// Actually we need this code because of flaws in hardlinks tracking. When we create child part during mutation we can hardlink some files from parent part, like
/// all_0_0_0:
///                     a.bin a.mrk2 columns.txt ...
/// all_0_0_0_1:          ^     ^
///                     a.bin a.mrk2 columns.txt
/// So when we deleting all_0_0_0 it doesn't remove blobs for a.bin and a.mrk2 because all_0_0_0_1 use them.
/// But sometimes we need an opposite. When we deleting all_0_0_0_1 it can be non replicated to other replicas, so we are the only owner of this part.
/// In this case when we will drop all_0_0_0_1 we will drop blobs for all_0_0_0. But it will lead to dataloss. For such case we need to check that other replicas
/// still need parent part.
std::pair<bool, NameSet> getParentLockedBlobs(const ZooKeeperWithFaultInjectionPtr & zookeeper_ptr, const std::string & zero_copy_part_path_prefix, const MergeTreePartInfo & part_info, MergeTreeDataFormatVersion format_version, LoggerPtr log)
{
    NameSet files_not_to_remove;

    /// No mutations -- no hardlinks -- no issues
    if (part_info.mutation == 0)
        return {false, files_not_to_remove};

    /// Getting all zero copy parts
    Strings parts_str;
    zookeeper_ptr->tryGetChildren(zero_copy_part_path_prefix, parts_str);

    /// Parsing infos. It's hard to convert info -> string for old-format merge tree
    /// so storing string as is.
    std::vector<std::pair<MergeTreePartInfo, std::string>> parts_infos;
    for (const auto & part_str : parts_str)
    {
        MergeTreePartInfo parent_candidate_info = MergeTreePartInfo::fromPartName(part_str, format_version);
        parts_infos.emplace_back(parent_candidate_info, part_str);
    }

    /// Sort is important. We need to find our closest parent, like:
    /// for part all_0_0_0_64 we can have parents
    /// all_0_0_0_6 < we need the closest parent, not others
    /// all_0_0_0_1
    /// all_0_0_0
    std::sort(parts_infos.begin(), parts_infos.end());
    std::string part_info_str = part_info.getPartNameV1();

    /// In reverse order to process from bigger to smaller
    for (const auto & [parent_candidate_info, part_candidate_info_str] : parts_infos | std::views::reverse)
    {
        if (parent_candidate_info == part_info)
            continue;

        /// We are mutation child of this parent
        if (part_info.isMutationChildOf(parent_candidate_info))
        {
            LOG_TRACE(log, "Found mutation parent {} for part {}", part_candidate_info_str, part_info_str);
            /// Get hardlinked files
            String files_not_to_remove_str;
            Coordination::Error code;
            zookeeper_ptr->tryGet(fs::path(zero_copy_part_path_prefix) / part_candidate_info_str, files_not_to_remove_str, nullptr, nullptr, &code);
            if (code != Coordination::Error::ZOK)
            {
                LOG_INFO(log, "Cannot get parent files from ZooKeeper on path ({}), error {}, assuming the parent was removed concurrently",
                            (fs::path(zero_copy_part_path_prefix) / part_candidate_info_str).string(), code);
                continue;
            }

            if (!files_not_to_remove_str.empty())
            {
                boost::split(files_not_to_remove, files_not_to_remove_str, boost::is_any_of("\n "));
                LOG_TRACE(log, "Found files not to remove from parent part {}: [{}]", part_candidate_info_str, fmt::join(files_not_to_remove, ", "));
            }
            else
            {
                std::vector<std::string> children;
                code = zookeeper_ptr->tryGetChildren(fs::path(zero_copy_part_path_prefix) / part_candidate_info_str, children);
                if (code != Coordination::Error::ZOK)
                {
                    LOG_INFO(log, "Cannot get parent locks in ZooKeeper on path ({}), error {}, assuming the parent was removed concurrently",
                              (fs::path(zero_copy_part_path_prefix) / part_candidate_info_str).string(), errorMessage(code));
                    continue;
                }

                if (children.size() > 1 || (children.size() == 1 && children[0] != ZeroCopyLock::ZERO_COPY_LOCK_NAME))
                {
                    LOG_TRACE(log, "No files not to remove found for part {} from parent {}", part_info_str, part_candidate_info_str);
                }
                else
                {
                    /// The case when part is actually removed, but some stale replica trying to execute merge/mutation.
                    /// We shouldn't use the part to check hardlinked blobs, it just doesn't exist.
                    LOG_TRACE(log, "Part {} is not parent (only merge/mutation locks exist), refusing to use as parent", part_candidate_info_str);
                    continue;
                }
            }

            return {true, files_not_to_remove};
        }
    }
    LOG_TRACE(log, "No mutation parent found for part {}", part_info_str);
    return {false, files_not_to_remove};
}

}

std::pair<bool, NameSet> StorageZeroCopyReplicatedMergeTree::unlockSharedDataByID(
        String part_id, const String & table_uuid, const MergeTreePartInfo & part_info,
        const String & replica_name_, const std::string & disk_type, const ZooKeeperWithFaultInjectionPtr & zookeeper_ptr, const MergeTreeSettings & settings,
        LoggerPtr logger, const String & zookeeper_path_old, MergeTreeDataFormatVersion data_format_version)
{
    boost::replace_all(part_id, "/", "_");

    auto part_name = part_info.getPartNameV1();

    Strings zc_zookeeper_paths = getZeroCopyPartPath(settings, disk_type, table_uuid, part_name, zookeeper_path_old);

    bool part_has_no_more_locks = true;
    NameSet files_not_to_remove;

    for (const auto & zc_zookeeper_path : zc_zookeeper_paths)
    {
        String files_not_to_remove_str;
        zookeeper_ptr->tryGet(zc_zookeeper_path, files_not_to_remove_str);

        files_not_to_remove.clear();
        if (!files_not_to_remove_str.empty())
            boost::split(files_not_to_remove, files_not_to_remove_str, boost::is_any_of("\n "));

        String zookeeper_part_uniq_node = fs::path(zc_zookeeper_path) / part_id;

        /// Delete our replica node for part from zookeeper (we are not interested in it anymore)
        String zookeeper_part_replica_node = fs::path(zookeeper_part_uniq_node) / replica_name_;

        auto [has_parent, parent_not_to_remove] = getParentLockedBlobs(
            zookeeper_ptr, fs::path(zc_zookeeper_path).parent_path(), part_info, data_format_version, logger);

        files_not_to_remove.insert(parent_not_to_remove.begin(), parent_not_to_remove.end());

        LOG_TRACE(logger, "Removing zookeeper lock {} for part {} (files to keep: [{}])", zookeeper_part_replica_node, part_name, fmt::join(files_not_to_remove, ", "));

        fiu_do_on(FailPoints::zero_copy_unlock_zk_fail_before_op, { zookeeper_ptr->forceFailureBeforeOperation(); });
        fiu_do_on(FailPoints::zero_copy_unlock_zk_fail_after_op, { zookeeper_ptr->forceFailureAfterOperation(); });

        if (auto ec = zookeeper_ptr->tryRemove(zookeeper_part_replica_node); ec != Coordination::Error::ZOK)
        {
            /// Very complex case. It means that lock already doesn't exist when we tried to remove it.
            /// So we don't know are we owner of this part or not. Maybe we just mutated it, renamed on disk and failed to lock in ZK.
            /// But during mutation we can have hardlinks to another part. So it's not Ok to remove blobs of this part if it was mutated.
            if (ec == Coordination::Error::ZNONODE)
            {
                if (has_parent)
                {
                    LOG_INFO(logger, "Lock on path {} for part {} doesn't exist, refuse to remove blobs", zookeeper_part_replica_node, part_name);
                    return {false, {}};
                }

                LOG_INFO(
                    logger,
                    "Lock on path {} for part {} doesn't exist, but we don't have mutation parent, can remove blobs",
                    zookeeper_part_replica_node,
                    part_name);
            }
            else
            {
                throw zkutil::KeeperException::fromPath(ec, zookeeper_part_replica_node);
            }
        }

        /// Check, maybe we were the last replica and can remove part forever
        Strings children;
        zookeeper_ptr->tryGetChildren(zookeeper_part_uniq_node, children);

        if (!children.empty())
        {
            LOG_TRACE(logger, "Found {} ({}) zookeeper locks for {}", children.size(), fmt::join(children, ", "), zookeeper_part_uniq_node);
            part_has_no_more_locks = false;
            continue;
        }

        LOG_TRACE(logger, "No more children left for {}, will try to remove the whole node", zookeeper_part_uniq_node);


        auto error_code = zookeeper_ptr->tryRemove(zookeeper_part_uniq_node);

        if (error_code == Coordination::Error::ZOK)
        {
            LOG_TRACE(logger, "Removed last parent zookeeper lock {} for part {} with id {}", zookeeper_part_uniq_node, part_name, part_id);
        }
        else if (error_code == Coordination::Error::ZNOTEMPTY)
        {
            LOG_TRACE(
                logger,
                "Cannot remove last parent zookeeper lock {} for part {} with id {}, another replica locked part concurrently",
                zookeeper_part_uniq_node,
                part_name,
                part_id);
            part_has_no_more_locks = false;
            continue;
        }
        else if (error_code == Coordination::Error::ZNONODE)
        {
            LOG_TRACE(logger, "Node with parent zookeeper lock {} for part {} with id {} doesn't exist", zookeeper_part_uniq_node, part_name, part_id);
        }
        else
        {
            throw zkutil::KeeperException::fromPath(error_code, zookeeper_part_uniq_node);
        }


        /// Even when we have lock with same part name, but with different uniq, we can remove files on S3
        children.clear();
        String zookeeper_part_node = fs::path(zookeeper_part_uniq_node).parent_path();
        zookeeper_ptr->tryGetChildren(zookeeper_part_node, children);

        if (children.empty())
        {
            /// Cleanup after last uniq removing
            error_code = zookeeper_ptr->tryRemove(zookeeper_part_node);

            if (error_code == Coordination::Error::ZOK)
            {
                LOG_TRACE(logger, "Removed last parent zookeeper lock {} for part {} (part is finally unlocked)", zookeeper_part_node, part_name);
            }
            else if (error_code == Coordination::Error::ZNOTEMPTY)
            {
                LOG_TRACE(logger, "Cannot remove last parent zookeeper lock {} for part {}, another replica locked part concurrently", zookeeper_part_uniq_node, part_name);
            }
            else if (error_code == Coordination::Error::ZNONODE)
            {
                /// We don't know what to do, because this part can be mutation part
                /// with hardlinked columns. Since we don't have this information (about blobs not to remove)
                /// we refuse to remove blobs.
                LOG_WARNING(logger, "Node with parent zookeeper lock {} for part {} doesn't exist (part was unlocked before), refuse to remove blobs", zookeeper_part_uniq_node, part_name);
                return {false, {}};
            }
            else
            {
                throw zkutil::KeeperException::fromPath(error_code, zookeeper_part_uniq_node);
            }
        }
        else
        {
            /// It's possible that we have two instances of the same part with different blob names of
            /// FILE_FOR_REFERENCES_CHECK aka checksums.txt aka part_unique_id,
            /// and other files in both parts are hardlinks (the same blobs are shared between part instances).
            /// It's possible after unsuccessful attempts to commit a mutated part to zk.
            /// It's not a problem if we have found the mutation parent (so we have files_not_to_remove).
            /// But in rare cases mutations parents could have been already removed (so we don't have the list of hardlinks).

            /// I'm not 100% sure that parent_not_to_remove list cannot be incomplete (when we've found a parent)
            if (part_info.mutation && !has_parent)
                part_has_no_more_locks = false;

            LOG_TRACE(logger, "Can't remove parent zookeeper lock {} for part {}, because children {} ({}) exists (can remove blobs: {})",
                zookeeper_part_node, part_name, children.size(), fmt::join(children, ", "), part_has_no_more_locks);
        }
    }

    return std::make_pair(part_has_no_more_locks, files_not_to_remove);
}


MergeTreeData::MutableDataPartPtr StorageZeroCopyReplicatedMergeTree::tryToFetchIfShared(
    const IMergeTreeDataPart & part,
    const DiskPtr & disk,
    const String & path)
{
    const auto settings = getSettings();
    auto data_source_description = disk->getDataSourceDescription();
    if (!(disk->supportZeroCopyReplication() && (*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication]))
        return nullptr;

    String replica = getSharedDataReplica(part, data_source_description);

    /// We can't fetch part when none replicas have this part on a same type remote disk
    if (replica.empty())
        return nullptr;

    return executeFetchShared(replica, part.name, disk, path);
}

String StorageZeroCopyReplicatedMergeTree::getSharedDataReplica(
    const IMergeTreeDataPart & part, const DataSourceDescription & data_source_description) const
{
    String best_replica;

    zkutil::ZooKeeperPtr zookeeper = tryGetZooKeeper();
    if (!zookeeper)
        return "";

    Strings zc_zookeeper_paths = getZeroCopyPartPath(*getSettings(), data_source_description.toString(), getTableSharedID(), part.name,
            zookeeper_path);

    std::set<String> replicas;

    for (const auto & zc_zookeeper_path : zc_zookeeper_paths)
    {
        Strings ids;
        zookeeper->tryGetChildren(zc_zookeeper_path, ids);

        for (const auto & id : ids)
        {
            String zookeeper_part_uniq_node = fs::path(zc_zookeeper_path) / id;
            Strings id_replicas;
            zookeeper->tryGetChildren(zookeeper_part_uniq_node, id_replicas);
            LOG_TRACE(log, "Found zookeeper replicas for {}: {}", zookeeper_part_uniq_node, id_replicas.size());
            replicas.insert(id_replicas.begin(), id_replicas.end());
        }
    }

    LOG_TRACE(log, "Found zookeeper replicas for part {}: {}", part.name, replicas.size());

    Strings active_replicas;

    /// TODO: Move best replica choose in common method (here is the same code as in StorageReplicatedMergeTree::fetchPartition)

    /// Leave only active replicas.
    active_replicas.reserve(replicas.size());

    for (const String & replica : replicas)
        if ((replica != replica_name) && (zookeeper->exists(fs::path(zookeeper_path) / "replicas" / replica / "is_active")))
            active_replicas.push_back(replica);

    LOG_TRACE(log, "Found zookeeper active replicas for part {}: {}", part.name, active_replicas.size());

    if (active_replicas.empty())
        return "";

    /** You must select the best (most relevant) replica.
    * This is a replica with the maximum `log_pointer`, then with the minimum `queue` size.
    * NOTE This is not exactly the best criteria. It does not make sense to download old partitions,
    *  and it would be nice to be able to choose the replica closest by network.
    * NOTE Of course, there are data races here. You can solve it by retrying.
    */
    Int64 max_log_pointer = -1;
    UInt64 min_queue_size = std::numeric_limits<UInt64>::max();

    for (const String & replica : active_replicas)
    {
        String current_replica_path = fs::path(zookeeper_path) / "replicas" / replica;

        String log_pointer_str = zookeeper->get(fs::path(current_replica_path) / "log_pointer");
        Int64 log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

        Coordination::Stat stat;
        zookeeper->get(fs::path(current_replica_path) / "queue", &stat);
        size_t queue_size = stat.numChildren;

        if (log_pointer > max_log_pointer
            || (log_pointer == max_log_pointer && queue_size < min_queue_size))
        {
            max_log_pointer = log_pointer;
            min_queue_size = queue_size;
            best_replica = replica;
        }
    }

    return best_replica;
}

Strings StorageZeroCopyReplicatedMergeTree::getZeroCopyPartPath(
    const MergeTreeSettings & settings, const std::string & disk_type, const String & table_uuid,
    const String & part_name, const String & zookeeper_path_old)
{
    Strings res;

    String zero_copy = fmt::format("zero_copy_{}", disk_type);

    String new_path = fs::path(settings[MergeTreeSetting::remote_fs_zero_copy_zookeeper_path].toString()) / zero_copy / table_uuid / part_name;
    res.push_back(std::move(new_path));
    if (settings[MergeTreeSetting::remote_fs_zero_copy_path_compatible_mode] && !zookeeper_path_old.empty())
    { /// Compatibility mode for cluster with old and new versions
        String old_path = fs::path(zookeeper_path_old) / zero_copy / "shared" / part_name;
        res.push_back(std::move(old_path));
    }

    return res;
}

void StorageZeroCopyReplicatedMergeTree::watchZeroCopyLock(const String & part_name, const DiskPtr & disk)
{
    auto path = getZeroCopyPartPath(part_name, disk);
    if (path)
    {
        auto zookeeper = getZooKeeper();
        auto lock_path = fs::path(*path) / "part_exclusive_lock";
        LOG_TEST(log, "Adding zero-copy lock on {}", lock_path);
        /// Looks ugly, but we cannot touch any storage fields inside Watch callback
        /// because it could lead to use-after-free (storage dropped and watch triggered)
        std::shared_ptr<std::atomic<bool>> flag = std::make_shared<std::atomic<bool>>(true);
        std::string replica;
        bool exists = zookeeper->tryGetWatch(lock_path, replica, nullptr, [flag] (const Coordination::WatchResponse &)
        {
            *flag = false;
        });

        if (exists)
        {
            std::lock_guard lock(existing_zero_copy_locks_mutex);
            existing_zero_copy_locks[lock_path] = ZeroCopyLockDescription{replica, flag};
        }
    }
}

bool StorageZeroCopyReplicatedMergeTree::checkZeroCopyLockExists(const String & part_name, const DiskPtr & disk, String & lock_replica)
{
    auto path = getZeroCopyPartPath(part_name, disk);

    std::lock_guard lock(existing_zero_copy_locks_mutex);
    /// Cleanup abandoned locks during each check. The set of locks is small and this is quite fast loop.
    /// Also it's hard to properly remove locks because we can execute replication queue
    /// in arbitrary order and some parts can be replaced by covering parts without merges.
    for (auto it = existing_zero_copy_locks.begin(); it != existing_zero_copy_locks.end();)
    {
        if (*it->second.exists)
            ++it;
        else
        {
            LOG_TEST(log, "Removing zero-copy lock on {}", it->first);
            it = existing_zero_copy_locks.erase(it);
        }
    }

    if (path)
    {
        auto lock_path = fs::path(*path) / "part_exclusive_lock";
        if (auto it = existing_zero_copy_locks.find(lock_path); it != existing_zero_copy_locks.end())
        {
            lock_replica = it->second.replica;
            if (*it->second.exists)
            {
                LOG_TEST(log, "Zero-copy lock on path {} exists", it->first);
                return true;
            }
        }

        LOG_TEST(log, "Zero-copy lock on path {} doesn't exist", lock_path);
    }

    return false;
}

std::optional<String> StorageZeroCopyReplicatedMergeTree::getZeroCopyPartPath(const String & part_name, const DiskPtr & disk)
{
    if (!disk || !disk->supportZeroCopyReplication())
        return std::nullopt;

    return getZeroCopyPartPath(*getSettings(), disk->getDataSourceDescription().toString(), getTableSharedID(), part_name, zookeeper_path)[0];
}

bool StorageZeroCopyReplicatedMergeTree::waitZeroCopyLockToDisappear(const ZeroCopyLock & lock, size_t milliseconds_to_wait)
{
    if (lock.isLocked())
        return true;

    if (partial_shutdown_called.load(std::memory_order_relaxed))
        return true;

    auto lock_path = lock.lock->getLockPath();
    zkutil::ZooKeeperPtr zookeeper = tryGetZooKeeper();
    if (!zookeeper)
        return true;

    Stopwatch time_waiting;
    const auto & stop_waiting = [&]()
    {
        bool timeout_exceeded = milliseconds_to_wait < time_waiting.elapsedMilliseconds();
        return partial_shutdown_called.load(std::memory_order_relaxed) || is_readonly.load(std::memory_order_relaxed) || timeout_exceeded;
    };

    return zookeeper->waitForDisappear(lock_path, stop_waiting);
}

std::optional<ZeroCopyLock> StorageZeroCopyReplicatedMergeTree::tryCreateZeroCopyExclusiveLock(const String & part_name, const DiskPtr & disk)
{
    if (!disk || !disk->supportZeroCopyReplication())
        return std::nullopt;

    if (partial_shutdown_called.load(std::memory_order_relaxed) || is_readonly.load(std::memory_order_relaxed))
        return std::nullopt;

    zkutil::ZooKeeperPtr zookeeper = tryGetZooKeeper();
    if (!zookeeper)
        return std::nullopt;

    String zc_zookeeper_path = *getZeroCopyPartPath(part_name, disk);

    /// Just recursively create ancestors for lock
    zookeeper->createAncestors(zc_zookeeper_path + "/");

    /// Create actual lock
    ZeroCopyLock lock(zookeeper, zc_zookeeper_path, replica_name);
    lock.lock->tryLock();
    return lock;
}

std::vector<String> StorageZeroCopyReplicatedMergeTree::getZookeeperZeroCopyLockPaths() const
{
    const auto settings = getSettings();
    if (!(*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        return {};
    }

    const auto & disks = getStoragePolicy()->getDisks();
    std::set<String> disk_types_with_zero_copy;
    for (const auto & disk : disks)
    {
        if (!disk->supportZeroCopyReplication())
            continue;

        disk_types_with_zero_copy.insert(disk->getDataSourceDescription().toString());
    }

    const auto actual_table_shared_id = getTableSharedID();

    std::vector<String> result;
    result.reserve(disk_types_with_zero_copy.size());

    for (const auto & disk_type: disk_types_with_zero_copy)
    {
        auto zero_copy = fmt::format("zero_copy_{}", disk_type);
        auto zero_copy_path = fs::path((*settings)[MergeTreeSetting::remote_fs_zero_copy_zookeeper_path].toString()) / zero_copy;

        result.push_back(zero_copy_path / actual_table_shared_id);
    }

    return result;
}

void StorageZeroCopyReplicatedMergeTree::dropZookeeperZeroCopyLockPaths(zkutil::ZooKeeperPtr zookeeper, std::vector<String> zero_copy_locks_paths,
                                                                LoggerPtr logger)
{
    for (const auto & zero_copy_locks_root : zero_copy_locks_paths)
    {
        auto code = zookeeper->tryRemove(zero_copy_locks_root);
        if (code == Coordination::Error::ZNOTEMPTY)
        {
            LOG_WARNING(logger, "Zero copy locks are not empty for {}. There are some lost locks inside."
                              "Removing them all.", zero_copy_locks_root);
            zookeeper->tryRemoveRecursive(zero_copy_locks_root);
        }
        else if (code == Coordination::Error::ZNONODE)
        {
            LOG_INFO(logger, "Zero copy locks directory {} is absent on ZooKeeper.", zero_copy_locks_root);
        }
        else
        {
            chassert(code == Coordination::Error::ZOK);
        }
    }
}


void StorageZeroCopyReplicatedMergeTree::createZeroCopyLockNode(
    const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_node, int32_t mode,
    bool replace_existing_lock, const String & path_to_set_hardlinked_files, const NameSet & hardlinked_files)
{
    /// In rare case other replica can remove path between createAncestors and createIfNotExists
    /// So we make up to 5 attempts

    auto is_ephemeral = [&](const String & node_path) -> bool
    {
        String dummy_res;
        Coordination::Stat node_stat;
        if (zookeeper->tryGet(node_path, dummy_res, &node_stat))
            return node_stat.ephemeralOwner;
        return false;
    };

    bool created = false;
    for (int attempts = 5; attempts > 0; --attempts)
    {
        Coordination::Requests ops;
        Coordination::Responses responses;
        getZeroCopyLockNodeCreateOps(zookeeper, zookeeper_node, ops, mode, replace_existing_lock, path_to_set_hardlinked_files, hardlinked_files);

        fiu_do_on(FailPoints::zero_copy_lock_zk_fail_before_op, { zookeeper->forceFailureBeforeOperation(); });
        fiu_do_on(FailPoints::zero_copy_lock_zk_fail_after_op, { zookeeper->forceFailureAfterOperation(); });

        auto error = zookeeper->tryMulti(ops, responses);
        if (error == Coordination::Error::ZOK)
        {
            created = true;
            break;
        }
        if (mode == zkutil::CreateMode::Persistent)
        {
            if (error == Coordination::Error::ZNONODE)
                continue;

            if (error == Coordination::Error::ZNODEEXISTS)
            {
                if (is_ephemeral(zookeeper_node))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} already exists, but it is ephemeral", zookeeper_node);

                size_t failed_op = zkutil::getFailedOpIndex(error, responses);
                /// Part was locked before, unfortunately it's possible during moves
                if (ops[failed_op]->getPath() == zookeeper_node)
                {
                    created = true;
                    break;
                }
                continue;
            }
        }
        else if (mode == zkutil::CreateMode::Ephemeral)
        {
            /// It is super rare case when we had part, but it was lost and we were unable to unlock it from keeper.
            /// Now we are trying to fetch it from other replica and unlocking.
            if (error == Coordination::Error::ZNODEEXISTS)
            {
                size_t failed_op = zkutil::getFailedOpIndex(error, responses);
                if (ops[failed_op]->getPath() == zookeeper_node)
                {
                    LOG_WARNING(
                        getLogger("ZeroCopyLocks"),
                        "Replacing persistent lock with ephemeral for path {}. It can happen only in case of local part loss",
                        zookeeper_node);
                    replace_existing_lock = true;
                    continue;
                }
            }
        }

        zkutil::KeeperMultiException::check(error, ops, responses);
    }

    if (!created)
    {
        String mode_str = mode == zkutil::CreateMode::Persistent ? "persistent" : "ephemeral";
        throw Exception(ErrorCodes::NOT_FOUND_NODE,
                        "Cannot create {} zero copy lock {} because part was unlocked from zookeeper",
                        mode_str, zookeeper_node);
    }
}

bool StorageZeroCopyReplicatedMergeTree::removeSharedDetachedPart(DiskPtr disk, const String & path, const String & part_name, const String & table_uuid,
    const String & detached_replica_name, const String & detached_zookeeper_path, const ContextPtr & local_context, const zkutil::ZooKeeperPtr & zookeeper)
{
    bool keep_shared = false;

    NameSet files_not_to_remove;

    // zero copy replication is only available since format version 1 so we can safely use it here
    auto part_info = DetachedPartInfo::parseDetachedPartName(disk, part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    if (!part_info.valid_name)
        throw Exception(ErrorCodes::BAD_DATA_PART_NAME, "Invalid detached part name {} on disk {}", path, disk->getName());

    fs::path checksums = fs::path(path) / IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK;
    if (disk->existsFile(checksums))
    {
        if (disk->getRefCount(checksums) == 0)
        {
            String id = disk->getUniqueId(checksums);
            bool can_remove = false;
            std::tie(can_remove, files_not_to_remove) = StorageReplicatedMergeTree::unlockSharedDataByID(
                id, table_uuid, part_info,
                detached_replica_name,
                disk->getDataSourceDescription().toString(),
                std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), local_context->getReplicatedMergeTreeSettings(),
                getLogger("StorageReplicatedMergeTree"),
                detached_zookeeper_path,
                MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);

            keep_shared = !can_remove;
        }
        else
            keep_shared = true;
    }

    disk->removeSharedRecursive(path, keep_shared, files_not_to_remove);

    return keep_shared;
}

bool StorageZeroCopyReplicatedMergeTree::canUseZeroCopyReplication() const
{
    auto settings_ptr = getSettings();
    if (!(*settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
        return false;

    auto disks = getStoragePolicy()->getDisks();
    for (const auto & disk : disks)
    {
        if (disk->supportZeroCopyReplication())
            return true;
    }
    return false;
}

zkutil::EphemeralNodeHolderPtr StorageZeroCopyReplicatedMergeTree::lockSharedDataTemporary(const String & part_name, const String & part_id, const DiskPtr & disk) const
{
    auto settings = getSettings();

    if (!disk || !disk->supportZeroCopyReplication() || !(*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
        return {};

    zkutil::ZooKeeperPtr zookeeper = tryGetZooKeeper();
    if (!zookeeper)
        return {};

    String id = part_id;
    boost::replace_all(id, "/", "_");

    String zc_zookeeper_path = getZeroCopyPartPath(*getSettings(), disk->getDataSourceDescription().toString(), getTableSharedID(),
        part_name, zookeeper_path)[0];

    String zookeeper_node = fs::path(zc_zookeeper_path) / id / replica_name;

    LOG_TRACE(log, "Set zookeeper temporary ephemeral lock {}", zookeeper_node);
    createZeroCopyLockNode(
        std::make_shared<ZooKeeperWithFaultInjection>(zookeeper), zookeeper_node, zkutil::CreateMode::Ephemeral, false);

    LOG_TRACE(log, "Zookeeper temporary ephemeral lock {} created", zookeeper_node);
    return zkutil::EphemeralNodeHolder::existing(zookeeper_node, *zookeeper);
}

void StorageZeroCopyReplicatedMergeTree::getZeroCopyLockNodeCreateOps(
    const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_node, Coordination::Requests & requests,
    int32_t mode, bool replace_existing_lock,
    const String & path_to_set_hardlinked_files, const NameSet & hardlinked_files)
{

    /// Ephemeral locks can be created only when we fetch shared data.
    /// So it never require to create ancestors. If we create them
    /// race condition with source replica drop is possible.
    if (mode == zkutil::CreateMode::Persistent)
        zookeeper->checkExistsAndGetCreateAncestorsOps(zookeeper_node, requests);

    if (replace_existing_lock && zookeeper->exists(zookeeper_node))
    {
        requests.emplace_back(zkutil::makeRemoveRequest(zookeeper_node, -1));
        requests.emplace_back(zkutil::makeCreateRequest(zookeeper_node, "", mode));
        if (!path_to_set_hardlinked_files.empty() && !hardlinked_files.empty())
        {
            std::string data = boost::algorithm::join(hardlinked_files, "\n");
            /// List of files used to detect hardlinks. path_to_set_hardlinked_files --
            /// is a path to source part zero copy node. During part removal hardlinked
            /// files will be left for source part.
            requests.emplace_back(zkutil::makeSetRequest(path_to_set_hardlinked_files, data, -1));
        }
    }
    else
    {
        Coordination::Requests ops;
        if (!path_to_set_hardlinked_files.empty() && !hardlinked_files.empty())
        {
            std::string data = boost::algorithm::join(hardlinked_files, "\n");
            /// List of files used to detect hardlinks. path_to_set_hardlinked_files --
            /// is a path to source part zero copy node. During part removal hardlinked
            /// files will be left for source part.
            requests.emplace_back(zkutil::makeSetRequest(path_to_set_hardlinked_files, data, -1));
        }
        requests.emplace_back(zkutil::makeCreateRequest(zookeeper_node, "", mode));
    }
}

void StorageZeroCopyReplicatedMergeTree::createAndStoreFreezeMetadata(DiskPtr disk, DataPartPtr, String backup_part_path) const
{
    if (disk->supportZeroCopyReplication())
    {
        FreezeMetaData meta;
        meta.fill(*this);
        meta.save(disk, backup_part_path);
    }
}

virtual bool StorageZeroCopyReplicatedMergeTree::checkPartChecksumsAndAddCommitOps(
    const ZooKeeperWithFaultInjectionPtr & zookeeper,
    const DataPartPtr & part,
    Coordination::Requests & ops,
    String part_name,
    NameSet & absent_replicas_paths)
{
    getLockSharedDataOps(*part, zookeeper, replace_zero_copy_lock, hardlinked_files, ops);
    StorageReplicatedMergeTree::checkPartChecksumsAndAddCommitOps(zookeeper, part, ops, part_name, absent_replicas_paths);
}

bool StorageZeroCopyReplicatedMergeTree::isCloneNeededForReplaceRange(const DataPartPtr & part, const PartDescriptionPtr & part_desc)
{
    if (!StorageReplicatedMergeTree::isCloneNeededForReplaceRange(part, part_desc))
        return false;


    if ((*storage_settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication] && part->isStoredOnRemoteDiskWithZeroCopySupport())
    {
        LOG_DEBUG(
            log,
            "Avoid copy local part {} from table {} because of zero-copy replication",
            part_desc->src_part_name,
            source_table_id.getNameForLogs());
        return false;
    }

    return true;
}

bool StorageZeroCopyReplicatedMergeTree::removeDetachedPart(DiskPtr disk, const String & path, const String & part_name)
{
    auto settings = getSettings();

    if (disk->supportZeroCopyReplication() && (settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        String table_id = getTableSharedID();
        return removeSharedDetachedPart(disk, path, part_name, table_id, replica_name, zookeeper_path, getContext(), current_zookeeper);
    }

    return StorageReplicatedMergeTree::removeDetachedPart(disk, path, part_name);
}

std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> StorageZeroCopyReplicatedMergeTree::fetchSelectedPart(
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        const String & part_name,
        const String & zookeeper_name,
        const String & replica_path,
        const String & host,
        int port,
        const ConnectionTimeouts & timeouts,
        const String & user,
        const String & password,
        const String & interserver_scheme,
        ThrottlerPtr throttler,
        bool to_detached,
        const String & tmp_prefix_,
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
        DiskPtr disk)
{
    // TODO ZeroCopy: make fetcher polymorphic and return it from factory
    return fetchSelectedPart(
                metadata_snapshot,
                getContext(),
                part_name,
                source_zookeeper_name,
                source_replica_path,
                address.host,
                address.replication_port,
                timeouts,
                credentials->getUser(),
                credentials->getPassword(),
                interserver_scheme,
                replicated_fetches_throttler,
                to_detached,
                "",
                &tagger_ptr,
                /* try_zero_copy = */ true);
}

void StorageZeroCopyReplicatedMergeTree::createNewZooKeeperNodesAttempt() const
{

    StorageReplicatedMergeTree::createNewZooKeeperNodesAttempt();

    /// Nodes for remote fs zero-copy replication
    const auto settings = getSettings();
    if ((*settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        for (const auto & zero_copy_locks_root : getZookeeperZeroCopyLockPaths())
        {
            for (const auto & ancestor : getAncestors(zero_copy_locks_root))
            {
                futures.push_back(zookeeper->asyncTryCreateNoThrow(ancestor, String(), zkutil::CreateMode::Persistent));
            }
        }

        futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/zero_copy_s3", String(), zkutil::CreateMode::Persistent));
        futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/zero_copy_s3/shared", String(), zkutil::CreateMode::Persistent));
        futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/zero_copy_hdfs", String(), zkutil::CreateMode::Persistent));
        futures.push_back(zookeeper->asyncTryCreateNoThrow(zookeeper_path + "/zero_copy_hdfs/shared", String(), zkutil::CreateMode::Persistent));
    }

}

void StorageZeroCopyReplicatedMergeTree::prepareDrop()
{
        /// Wait for loading of all outdated parts because
    /// in case of zero copy recursive removal of directory
    /// is not supported and table cannot be dropped.
    if (canUseZeroCopyReplication() /*isLoadingOutadatedPartsNeeded()*/)
    {
        /// Load remaining parts synchronously because task
        /// for loading is already cancelled in shutdown().
        loadOutdatedDataParts(/*is_async=*/ false);
    }

    /// getZookeeperZeroCopyLockPaths has to be called before dropAllData
    /// otherwise table_shared_id is unknown
    zero_copy_locks_paths = getZookeeperZeroCopyLockPaths();
}

void StorageZeroCopyReplicatedMergeTree::onLastReplicaDropped()
{
    dropZookeeperZeroCopyLockPaths(getZooKeeper(), zero_copy_locks_paths, log.load());
}

bool StorageZeroCopyReplicatedMergeTree::isPreferredFetchFromOtherReplicaForReplaceRange(PartDescriptionPtr & part_desc)
{
    /// Fetches with zero-copy-replication are cheap, but cloneAndLoadDataPart(must_on_same_disk=true) will do full copy.
    /// It's okay to check the setting for current table and disk for the source table, because src and dst part are on the same disk.
    return !part_desc->replica.empty()
        && (*storage_settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication] && part_desc->src_table_part
        && part_desc->src_table_part->isStoredOnRemoteDiskWithZeroCopySupport();
}

ExecutableTaskPtr StorageReplicatedMergeTree::makeMutateFromLogEntryTask(const SelectedEntryPtr & selected_entry) const
{
    return std::make_shared<MutateZeroCopyFromLogEntryTask>(selected_entry, *this, common_assignee_trigger);
}



}