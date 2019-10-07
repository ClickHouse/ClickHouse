#include <Storages/MergeTree/ReplicatedMergeTreeAlterThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Databases/IDatabase.h>

#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
}

static const auto ALTER_ERROR_SLEEP_MS = 10 * 1000;


ReplicatedMergeTreeAlterThread::ReplicatedMergeTreeAlterThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , zk_node_cache([&] { return storage.getZooKeeper(); })
    , log_name(storage.database_name + "." + storage.table_name + " (ReplicatedMergeTreeAlterThread)")
    , log(&Logger::get(log_name))
{
    task = storage_.global_context.getSchedulePool().createTask(log_name, [this]{ run(); });
}

void ReplicatedMergeTreeAlterThread::run()
{
    try
    {
        /** We have a description of columns in ZooKeeper, common for all replicas (Example: /clickhouse/tables/02-06/visits/columns),
          *  as well as a description of columns in local file with metadata (storage.getColumnsList()).
          *
          * If these descriptions are different - you need to do ALTER.
          *
          * If stored version of the node (columns_version) differs from the version in ZK,
          *  then the description of the columns in ZK does not necessarily differ from the local
          *  - this can happen with a loop from ALTER-s, which as a whole, does not change anything.
          * In this case, you need to update the stored version number,
          *  and also check the structure of parts, and, if necessary, make ALTER.
          *
          * Recorded version number needs to be updated after updating the metadata, under lock.
          * This version number is checked against the current one for INSERT.
          * That is, we make sure to insert blocks with the correct structure.
          *
          * When the server starts, previous ALTER might not have been completed.
          * Therefore, for the first time, regardless of the changes, we check the structure of all parts,
          *  (Example: /clickhouse/tables/02-06/visits/replicas/example02-06-1.yandex.ru/parts/20140806_20140831_131664_134988_3296/columns)
          *  and do ALTER if necessary.
          *
          * TODO: Too complicated, rewrite everything.
          */

        auto zookeeper = storage.getZooKeeper();

        String columns_path = storage.zookeeper_path + "/columns";
        auto columns_znode = zk_node_cache.get(columns_path, task->getWatchCallback());
        if (!columns_znode.exists)
            throw Exception(columns_path + " doesn't exist", ErrorCodes::NOT_FOUND_NODE);
        int32_t columns_version = columns_znode.stat.version;

        String metadata_path = storage.zookeeper_path + "/metadata";
        auto metadata_znode = zk_node_cache.get(metadata_path, task->getWatchCallback());
        if (!metadata_znode.exists)
            throw Exception(metadata_path + " doesn't exist", ErrorCodes::NOT_FOUND_NODE);
        int32_t metadata_version = metadata_znode.stat.version;

        const bool changed_columns_version = (columns_version != storage.columns_version);
        const bool changed_metadata_version = (metadata_version != storage.metadata_version);

        if (!(changed_columns_version || changed_metadata_version || force_recheck_parts))
            return;

        const String & columns_str = columns_znode.contents;
        auto columns_in_zk = ColumnsDescription::parse(columns_str);

        const String & metadata_str = metadata_znode.contents;
        auto metadata_in_zk = ReplicatedMergeTreeTableMetadata::parse(metadata_str);
        auto metadata_diff = ReplicatedMergeTreeTableMetadata(storage).checkAndFindDiff(metadata_in_zk, /* allow_alter = */ true);

        /// If you need to lock table structure, then suspend merges.
        ActionLock merge_blocker = storage.merger_mutator.actions_blocker.cancel();

        MergeTreeData::DataParts parts;

        /// If metadata nodes have changed, we will update table structure locally.
        if (changed_columns_version || changed_metadata_version)
        {
            /// Temporarily cancel part checks to avoid locking for long time.
            auto temporarily_stop_part_checks = storage.part_check_thread.temporarilyStop();

            /// Temporarily cancel parts sending
            ActionLock data_parts_exchange_blocker;
            if (storage.data_parts_exchange_endpoint_holder)
                data_parts_exchange_blocker = storage.data_parts_exchange_endpoint_holder->getBlocker().cancel();

            /// Temporarily cancel part fetches
            auto fetches_blocker = storage.fetcher.blocker.cancel();

            LOG_INFO(log, "Version of metadata nodes in ZooKeeper changed. Waiting for structure write lock.");

            auto table_lock = storage.lockExclusively(RWLockImpl::NO_QUERY);

            if (columns_in_zk == storage.getColumns() && metadata_diff.empty())
            {
                LOG_INFO(log, "Metadata nodes changed in ZooKeeper, but their contents didn't change. "
                    "Most probably it is a cyclic ALTER.");
            }
            else
            {
                LOG_INFO(log, "Metadata changed in ZooKeeper. Applying changes locally.");

                storage.setTableStructure(std::move(columns_in_zk), metadata_diff);

                LOG_INFO(log, "Applied changes to the metadata of the table.");
            }

            /// You need to get a list of parts under table lock to avoid race condition with merge.
            parts = storage.getDataParts();

            storage.columns_version = columns_version;
            storage.metadata_version = metadata_version;
        }

        /// Update parts.
        if (changed_columns_version || force_recheck_parts)
        {
            auto table_lock = storage.lockStructureForShare(false, RWLockImpl::NO_QUERY);

            if (changed_columns_version)
                LOG_INFO(log, "ALTER-ing parts");

            int changed_parts = 0;

            if (!changed_columns_version)
                parts = storage.getDataParts();

            const auto columns_for_parts = storage.getColumns().getAllPhysical();
            const auto indices_for_parts = storage.getIndices();

            for (const MergeTreeData::DataPartPtr & part : parts)
            {
                /// Update the part and write result to temporary files.
                /// TODO: You can skip checking for too large changes if ZooKeeper has, for example,
                /// node /flags/force_alter.
                MergeTreeData::AlterDataPartTransactionPtr transaction(new MergeTreeData::AlterDataPartTransaction(part));
                storage.alterDataPart(columns_for_parts, indices_for_parts.indices, false, transaction);
                if (!transaction->isValid())
                    continue;

                storage.updatePartHeaderInZooKeeperAndCommit(zookeeper, *transaction);

                ++changed_parts;
            }

            /// Columns sizes could be quietly changed in case of MODIFY/ADD COLUMN
            storage.recalculateColumnSizes();

            if (changed_columns_version)
            {
                if (changed_parts != 0)
                    LOG_INFO(log, "ALTER-ed " << changed_parts << " parts");
                else
                    LOG_INFO(log, "No parts ALTER-ed");
            }
        }

        /// Update metadata ZK nodes for a specific replica.
        if (changed_columns_version || force_recheck_parts)
            zookeeper->set(storage.replica_path + "/columns", columns_str);
        if (changed_metadata_version || force_recheck_parts)
            zookeeper->set(storage.replica_path + "/metadata", metadata_str);

        force_recheck_parts = false;
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::ZSESSIONEXPIRED)
            return;

        force_recheck_parts = true;
        task->scheduleAfter(ALTER_ERROR_SLEEP_MS);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        force_recheck_parts = true;
        task->scheduleAfter(ALTER_ERROR_SLEEP_MS);
    }
}

}
