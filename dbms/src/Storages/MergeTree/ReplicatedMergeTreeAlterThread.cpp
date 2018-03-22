#include <memory>
#include <Common/setThreadName.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAlterThread.h>
#include <Databases/IDatabase.h>


namespace DB
{

static const auto ALTER_ERROR_SLEEP_MS = 10 * 1000;


ReplicatedMergeTreeAlterThread::ReplicatedMergeTreeAlterThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_),
    log(&Logger::get(storage.database_name + "." + storage.table_name + " (StorageReplicatedMergeTree, AlterThread)")),
    thread([this] { run(); }) {}


void ReplicatedMergeTreeAlterThread::run()
{
    setThreadName("ReplMTAlter");

    bool force_recheck_parts = true;

    while (!need_stop)
    {
        try
        {
            /** We have a description of columns in ZooKeeper, common for all replicas (Example: /clickhouse/tables/02-06/visits/columns),
              *  as well as a description of columns in local file with metadata (storage.data.getColumnsList()).
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

            zkutil::Stat stat;
            const String columns_str = zookeeper->get(storage.zookeeper_path + "/columns", &stat, wakeup_event);
            auto columns_in_zk = ColumnsDescription::parse(columns_str);

            bool changed_version = (stat.version != storage.columns_version);

            {
                /// If you need to lock table structure, then suspend merges.
                ActionBlocker::LockHolder merge_blocker;

                if (changed_version || force_recheck_parts)
                    merge_blocker = storage.merger.merges_blocker.cancel();

                MergeTreeData::DataParts parts;

                /// If columns description has changed, we will update table structure locally.
                if (changed_version)
                {
                    /// Temporarily cancel part checks to avoid locking for long time.
                    auto temporarily_stop_part_checks = storage.part_check_thread.temporarilyStop();

                    /// Temporarily cancel parts sending
                    ActionBlocker::LockHolder data_parts_exchange_blocker;
                    if (storage.data_parts_exchange_endpoint_holder)
                        data_parts_exchange_blocker = storage.data_parts_exchange_endpoint_holder->cancel();

                    /// Temporarily cancel part fetches
                    auto fetches_blocker = storage.fetcher.blocker.cancel();

                    LOG_INFO(log, "Changed version of 'columns' node in ZooKeeper. Waiting for structure write lock.");

                    auto table_lock = storage.lockStructureForAlter(__PRETTY_FUNCTION__);

                    if (columns_in_zk != storage.getColumns())
                    {
                        LOG_INFO(log, "Columns list changed in ZooKeeper. Applying changes locally.");

                        storage.context.getDatabase(storage.database_name)->alterTable(
                            storage.context, storage.table_name, columns_in_zk, {});
                        storage.setColumns(std::move(columns_in_zk));

                        /// Reinitialize primary key because primary key column types might have changed.
                        storage.data.initPrimaryKey();

                        LOG_INFO(log, "Applied changes to table.");
                    }
                    else
                    {
                        LOG_INFO(log, "Columns version changed in ZooKeeper, but data wasn't changed. It's like cyclic ALTERs.");
                    }

                    /// You need to get a list of parts under table lock to avoid race condition with merge.
                    parts = storage.data.getDataParts();

                    storage.columns_version = stat.version;
                }

                /// Update parts.
                if (changed_version || force_recheck_parts)
                {
                    auto table_lock = storage.lockStructure(false, __PRETTY_FUNCTION__);

                    if (changed_version)
                        LOG_INFO(log, "ALTER-ing parts");

                    int changed_parts = 0;

                    if (!changed_version)
                        parts = storage.data.getDataParts();

                    const auto columns_for_parts = storage.getColumns().getAllPhysical();

                    for (const MergeTreeData::DataPartPtr & part : parts)
                    {
                        /// Update the part and write result to temporary files.
                        /// TODO: You can skip checking for too large changes if ZooKeeper has, for example,
                        /// node /flags/force_alter.
                        auto transaction = storage.data.alterDataPart(
                            part, columns_for_parts, storage.data.primary_expr_ast, false);

                        if (!transaction)
                            continue;

                        ++changed_parts;

                        /// Update part metadata in ZooKeeper.
                        zkutil::Ops ops;
                        ops.emplace_back(std::make_shared<zkutil::Op::SetData>(
                            storage.replica_path + "/parts/" + part->name + "/columns", transaction->getNewColumns().toString(), -1));
                        ops.emplace_back(std::make_shared<zkutil::Op::SetData>(
                            storage.replica_path + "/parts/" + part->name + "/checksums",
                            storage.getChecksumsForZooKeeper(transaction->getNewChecksums()),
                            -1));

                        try
                        {
                            zookeeper->multi(ops);
                        }
                        catch (const zkutil::KeeperException & e)
                        {
                            /// The part does not exist in ZK. We will add to queue for verification - maybe the part is superfluous, and it must be removed locally.
                            if (e.code == ZNONODE)
                                storage.enqueuePartForCheck(part->name);

                            throw;
                        }

                        /// Apply file changes.
                        transaction->commit();
                    }

                    /// Columns sizes could be quietly changed in case of MODIFY/ADD COLUMN
                    storage.data.recalculateColumnSizes();

                    /// List of columns for a specific replica.
                    zookeeper->set(storage.replica_path + "/columns", columns_str);

                    if (changed_version)
                    {
                        if (changed_parts != 0)
                            LOG_INFO(log, "ALTER-ed " << changed_parts << " parts");
                        else
                            LOG_INFO(log, "No parts ALTER-ed");
                    }

                    force_recheck_parts = false;
                }

                /// It's important that parts and merge_blocker are destroyed before the wait.
            }

            wakeup_event->wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);

            force_recheck_parts = true;

            wakeup_event->tryWait(ALTER_ERROR_SLEEP_MS);
        }
    }

    LOG_DEBUG(log, "Alter thread finished");
}

}
