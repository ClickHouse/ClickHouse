#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/MergeTree/DataPartsExchange.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <ext/shared_ptr_helper.h>

namespace DB
{

class StorageNextGenReplicatedMergeTree : public ext::shared_ptr_helper<StorageNextGenReplicatedMergeTree>, public IStorage
{
protected:
    StorageNextGenReplicatedMergeTree(
        const String & zookeeper_path_,
        const String & replica_name_,
        bool attach,
        const String & path_, const String & database_name_, const String & name_,
        const NamesAndTypesList & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        Context & context_,
        const ASTPtr & primary_expr_ast_,
        const String & date_column_name,
        const ASTPtr & partition_expr_ast_,
        const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
        const MergeTreeData::MergingParams & merging_params_,
        const MergeTreeSettings & settings_,
        bool has_force_restore_data_flag);

public:
    void startup() override;
    void shutdown() override;

    ~StorageNextGenReplicatedMergeTree() override;


    String getName() const override
    {
        return "NextGenReplicated" + data.merging_params.getModeName() + "MergeTree";
    }

    String getTableName() const override { return table_name; }
    bool supportsSampling() const override { return data.supportsSampling(); }
    bool supportsFinal() const override { return data.supportsFinal(); }
    bool supportsPrewhere() const override { return data.supportsPrewhere(); }
    bool supportsReplication() const override { return true; }
    bool supportsIndexForIn() const override { return true; }

    const NamesAndTypesList & getColumnsListImpl() const override { return data.getColumnsListNonMaterialized(); }
    NameAndTypePair getColumn(const String & column_name) const override { return data.getColumn(column_name); }
    bool hasColumn(const String & column_name) const override { return data.hasColumn(column_name); }


    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;


    void drop() override;

private:
    Context & context;
    Logger * log;

    String database_name;
    String table_name;
    String full_path;

    MergeTreeData data;
    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMerger merger;

    InterserverIOEndpointHolderPtr parts_exchange_service;
    DataPartsExchange::Fetcher parts_fetcher;

    String zookeeper_path;
    String replica_name;
    String replica_path;

    void createTableOrReplica(zkutil::ZooKeeper & zookeeper);

    zkutil::ZooKeeperPtr tryGetZooKeeper();
    zkutil::ZooKeeperPtr getZooKeeper();

    std::atomic<bool> is_readonly {false};
    std::atomic<bool> shutdown_called {false};

    zkutil::EphemeralNodeHolderPtr is_active_node;

    struct Part
    {
        enum class State
        {
            Ephemeral,
            Virtual,
            Preparing,
            Prepared,
            MaybeCommitted,
            Committed,
            Outdated,
            Deleting,
        };

        MergeTreePartInfo info;
        String name;
        State state;
    };

    mutable std::mutex parts_mutex;
    std::map<MergeTreePartInfo, Part> parts;

    void initPartSet();

    /// A thread that updates the part set based on ZooKeeper notifications.
    std::thread part_set_updating_thread;
    zkutil::EventPtr part_set_updating_event = std::make_shared<Poco::Event>();
    void runPartSetUpdatingThread();

    /// A task that performs actions to get needed parts.
    BackgroundProcessingPool::TaskHandle parts_producing_task;
    bool runPartsProducingTask();

    /// A thread that selects parts to merge.
    std::thread merge_selecting_thread;
    zkutil::EventPtr merge_selecting_event = std::make_shared<Poco::Event>();
    void runMergeSelectingThread();

    friend class NextGenReplicatedBlockOutputStream;
};

}
