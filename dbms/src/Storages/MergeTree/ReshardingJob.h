#pragma once

#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <string>

namespace DB
{


class StorageReplicatedMergeTree;

/** Description of the task of rescheduling.
  */
struct ReshardingJob final
{
public:
    ReshardingJob() = default;

    /// Creates a description based on its serialized representation.
    ReshardingJob(const std::string & serialized_job);

    ReshardingJob(const std::string & database_name_, const std::string & table_name_,
        const std::string & partition_, const WeightedZooKeeperPaths & paths_,
        const ASTPtr & sharding_key_expr_, const std::string & coordinator_id_);

    ReshardingJob(const ReshardingJob &) = delete;
    ReshardingJob & operator=(const ReshardingJob &) = delete;

    ReshardingJob(ReshardingJob &&) = default;
    ReshardingJob & operator=(ReshardingJob &&) = default;

    operator bool() const;

    /// Serializes the task description.
    std::string toString() const;

    bool isCoordinated() const;

    void clear();

public:
    std::string database_name;
    std::string table_name;
    std::string partition;
    std::string job_name;
    WeightedZooKeeperPaths paths;
    ASTPtr sharding_key_expr;
    std::string coordinator_id;
    StorageReplicatedMergeTree * storage = nullptr;
    UInt64 block_number = 0;
    bool do_copy;
    bool is_aborted = false;
};

}
