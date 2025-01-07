#pragma once

#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <base/types.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

class MergeTreeData;
class WriteBuffer;
class ReadBuffer;

/** The basic parameters of ReplicatedMergeTree table engine for saving in ZooKeeper.
 * Lets you verify that they match local ones.
 */
struct ReplicatedMergeTreeTableMetadata
{
    static constexpr int REPLICATED_MERGE_TREE_METADATA_LEGACY_VERSION = 1;
    static constexpr int REPLICATED_MERGE_TREE_METADATA_WITH_ALL_MERGE_PARAMETERS = 2;

    String date_column;
    String sampling_expression;
    UInt64 index_granularity;
    /// Merging related params
    int merging_params_mode;
    int merge_params_version = REPLICATED_MERGE_TREE_METADATA_WITH_ALL_MERGE_PARAMETERS;
    String sign_column;
    String version_column;
    String is_deleted_column;
    String columns_to_sum;
    String graphite_params_hash;
    String primary_key;
    MergeTreeDataFormatVersion data_format_version;
    String partition_key;
    String sorting_key;
    String skip_indices;
    String projections;
    String constraints;
    String ttl_table;
    UInt64 index_granularity_bytes;

    ReplicatedMergeTreeTableMetadata() = default;
    explicit ReplicatedMergeTreeTableMetadata(const MergeTreeData & data, const StorageMetadataPtr & metadata_snapshot);

    void read(ReadBuffer & in);
    static ReplicatedMergeTreeTableMetadata parse(const String & s);

    void write(WriteBuffer & out) const;
    String toString() const;

    struct Diff
    {
        bool sorting_key_changed = false;
        String new_sorting_key;

        bool sampling_expression_changed = false;
        String new_sampling_expression;

        bool skip_indices_changed = false;
        String new_skip_indices;

        bool constraints_changed = false;
        String new_constraints;

        bool projections_changed = false;
        String new_projections;

        bool ttl_table_changed = false;
        String new_ttl_table;

        bool empty() const
        {
            return !sorting_key_changed && !sampling_expression_changed && !skip_indices_changed && !projections_changed
                && !ttl_table_changed && !constraints_changed;
        }

        StorageInMemoryMetadata getNewMetadata(const ColumnsDescription & new_columns, ContextPtr context, const StorageInMemoryMetadata & old_metadata) const;
    };

    void checkEquals(const ReplicatedMergeTreeTableMetadata & from_zk, const ColumnsDescription & columns, ContextPtr context) const;

    Diff checkAndFindDiff(const ReplicatedMergeTreeTableMetadata & from_zk, const ColumnsDescription & columns, ContextPtr context) const;

private:

    void checkImmutableFieldsEquals(const ReplicatedMergeTreeTableMetadata & from_zk, const ColumnsDescription & columns, ContextPtr context) const;

    bool index_granularity_bytes_found_in_zk = false;
};


}
