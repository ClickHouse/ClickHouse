#pragma once

#include <Parsers/IAST.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Core/Types.h>

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
    String date_column;
    String sampling_expression;
    UInt64 index_granularity;
    int merging_params_mode;
    String sign_column;
    String primary_key;
    MergeTreeDataFormatVersion data_format_version;
    String partition_key;
    String sorting_key;

    ReplicatedMergeTreeTableMetadata() = default;
    explicit ReplicatedMergeTreeTableMetadata(const MergeTreeData & data);

    void read(ReadBuffer & in);
    static ReplicatedMergeTreeTableMetadata parse(const String & s);

    void write(WriteBuffer & out) const;
    String toString() const;

    struct Diff
    {
        bool sorting_key_changed = false;
        String new_sorting_key;

        bool empty() const { return !sorting_key_changed; }
    };

    Diff checkAndFindDiff(const ReplicatedMergeTreeTableMetadata & from_zk, bool allow_alter) const;
};

}
