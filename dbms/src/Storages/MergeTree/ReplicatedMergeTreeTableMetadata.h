#pragma once

#include <Parsers/IAST.h>

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
    const MergeTreeData & data;

    bool sorting_and_primary_keys_independent = false;
    String sorting_key_str;

    explicit ReplicatedMergeTreeTableMetadata(const MergeTreeData & data_);

    void write(WriteBuffer & out) const;
    String toString() const;

    void read(ReadBuffer & in);
    static ReplicatedMergeTreeTableMetadata parse(const MergeTreeData & data_, const String & s);

    void check(ReadBuffer & in) const;
    void check(const String & s) const;

    /// TODO: checkAndFindDiff(other);
};

}
