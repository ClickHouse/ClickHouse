#pragma once

#include <Core/Types.h>
#include <Core/Row.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class MergeTreeData;
struct MergeTreeDataPartChecksums;

/// This class represents a partition value of a single part and encapsulates its loading/storing logic.
struct MergeTreePartition
{
    Row value;

public:
    MergeTreePartition() = default;

    explicit MergeTreePartition(Row value_) : value(std::move(value_)) {}

    /// For month-based partitioning.
    explicit MergeTreePartition(UInt32 yyyymm) : value(1, static_cast<UInt64>(yyyymm)) {}

    String getID(const MergeTreeData & storage) const;

    void serializeTextQuoted(const MergeTreeData & storage, WriteBuffer & out) const;

    void load(const MergeTreeData & storage, const String & part_path);
    void store(const MergeTreeData & storage, const String & part_path, MergeTreeDataPartChecksums & checksums) const;

    void assign(const MergeTreePartition & other) { value.assign(other.value); }
};


}
