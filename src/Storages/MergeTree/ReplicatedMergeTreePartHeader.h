#pragma once

#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <common/types.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/Operators.h>
#include <array>
#include <optional>


namespace DB
{

class NamesAndTypesList;

/// This class provides a compact representation of part metadata (available columns and checksums)
/// that is intended to be stored in the part znode in ZooKeeper.
/// It can also be initialized from the legacy format (from the contents of separate <part>/columns
/// and <part>/checksums znodes).
class ReplicatedMergeTreePartHeader
{
public:
    ReplicatedMergeTreePartHeader() = default;

    static ReplicatedMergeTreePartHeader fromColumnsAndChecksumsZNodes(
        const String & columns_znode, const String & checksums_znode);

    static ReplicatedMergeTreePartHeader fromColumnsAndChecksums(
        const NamesAndTypesList & columns, const MergeTreeDataPartChecksums & full_checksums);

    static ReplicatedMergeTreePartHeader fromColumnsChecksumsBlockID(
        const NamesAndTypesList & columns, const MergeTreeDataPartChecksums & full_checksums, const String & block_id_);

    void read(ReadBuffer & in);
    static ReplicatedMergeTreePartHeader fromString(const String & str);

    void write(WriteBuffer & out) const;
    String toString() const;

    const std::array<char, 16> & getColumnsHash() const { return columns_hash; }
    const MinimalisticDataPartChecksums & getChecksums() const { return checksums; }
    const std::optional<String> & getBlockID() const { return block_id; }

private:
    ReplicatedMergeTreePartHeader(std::array<char, 16> columns_hash_, MinimalisticDataPartChecksums checksums_,
        std::optional<String> block_id_ = std::nullopt)
        : columns_hash(std::move(columns_hash_)), checksums(std::move(checksums_)), block_id(std::move(block_id_))
    {
    }

    std::array<char, 16> columns_hash;
    MinimalisticDataPartChecksums checksums;
    std::optional<String> block_id;
};

}
