#pragma once

#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <base/types.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/Operators.h>
#include <array>
#include <optional>


namespace DB
{

class NamesAndTypesList;
class IMergeTreeDataPart;

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

    void read(ReadBuffer & in);
    static ReplicatedMergeTreePartHeader fromString(const String & str);

    void write(WriteBuffer & out) const;
    [[nodiscard]] String toString() const;

    [[nodiscard]] const std::array<char, 16> & getColumnsHash() const { return columns_hash; }
    [[nodiscard]] const MinimalisticDataPartChecksums & getChecksums() const { return checksums; }

    /// The columns hash of the `columns.txt` of `part` as stored, if it has one. It differs from the hash of the loaded
    /// columns when loading rewrote a type that an older version stored (see `getStoredDataType`).
    [[nodiscard]] static std::optional<std::array<char, 16>> getStoredColumnsHash(const IMergeTreeDataPart & part);

    /// The columns hash of the text of a `columns.txt`.
    [[nodiscard]] static std::array<char, 16> getColumnsHash(const String & columns_text);

    /// The header of a local part. It has the hash of the `columns.txt` of the part as stored when loading rewrote
    /// a type of it (see `IMergeTreeDataPart::getLegacyColumnsHash`): the part is fetched with that file unchanged,
    /// so a replica of an older version, which reads it as written, has to find the same hash in ZooKeeper.
    static ReplicatedMergeTreePartHeader fromPart(const IMergeTreeDataPart & part);

private:
    ReplicatedMergeTreePartHeader(std::array<char, 16> columns_hash_, MinimalisticDataPartChecksums checksums_)
        : columns_hash(std::move(columns_hash_)), checksums(std::move(checksums_))
    {
    }

    std::array<char, 16> columns_hash{};
    MinimalisticDataPartChecksums checksums;
};

}
