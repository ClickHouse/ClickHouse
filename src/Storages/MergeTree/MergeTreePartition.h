#pragma once

#include <base/types.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/KeyDescription.h>
#include <Core/Block.h>
#include <Core/Field.h>

namespace DB
{

class Block;
class MergeTreeData;
struct FormatSettings;
struct MergeTreeDataPartChecksums;
struct StorageInMemoryMetadata;
class IDataPartStorage;
class IMergeTreeDataPart;
struct WriteSettings;

using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

/// This class represents a partition value of a single part and encapsulates its loading/storing logic.
struct MergeTreePartition
{
    Row value;

    MergeTreePartition() = default;

    explicit MergeTreePartition(Row value_);

    /// For month-based partitioning.
    explicit MergeTreePartition(UInt32 yyyymm) : value(1, yyyymm) {}

    String getID(const MergeTreeData & storage) const;
    String getID(const Block & partition_key_sample) const;

    static std::optional<Row> tryParseValueFromID(const String & partition_id, const Block & partition_key_sample);

    /// `partition_key_sample` must be the adjusted partition key (see `adjustPartitionKey`): it is the key
    /// whose types produced the values in `value`, and its signedness can differ from the declared key's.
    void serializeText(const Block & partition_key_sample, WriteBuffer & out, const FormatSettings & format_settings) const;
    String serializeToString(const Block & partition_key_sample) const;
    String serializeToString(const IMergeTreeDataPart & part) const;

    void load(const IMergeTreeDataPart & part);

    /// Store functions return write buffer with written but not finalized data.
    /// User must call finish() for returned object.
    [[nodiscard]] std::unique_ptr<WriteBufferFromFileBase> store(
        StorageMetadataPtr metadata_snapshot,
        ContextPtr storage_context,
        IDataPartStorage & data_part_storage,
        MergeTreeDataPartChecksums & checksums) const;

    [[nodiscard]] std::unique_ptr<WriteBufferFromFileBase> store(
        const Block & partition_key_sample,
        IDataPartStorage & data_part_storage,
        MergeTreeDataPartChecksums & checksums,
        const WriteSettings & settings) const;

    void assign(const MergeTreePartition & other) { value = other.value; }

    void create(const StorageMetadataPtr & metadata_snapshot, Block block, size_t row, ContextPtr context);

    /// Adjust partition key and execute its expression on block. Return sample block according to used expression.
    static NamesAndTypesList executePartitionByExpression(const StorageMetadataPtr & metadata_snapshot, Block & block, ContextPtr context);

    /// Make a modified partition key with substitution from modulo to moduloLegacy. Used in paritionPruner.
    static KeyDescription adjustPartitionKey(const StorageMetadataPtr & metadata_snapshot, ContextPtr context);
};

/// Resolves the partition key that a part's stored partition value must be rendered with, reusing the
/// table's adjusted key across the parts it is asked about. The returned reference is valid until the
/// next `get`. A patch part carries a generated `__patchPartitionID` key, already its own adjusted key.
class PartitionKeySamples
{
public:
    const Block & get(const IMergeTreeDataPart & part);

private:
    const MergeTreeData * table = nullptr;
    Block table_sample;
    Block patch_sample;
};

}
