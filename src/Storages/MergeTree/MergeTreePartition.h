#pragma once

#include <base/types.h>
#include <Disks/IDisk.h>
#include <IO/WriteBuffer.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IPartMetadataManager.h>
#include <Core/Field.h>

namespace DB
{

class Block;
class MergeTreeData;
struct FormatSettings;
struct MergeTreeDataPartChecksums;
struct StorageInMemoryMetadata;
class IDataPartStorageBuilder;

using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
using DataPartStorageBuilderPtr = std::shared_ptr<IDataPartStorageBuilder>;

/// This class represents a partition value of a single part and encapsulates its loading/storing logic.
struct MergeTreePartition
{
    Row value;

public:
    MergeTreePartition() = default;

    explicit MergeTreePartition(Row value_) : value(std::move(value_)) {}

    /// For month-based partitioning.
    explicit MergeTreePartition(UInt32 yyyymm) : value(1, yyyymm) {}

    String getID(const MergeTreeData & storage) const;
    String getID(const Block & partition_key_sample) const;

    static std::optional<Row> tryParseValueFromID(const String & partition_id, const Block & partition_key_sample);

    void serializeText(const MergeTreeData & storage, WriteBuffer & out, const FormatSettings & format_settings) const;

    void load(const MergeTreeData & storage, const PartMetadataManagerPtr & manager);

    /// Store functions return write buffer with written but not finalized data.
    /// User must call finish() for returned object.
    [[nodiscard]] std::unique_ptr<WriteBufferFromFileBase> store(const MergeTreeData & storage, const DataPartStorageBuilderPtr & data_part_storage_builder, MergeTreeDataPartChecksums & checksums) const;
    [[nodiscard]] std::unique_ptr<WriteBufferFromFileBase> store(const Block & partition_key_sample, const DataPartStorageBuilderPtr & data_part_storage_builder, MergeTreeDataPartChecksums & checksums, const WriteSettings & settings) const;

    void assign(const MergeTreePartition & other) { value = other.value; }

    void create(const StorageMetadataPtr & metadata_snapshot, Block block, size_t row, ContextPtr context);

    static void appendFiles(const MergeTreeData & storage, Strings & files);

    /// Adjust partition key and execute its expression on block. Return sample block according to used expression.
    static NamesAndTypesList executePartitionByExpression(const StorageMetadataPtr & metadata_snapshot, Block & block, ContextPtr context);

    /// Make a modified partition key with substitution from modulo to moduloLegacy. Used in paritionPruner.
    static KeyDescription adjustPartitionKey(const StorageMetadataPtr & metadata_snapshot, ContextPtr context);
};

}
