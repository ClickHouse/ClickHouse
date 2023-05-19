#pragma once

#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/IPartMetadataManager.h>
#include <IO/WriteBufferFromFileBase.h>

namespace DB
{

class MergeTreeData;
class IDataPartStorage;
struct MergeTreeDataPartChecksums;

/// Index that for each part stores min and max values of a set of columns. This allows quickly excluding
/// parts based on conditions on these columns imposed by a query.
/// Currently this index is built using only columns required by partition expression, but in principle it
/// can be built using any set of columns.
struct MinMaxIndex
{
    /// A direct product of ranges for each key column. See Storages/MergeTree/KeyCondition.cpp for details.
    std::vector<Range> hyperrectangle;
    bool initialized = false;

public:
    MinMaxIndex() = default;

    /// For month-based partitioning.
    MinMaxIndex(DayNum min_date, DayNum max_date)
        : hyperrectangle(1, Range(min_date, true, max_date, true))
        , initialized(true)
    {
    }

    void loadFromOldStyleFiles(const MergeTreeData & data, const PartMetadataManagerPtr & manager);

    using WrittenFiles = std::vector<std::unique_ptr<WriteBufferFromFileBase>>;

    [[nodiscard]] WrittenFiles storeToOldStyleFiles(const MergeTreeData & data, IDataPartStorage & part_storage, MergeTreeDataPartChecksums & checksums) const;
    [[nodiscard]] WrittenFiles storeToOldStyleFiles(const Names & column_names, const DataTypes & data_types, IDataPartStorage & part_storage, MergeTreeDataPartChecksums & checksums) const;

    void serialize(WriteBuffer & out);
    void deserialize(ReadBuffer & in);

    void update(const Block & block, const Names & column_names);
    void merge(const MinMaxIndex & other);
    static void appendFiles(const MergeTreeData & data, Strings & files);
};

using MinMaxIndexPtr = std::shared_ptr<MinMaxIndex>;

}
