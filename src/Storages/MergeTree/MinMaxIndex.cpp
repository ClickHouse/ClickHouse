#include "MinMaxIndex.h"

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Common/escapeForFileName.h>
#include <IO/HashingWriteBuffer.h>
#include <Common/FieldVisitorsAccurateComparison.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void MinMaxIndex::loadFromOldStyleFiles(const MergeTreeData & data, const PartMetadataManagerPtr & manager)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);
    size_t minmax_idx_size = minmax_column_types.size();

    hyperrectangle.reserve(minmax_idx_size);
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = "minmax_" + escapeForFileName(minmax_column_names[i]) + ".idx";
        auto file = manager->read(file_name);
        auto serialization = minmax_column_types[i]->getDefaultSerialization();

        Field min_val;
        serialization->deserializeBinary(min_val, *file, {});
        Field max_val;
        serialization->deserializeBinary(max_val, *file, {});

        // NULL_LAST
        if (min_val.isNull())
            min_val = POSITIVE_INFINITY;
        if (max_val.isNull())
            max_val = POSITIVE_INFINITY;

        hyperrectangle.emplace_back(min_val, true, max_val, true);
    }
    initialized = true;
}

MinMaxIndex::WrittenFiles MinMaxIndex::storeToOldStyleFiles(
    const MergeTreeData & data, IDataPartStorage & part_storage, MergeTreeDataPartChecksums & out_checksums) const
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();

    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    auto minmax_column_types = data.getMinMaxColumnsTypes(partition_key);

    return storeToOldStyleFiles(minmax_column_names, minmax_column_types, part_storage, out_checksums);
}

MinMaxIndex::WrittenFiles MinMaxIndex::storeToOldStyleFiles(
    const Names & column_names,
    const DataTypes & data_types,
    IDataPartStorage & part_storage,
    MergeTreeDataPartChecksums & out_checksums) const
{
    if (!initialized)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Attempt to store uninitialized MinMax index for part {}. This is a bug",
            part_storage.getFullPath());

    WrittenFiles written_files;

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        String file_name = "minmax_" + escapeForFileName(column_names[i]) + ".idx";
        auto serialization = data_types.at(i)->getDefaultSerialization();

        auto out = part_storage.writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, {});
        HashingWriteBuffer out_hashing(*out);
        serialization->serializeBinary(hyperrectangle[i].left, out_hashing, {});
        serialization->serializeBinary(hyperrectangle[i].right, out_hashing, {});
        out_hashing.next();
        out_checksums.files[file_name].file_size = out_hashing.count();
        out_checksums.files[file_name].file_hash = out_hashing.getHash();
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    }

    return written_files;
}

void MinMaxIndex::update(const Block & block, const Names & column_names)
{
    if (!initialized)
        hyperrectangle.reserve(column_names.size());

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        FieldRef min_value;
        FieldRef max_value;
        const ColumnWithTypeAndName & column = block.getByName(column_names[i]);
        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.column.get()))
            column_nullable->getExtremesNullLast(min_value, max_value);
        else
            column.column->getExtremes(min_value, max_value);

        if (!initialized)
            hyperrectangle.emplace_back(min_value, true, max_value, true);
        else
        {
            hyperrectangle[i].left
                = applyVisitor(FieldVisitorAccurateLess(), hyperrectangle[i].left, min_value) ? hyperrectangle[i].left : min_value;
            hyperrectangle[i].right
                = applyVisitor(FieldVisitorAccurateLess(), hyperrectangle[i].right, max_value) ? max_value : hyperrectangle[i].right;
        }
    }

    initialized = true;
}

void MinMaxIndex::merge(const MinMaxIndex & other)
{
    if (!other.initialized)
        return;

    if (!initialized)
    {
        hyperrectangle = other.hyperrectangle;
        initialized = true;
    }
    else
    {
        for (size_t i = 0; i < hyperrectangle.size(); ++i)
        {
            hyperrectangle[i].left = std::min(hyperrectangle[i].left, other.hyperrectangle[i].left);
            hyperrectangle[i].right = std::max(hyperrectangle[i].right, other.hyperrectangle[i].right);
        }
    }
}

void MinMaxIndex::appendFiles(const MergeTreeData & data, Strings & files)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto & partition_key = metadata_snapshot->getPartitionKey();
    auto minmax_column_names = data.getMinMaxColumnsNames(partition_key);
    size_t minmax_idx_size = minmax_column_names.size();
    for (size_t i = 0; i < minmax_idx_size; ++i)
    {
        String file_name = "minmax_" + escapeForFileName(minmax_column_names[i]) + ".idx";
        files.push_back(file_name);
    }
}

void MinMaxIndex::serialize(WriteBuffer & /*out*/)
{
    // TODO
}

void MinMaxIndex::deserialize(ReadBuffer & /*in*/)
{
    *this = {};

    // TODO
}

}
