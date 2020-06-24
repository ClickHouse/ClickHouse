#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <IO/HashingWriteBuffer.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Common/hex.h>
#include <Core/Block.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const DiskPtr & disk, const String & path)
{
    return disk->readFile(path, std::min(size_t(DBMS_DEFAULT_BUFFER_SIZE), disk->getFileSize(path)));
}

String MergeTreePartition::getID(const MergeTreeData & storage) const
{
    return getID(storage.getInMemoryMetadataPtr()->getPartitionKey().sample_block);
}

/// NOTE: This ID is used to create part names which are then persisted in ZK and as directory names on the file system.
/// So if you want to change this method, be sure to guarantee compatibility with existing table data.
String MergeTreePartition::getID(const Block & partition_key_sample) const
{
    if (value.size() != partition_key_sample.columns())
        throw Exception("Invalid partition key size: " + toString(value.size()), ErrorCodes::LOGICAL_ERROR);

    if (value.empty())
        return "all"; /// It is tempting to use an empty string here. But that would break directory structure in ZK.

    /// In case all partition fields are represented by integral types, try to produce a human-readable ID.
    /// Otherwise use a hex-encoded hash.
    bool are_all_integral = true;
    for (const Field & field : value)
    {
        if (field.getType() != Field::Types::UInt64 && field.getType() != Field::Types::Int64)
        {
            are_all_integral = false;
            break;
        }
    }

    String result;

    if (are_all_integral)
    {
        FieldVisitorToString to_string_visitor;
        for (size_t i = 0; i < value.size(); ++i)
        {
            if (i > 0)
                result += '-';

            if (typeid_cast<const DataTypeDate *>(partition_key_sample.getByPosition(i).type.get()))
                result += toString(DateLUT::instance().toNumYYYYMMDD(DayNum(value[i].safeGet<UInt64>())));
            else
                result += applyVisitor(to_string_visitor, value[i]);

            /// It is tempting to output DateTime as YYYYMMDDhhmmss, but that would make partition ID
            /// timezone-dependent.
        }

        return result;
    }

    SipHash hash;
    FieldVisitorHash hashing_visitor(hash);
    for (const Field & field : value)
        applyVisitor(hashing_visitor, field);

    char hash_data[16];
    hash.get128(hash_data);
    result.resize(32);
    for (size_t i = 0; i < 16; ++i)
        writeHexByteLowercase(hash_data[i], &result[2 * i]);

    return result;
}

void MergeTreePartition::serializeText(const MergeTreeData & storage, WriteBuffer & out, const FormatSettings & format_settings) const
{
    const auto & partition_key_sample = storage.getInMemoryMetadataPtr()->getPartitionKey().sample_block;
    size_t key_size = partition_key_sample.columns();

    if (key_size == 0)
    {
        writeCString("tuple()", out);
    }
    else if (key_size == 1)
    {
        const DataTypePtr & type = partition_key_sample.getByPosition(0).type;
        auto column = type->createColumn();
        column->insert(value[0]);
        type->serializeAsText(*column, 0, out, format_settings);
    }
    else
    {
        DataTypes types;
        Columns columns;
        for (size_t i = 0; i < key_size; ++i)
        {
            const auto & type = partition_key_sample.getByPosition(i).type;
            types.push_back(type);
            auto column = type->createColumn();
            column->insert(value[i]);
            columns.push_back(std::move(column));
        }

        DataTypeTuple tuple_type(types);
        auto tuple_column = ColumnTuple::create(columns);
        tuple_type.serializeText(*tuple_column, 0, out, format_settings);
    }
}

void MergeTreePartition::load(const MergeTreeData & storage, const DiskPtr & disk, const String & part_path)
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (!metadata_snapshot->hasPartitionKey())
        return;

    const auto & partition_key_sample = metadata_snapshot->getPartitionKey().sample_block;
    auto partition_file_path = part_path + "partition.dat";
    auto file = openForReading(disk, partition_file_path);
    value.resize(partition_key_sample.columns());
    for (size_t i = 0; i < partition_key_sample.columns(); ++i)
        partition_key_sample.getByPosition(i).type->deserializeBinary(value[i], *file);
}

void MergeTreePartition::store(const MergeTreeData & storage, const DiskPtr & disk, const String & part_path, MergeTreeDataPartChecksums & checksums) const
{
    store(storage.getInMemoryMetadataPtr()->getPartitionKey().sample_block, disk, part_path, checksums);
}

void MergeTreePartition::store(const Block & partition_key_sample, const DiskPtr & disk, const String & part_path, MergeTreeDataPartChecksums & checksums) const
{
    if (!partition_key_sample)
        return;

    auto out = disk->writeFile(part_path + "partition.dat");
    HashingWriteBuffer out_hashing(*out);
    for (size_t i = 0; i < value.size(); ++i)
        partition_key_sample.getByPosition(i).type->serializeBinary(value[i], out_hashing);
    out_hashing.next();
    checksums.files["partition.dat"].file_size = out_hashing.count();
    checksums.files["partition.dat"].file_hash = out_hashing.getHash();
}

}
