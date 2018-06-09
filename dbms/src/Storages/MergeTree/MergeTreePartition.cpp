#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/HashingWriteBuffer.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeDate.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Common/hex.h>

#include <Poco/File.h>

namespace DB
{

static ReadBufferFromFile openForReading(const String & path)
{
    return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

/// NOTE: This ID is used to create part names which are then persisted in ZK and as directory names on the file system.
/// So if you want to change this method, be sure to guarantee compatibility with existing table data.
String MergeTreePartition::getID(const MergeTreeData & storage) const
{
    if (value.size() != storage.partition_key_sample.columns())
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

            if (typeid_cast<const DataTypeDate *>(storage.partition_key_sample.getByPosition(i).type.get()))
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

void MergeTreePartition::serializeTextQuoted(const MergeTreeData & storage, WriteBuffer & out, const FormatSettings & format_settings) const
{
    size_t key_size = storage.partition_key_sample.columns();

    if (key_size == 0)
    {
        writeCString("tuple()", out);
        return;
    }

    if (key_size > 1)
        writeChar('(', out);

    for (size_t i = 0; i < key_size; ++i)
    {
        if (i > 0)
            writeCString(", ", out);

        const DataTypePtr & type = storage.partition_key_sample.getByPosition(i).type;
        auto column = type->createColumn();
        column->insert(value[i]);
        type->serializeTextQuoted(*column, 0, out, format_settings);
    }

    if (key_size > 1)
        writeChar(')', out);
}

void MergeTreePartition::load(const MergeTreeData & storage, const String & part_path)
{
    if (!storage.partition_expr)
        return;

    ReadBufferFromFile file = openForReading(part_path + "partition.dat");
    value.resize(storage.partition_key_sample.columns());
    for (size_t i = 0; i < storage.partition_key_sample.columns(); ++i)
        storage.partition_key_sample.getByPosition(i).type->deserializeBinary(value[i], file);
}

void MergeTreePartition::store(const MergeTreeData & storage, const String & part_path, MergeTreeDataPartChecksums & checksums) const
{
    if (!storage.partition_expr)
        return;

    WriteBufferFromFile out(part_path + "partition.dat");
    HashingWriteBuffer out_hashing(out);
    for (size_t i = 0; i < value.size(); ++i)
        storage.partition_key_sample.getByPosition(i).type->serializeBinary(value[i], out_hashing);
    out_hashing.next();
    checksums.files["partition.dat"].file_size = out_hashing.count();
    checksums.files["partition.dat"].file_hash = out_hashing.getHash();
}

}
