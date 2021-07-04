#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <IO/HashingWriteBuffer.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorHash.h>
#include <Common/typeid_cast.h>
#include <Common/hex.h>
#include <Core/Block.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// This is a special visitor which is used to get partition ID.
    /// Calculate hash for UUID the same way as for UInt128.
    /// It worked this way until 21.5, and we cannot change it,
    /// or partition ID will be different in case UUID is used in partition key.
    /// (It is not recommended to use UUID as partition key).
    /// NOTE: The code is intentionally copy-pasted,
    /// so when FieldVisitorHash is changed, LegacyFieldVisitorHash will not change.
    class LegacyFieldVisitorHash : public StaticVisitor<>
    {
    private:
        SipHash & hash;
    public:
        explicit LegacyFieldVisitorHash(SipHash & hash_) : hash(hash_) {}

        void operator() (const Null &) const
        {
            UInt8 type = Field::Types::Null;
            hash.update(type);
        }
        void operator() (const UInt64 & x) const
        {
            UInt8 type = Field::Types::UInt64;
            hash.update(type);
            hash.update(x);
        }
        void operator() (const UInt128 & x) const
        {
            UInt8 type = Field::Types::UInt128;
            hash.update(type);
            hash.update(x);
        }
        void operator() (const UInt256 & x) const
        {
            UInt8 type = Field::Types::UInt256;
            hash.update(type);
            hash.update(x);
        }
        void operator() (const Int64 & x) const
        {
            UInt8 type = Field::Types::Int64;
            hash.update(type);
            hash.update(x);
        }
        void operator() (const Int128 & x) const
        {
            UInt8 type = Field::Types::Int128;
            hash.update(type);
            hash.update(x);
        }
        void operator() (const Int256 & x) const
        {
            UInt8 type = Field::Types::Int256;
            hash.update(type);
            hash.update(x);
        }
        void operator() (const UUID & x) const
        {
            operator()(x.toUnderType());
        }
        void operator() (const Float64 & x) const
        {
            UInt8 type = Field::Types::Float64;
            hash.update(type);
            hash.update(x);
        }
        void operator() (const String & x) const
        {
            UInt8 type = Field::Types::String;
            hash.update(type);
            hash.update(x.size());
            hash.update(x.data(), x.size());
        }
        void operator() (const Array & x) const
        {
            UInt8 type = Field::Types::Array;
            hash.update(type);
            hash.update(x.size());

            for (const auto & elem : x)
                applyVisitor(*this, elem);
        }
        void operator() (const Tuple & x) const
        {
            UInt8 type = Field::Types::Tuple;
            hash.update(type);
            hash.update(x.size());

            for (const auto & elem : x)
                applyVisitor(*this, elem);
        }
        void operator() (const Map & x) const
        {
            UInt8 type = Field::Types::Map;
            hash.update(type);
            hash.update(x.size());

            for (const auto & elem : x)
                applyVisitor(*this, elem);
        }
        void operator() (const DecimalField<Decimal32> & x) const
        {
            UInt8 type = Field::Types::Decimal32;
            hash.update(type);
            hash.update(x.getValue().value);
        }
        void operator() (const DecimalField<Decimal64> & x) const
        {
            UInt8 type = Field::Types::Decimal64;
            hash.update(type);
            hash.update(x.getValue().value);
        }
        void operator() (const DecimalField<Decimal128> & x) const
        {
            UInt8 type = Field::Types::Decimal128;
            hash.update(type);
            hash.update(x.getValue().value);
        }
        void operator() (const DecimalField<Decimal256> & x) const
        {
            UInt8 type = Field::Types::Decimal256;
            hash.update(type);
            hash.update(x.getValue().value);
        }
        void operator() (const AggregateFunctionStateData & x) const
        {
            UInt8 type = Field::Types::AggregateFunctionState;
            hash.update(type);
            hash.update(x.name.size());
            hash.update(x.name.data(), x.name.size());
            hash.update(x.data.size());
            hash.update(x.data.data(), x.data.size());
        }
    };
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
    LegacyFieldVisitorHash hashing_visitor(hash);
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
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    const auto & partition_key_sample = metadata_snapshot->getPartitionKey().sample_block;
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
        type->getDefaultSerialization()->serializeText(*column, 0, out, format_settings);
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

        auto tuple_serialization = DataTypeTuple(types).getDefaultSerialization();
        auto tuple_column = ColumnTuple::create(columns);
        tuple_serialization->serializeText(*tuple_column, 0, out, format_settings);
    }
}

void MergeTreePartition::load(const MergeTreeData & storage, const DiskPtr & disk, const String & part_path)
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    if (!metadata_snapshot->hasPartitionKey())
        return;

    const auto & partition_key_sample = adjustPartitionKey(metadata_snapshot, storage.getContext()).sample_block;
    auto partition_file_path = part_path + "partition.dat";
    auto file = openForReading(disk, partition_file_path);
    value.resize(partition_key_sample.columns());
    for (size_t i = 0; i < partition_key_sample.columns(); ++i)
        partition_key_sample.getByPosition(i).type->getDefaultSerialization()->deserializeBinary(value[i], *file);
}

void MergeTreePartition::store(const MergeTreeData & storage, const DiskPtr & disk, const String & part_path, MergeTreeDataPartChecksums & checksums) const
{
    auto metadata_snapshot = storage.getInMemoryMetadataPtr();
    const auto & partition_key_sample = adjustPartitionKey(metadata_snapshot, storage.getContext()).sample_block;
    store(partition_key_sample, disk, part_path, checksums);
}

void MergeTreePartition::store(const Block & partition_key_sample, const DiskPtr & disk, const String & part_path, MergeTreeDataPartChecksums & checksums) const
{
    if (!partition_key_sample)
        return;

    auto out = disk->writeFile(part_path + "partition.dat");
    HashingWriteBuffer out_hashing(*out);
    for (size_t i = 0; i < value.size(); ++i)
        partition_key_sample.getByPosition(i).type->getDefaultSerialization()->serializeBinary(value[i], out_hashing);

    out_hashing.next();
    checksums.files["partition.dat"].file_size = out_hashing.count();
    checksums.files["partition.dat"].file_hash = out_hashing.getHash();
    out->finalize();
}

void MergeTreePartition::create(const StorageMetadataPtr & metadata_snapshot, Block block, size_t row, ContextPtr context)
{
    if (!metadata_snapshot->hasPartitionKey())
        return;

    auto partition_key_names_and_types = executePartitionByExpression(metadata_snapshot, block, context);
    value.resize(partition_key_names_and_types.size());

    /// Executing partition_by expression adds new columns to passed block according to partition functions.
    /// The block is passed by reference and is used afterwards. `moduloLegacy` needs to be substituted back
    /// with just `modulo`, because it was a temporary substitution.
    static constexpr auto modulo_legacy_function_name = "moduloLegacy";

    size_t i = 0;
    for (const auto & element : partition_key_names_and_types)
    {
        auto & partition_column = block.getByName(element.name);

        if (element.name.starts_with(modulo_legacy_function_name))
            partition_column.name = "modulo" + partition_column.name.substr(std::strlen(modulo_legacy_function_name));

        partition_column.column->get(row, value[i++]);
    }
}

NamesAndTypesList MergeTreePartition::executePartitionByExpression(const StorageMetadataPtr & metadata_snapshot, Block & block, ContextPtr context)
{
    auto adjusted_partition_key = adjustPartitionKey(metadata_snapshot, context);
    adjusted_partition_key.expression->execute(block);
    return adjusted_partition_key.sample_block.getNamesAndTypesList();
}

KeyDescription MergeTreePartition::adjustPartitionKey(const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    const auto & partition_key = metadata_snapshot->getPartitionKey();
    if (!partition_key.definition_ast)
        return partition_key;

    ASTPtr ast_copy = partition_key.definition_ast->clone();

    /// Implementation of modulo function was changed from 8bit result type to 16bit. For backward compatibility partition by expression is always
    /// calculated according to previous version - `moduloLegacy`.
    if (KeyDescription::moduloToModuloLegacyRecursive(ast_copy))
    {
        auto adjusted_partition_key = KeyDescription::getKeyFromAST(ast_copy, metadata_snapshot->columns, context);
        return adjusted_partition_key;
    }

    return partition_key;
}

}
