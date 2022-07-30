#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageGenerateRandom.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Parsers/ASTLiteral.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/NestedUtils.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>

#include <Common/SipHash.h>
#include <Common/randomSeed.h>
#include <base/unaligned.h>

#include <Functions/FunctionFactory.h>

#include <pcg_random.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int TOO_LARGE_STRING_SIZE;
}


namespace
{

void fillBufferWithRandomData(char * __restrict data, size_t size, pcg64 & rng)
{
    char * __restrict end = data + size;
    while (data < end)
    {
        /// The loop can be further optimized.
        UInt64 number = rng();
        unalignedStore<UInt64>(data, number);
        data += sizeof(UInt64); /// We assume that data has at least 7-byte padding (see PaddedPODArray)
    }
}


ColumnPtr fillColumnWithRandomData(
    const DataTypePtr type,
    UInt64 limit,
    UInt64 max_array_length,
    UInt64 max_string_length,
    pcg64 & rng,
    ContextPtr context)
{
    TypeIndex idx = type->getTypeId();

    switch (idx)
    {
        case TypeIndex::String:
        {
            /// Mostly the same as the implementation of randomPrintableASCII function.

            auto column = ColumnString::create();
            ColumnString::Chars & data_to = column->getChars();
            ColumnString::Offsets & offsets_to = column->getOffsets();
            offsets_to.resize(limit);

            IColumn::Offset offset = 0;
            for (size_t row_num = 0; row_num < limit; ++row_num)
            {
                size_t length = rng() % (max_string_length + 1);    /// Slow

                IColumn::Offset next_offset = offset + length + 1;
                data_to.resize(next_offset);
                offsets_to[row_num] = next_offset;

                auto * data_to_ptr = data_to.data();    /// avoid assert on array indexing after end
                for (size_t pos = offset, end = offset + length; pos < end; pos += 4)    /// We have padding in column buffers that we can overwrite.
                {
                    UInt64 rand = rng();

                    UInt16 rand1 = rand;
                    UInt16 rand2 = rand >> 16;
                    UInt16 rand3 = rand >> 32;
                    UInt16 rand4 = rand >> 48;

                    /// Printable characters are from range [32; 126].
                    /// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/

                    data_to_ptr[pos + 0] = 32 + ((rand1 * 95) >> 16);
                    data_to_ptr[pos + 1] = 32 + ((rand2 * 95) >> 16);
                    data_to_ptr[pos + 2] = 32 + ((rand3 * 95) >> 16);
                    data_to_ptr[pos + 3] = 32 + ((rand4 * 95) >> 16);

                    /// NOTE gcc failed to vectorize this code (aliasing of char?)
                    /// TODO Implement SIMD optimizations from Danila Kutenin.
                }

                data_to[offset + length] = 0;

                offset = next_offset;
            }

            return column;
        }

        case TypeIndex::Enum8:
        {
            auto column = ColumnVector<Int8>::create();
            auto values = typeid_cast<const DataTypeEnum<Int8> *>(type.get())->getValues();
            auto & data = column->getData();
            data.resize(limit);

            UInt8 size = values.size();
            UInt8 off;
            for (UInt64 i = 0; i < limit; ++i)
            {
                off = static_cast<UInt8>(rng()) % size;
                data[i] = values[off].second;
            }

            return column;
        }

        case TypeIndex::Enum16:
        {
            auto column = ColumnVector<Int16>::create();
            auto values = typeid_cast<const DataTypeEnum<Int16> *>(type.get())->getValues();
            auto & data = column->getData();
            data.resize(limit);

            UInt16 size = values.size();
            UInt8 off;
            for (UInt64 i = 0; i < limit; ++i)
            {
                off = static_cast<UInt16>(rng()) % size;
                data[i] = values[off].second;
            }

            return column;
        }

        case TypeIndex::Array:
        {
            auto nested_type = typeid_cast<const DataTypeArray *>(type.get())->getNestedType();

            auto offsets_column = ColumnVector<ColumnArray::Offset>::create();
            auto & offsets = offsets_column->getData();

            UInt64 offset = 0;
            offsets.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                offset += static_cast<UInt64>(rng()) % (max_array_length + 1);
                offsets[i] = offset;
            }

            auto data_column = fillColumnWithRandomData(nested_type, offset, max_array_length, max_string_length, rng, context);

            return ColumnArray::create(data_column, std::move(offsets_column));
        }

        case TypeIndex::Tuple:
        {
            auto elements = typeid_cast<const DataTypeTuple *>(type.get())->getElements();
            const size_t tuple_size = elements.size();
            Columns tuple_columns(tuple_size);

            for (size_t i = 0; i < tuple_size; ++i)
                tuple_columns[i] = fillColumnWithRandomData(elements[i], limit, max_array_length, max_string_length, rng, context);

            return ColumnTuple::create(std::move(tuple_columns));
        }

        case TypeIndex::Nullable:
        {
            auto nested_type = typeid_cast<const DataTypeNullable *>(type.get())->getNestedType();
            auto nested_column = fillColumnWithRandomData(nested_type, limit, max_array_length, max_string_length, rng, context);

            auto null_map_column = ColumnUInt8::create();
            auto & null_map = null_map_column->getData();
            null_map.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
                null_map[i] = rng() % 16 == 0; /// No real motivation for this.

            return ColumnNullable::create(nested_column, std::move(null_map_column));
        }

        case TypeIndex::UInt8:
        {
            auto column = ColumnUInt8::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(UInt8), rng);
            return column;
        }
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::Date:
        {
            auto column = ColumnUInt16::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(UInt16), rng);
            return column;
        }
        case TypeIndex::Date32:
        {
            auto column = ColumnInt32::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Int32), rng);
            return column;
        }
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::DateTime:
        {
            auto column = ColumnUInt32::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(UInt32), rng);
            return column;
        }
        case TypeIndex::UInt64:
        {
            auto column = ColumnUInt64::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(UInt64), rng);
            return column;
        }
        case TypeIndex::UInt128:
        {
            auto column = ColumnUInt128::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(UInt128), rng);
            return column;
        }
        case TypeIndex::UInt256:
        {
            auto column = ColumnUInt256::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(UInt256), rng);
            return column;
        }
        case TypeIndex::UUID:
        {
            auto column = ColumnUUID::create();
            column->getData().resize(limit);
            /// NOTE This is slightly incorrect as random UUIDs should have fixed version 4.
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(UUID), rng);
            return column;
        }
        case TypeIndex::Int8:
        {
            auto column = ColumnInt8::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Int8), rng);
            return column;
        }
        case TypeIndex::Int16:
        {
            auto column = ColumnInt16::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Int16), rng);
            return column;
        }
        case TypeIndex::Int32:
        {
            auto column = ColumnInt32::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Int32), rng);
            return column;
        }
        case TypeIndex::Int64:
        {
            auto column = ColumnInt64::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Int64), rng);
            return column;
        }
        case TypeIndex::Int128:
        {
            auto column = ColumnInt128::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Int128), rng);
            return column;
        }
        case TypeIndex::Int256:
        {
            auto column = ColumnInt256::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Int256), rng);
            return column;
        }
        case TypeIndex::Float32:
        {
            auto column = ColumnFloat32::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Float32), rng);
            return column;
        }
        case TypeIndex::Float64:
        {
            auto column = ColumnFloat64::create();
            column->getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(Float64), rng);
            return column;
        }
        case TypeIndex::Decimal32:
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal32> &>(*column);
            column_concrete.getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column_concrete.getData().data()), limit * sizeof(Decimal32), rng);
            return column;
        }
        case TypeIndex::Decimal64:  /// TODO Decimal may be generated out of range.
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal64> &>(*column);
            column_concrete.getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column_concrete.getData().data()), limit * sizeof(Decimal64), rng);
            return column;
        }
        case TypeIndex::Decimal128:
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal128> &>(*column);
            column_concrete.getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column_concrete.getData().data()), limit * sizeof(Decimal128), rng);
            return column;
        }
        case TypeIndex::Decimal256:
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal256> &>(*column);
            column_concrete.getData().resize(limit);
            fillBufferWithRandomData(reinterpret_cast<char *>(column_concrete.getData().data()), limit * sizeof(Decimal256), rng);
            return column;
        }
        case TypeIndex::FixedString:
        {
            size_t n = typeid_cast<const DataTypeFixedString &>(*type).getN();
            auto column = ColumnFixedString::create(n);
            column->getChars().resize(limit * n);
            fillBufferWithRandomData(reinterpret_cast<char *>(column->getChars().data()), limit * n, rng);
            return column;
        }
        case TypeIndex::DateTime64:
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<DateTime64> &>(*column);
            column_concrete.getData().resize(limit);

            UInt64 range = (1ULL << 32) * intExp10(typeid_cast<const DataTypeDateTime64 &>(*type).getScale());

            for (size_t i = 0; i < limit; ++i)
                column_concrete.getData()[i] = rng() % range;   /// Slow

            return column;
        }

        default:
            throw Exception("The 'GenerateRandom' is not implemented for type " + type->getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
}


class GenerateSource : public ISource
{
public:
    GenerateSource(UInt64 block_size_, UInt64 max_array_length_, UInt64 max_string_length_, UInt64 random_seed_, Block block_header_, ContextPtr context_)
        : ISource(Nested::flatten(prepareBlockToFill(block_header_)))
        , block_size(block_size_), max_array_length(max_array_length_), max_string_length(max_string_length_)
        , block_to_fill(std::move(block_header_)), rng(random_seed_), context(context_) {}

    String getName() const override { return "GenerateRandom"; }

protected:
    Chunk generate() override
    {
        Columns columns;
        columns.reserve(block_to_fill.columns());

        for (const auto & elem : block_to_fill)
            columns.emplace_back(fillColumnWithRandomData(elem.type, block_size, max_array_length, max_string_length, rng, context));

        columns = Nested::flatten(block_to_fill.cloneWithColumns(columns)).getColumns();
        return {std::move(columns), block_size};
    }

private:
    UInt64 block_size;
    UInt64 max_array_length;
    UInt64 max_string_length;
    Block block_to_fill;

    pcg64 rng;

    ContextPtr context;

    static Block & prepareBlockToFill(Block & block)
    {
        /// To support Nested types, we will collect them to single Array of Tuple.
        auto names_and_types = Nested::collect(block.getNamesAndTypesList());
        block.clear();

        for (auto & column : names_and_types)
            block.insert(ColumnWithTypeAndName(column.type, column.name));

        return block;
    }
};

}


StorageGenerateRandom::StorageGenerateRandom(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    UInt64 max_array_length_,
    UInt64 max_string_length_,
    std::optional<UInt64> random_seed_)
    : IStorage(table_id_), max_array_length(max_array_length_), max_string_length(max_string_length_)
{
    static constexpr size_t MAX_ARRAY_SIZE = 1 << 30;
    static constexpr size_t MAX_STRING_SIZE = 1 << 30;

    if (max_array_length > MAX_ARRAY_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in GenerateRandom: {}, maximum: {}",
                        max_array_length, MAX_ARRAY_SIZE);
    if (max_string_length > MAX_STRING_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size in GenerateRandom: {}, maximum: {}",
                        max_string_length, MAX_STRING_SIZE);

    random_seed = random_seed_ ? sipHash64(*random_seed_) : randomSeed();
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


void registerStorageGenerateRandom(StorageFactory & factory)
{
    factory.registerStorage("GenerateRandom", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() > 3)
            throw Exception("Storage GenerateRandom requires at most three arguments: "
                "random_seed, max_string_length, max_array_length.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        std::optional<UInt64> random_seed;
        UInt64 max_string_length = 10;
        UInt64 max_array_length = 10;

        if (!engine_args.empty())
        {
            const auto & ast_literal = engine_args[0]->as<const ASTLiteral &>();
            if (!ast_literal.value.isNull())
                random_seed = checkAndGetLiteralArgument<UInt64>(ast_literal, "random_seed");
        }

        if (engine_args.size() >= 2)
            max_string_length = checkAndGetLiteralArgument<UInt64>(engine_args[1], "max_string_length");

        if (engine_args.size() == 3)
            max_array_length = checkAndGetLiteralArgument<UInt64>(engine_args[2], "max_array_length");

        return std::make_shared<StorageGenerateRandom>(args.table_id, args.columns, args.comment, max_array_length, max_string_length, random_seed);
    });
}

Pipe StorageGenerateRandom::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    storage_snapshot->check(column_names);

    Pipes pipes;
    pipes.reserve(num_streams);

    const ColumnsDescription & our_columns = storage_snapshot->metadata->getColumns();
    Block block_header;
    for (const auto & name : column_names)
    {
        const auto & name_type = our_columns.get(name);
        MutableColumnPtr column = name_type.type->createColumn();
        block_header.insert({std::move(column), name_type.type, name_type.name});
    }

    /// Will create more seed values for each source from initial seed.
    pcg64 generate(random_seed);

    for (UInt64 i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<GenerateSource>(max_block_size, max_array_length, max_string_length, generate(), block_header, context));

    return Pipe::unitePipes(std::move(pipes));
}

}
