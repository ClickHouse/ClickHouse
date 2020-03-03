#include <Storages/IStorage.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageGenerate.h>
#include <Storages/StorageFactory.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Pipe.h>
#include <Parsers/ASTLiteral.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>

#include <Common/SipHash.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int BAD_TYPE_OF_FIELD;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


void fillColumnWithRandomData(IColumn & column, const DataTypePtr type, UInt64 limit,
                              UInt64 max_array_length, UInt64 max_string_length, pcg32& generator, pcg64_fast& generator64)
{
    TypeIndex idx = type->getTypeId();
    (void) max_string_length;

    switch (idx)
    {
        case TypeIndex::Nothing:
            throw Exception("Random Generator not implemented for type 'Nothing'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::UInt8:
        {
            auto & data = typeid_cast<ColumnVector<UInt8> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<UInt8>(generator());
            }
            break;
        }
        case TypeIndex::UInt16:
        {
            auto & data = typeid_cast<ColumnVector<UInt16> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<UInt16>(generator());
            }
            break;
        }
        case TypeIndex::UInt32:
        {
            auto & data = typeid_cast<ColumnVector<UInt32> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<UInt32>(generator());
            }
            break;
        }
        case TypeIndex::UInt64:
        {
            auto & data = typeid_cast<ColumnVector<UInt64> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                UInt64 a = static_cast<UInt64>(generator64());
                data[i] = static_cast<UInt64>(a);
            }
            break;
        }
        case TypeIndex::UInt128:
            throw Exception("There is no DataType 'UInt128' support.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Int8:
        {
            auto & data = typeid_cast<ColumnVector<Int8> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<Int8>(generator());
            }
            break;
        }
        case TypeIndex::Int16:
        {
            auto & data = typeid_cast<ColumnVector<Int16> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<Int16>(generator());
            }
            break;
        }
        case TypeIndex::Int32:
        {
            auto & data = typeid_cast<ColumnVector<Int32> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<Int32>(generator());
            }
            break;
        }
        case TypeIndex::Int64:
        {
            auto & data = typeid_cast<ColumnVector<Int64> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] =  static_cast<Int64>(generator64());
            }
            break;
        }
        case TypeIndex::Int128:
            throw Exception("There is no DataType 'Int128' support.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::Float32:
        {
            auto & data = typeid_cast<ColumnVector<Float32> &>(column).getData();
            data.resize(limit);
            double d = 1.0;
            for (UInt64 i = 0; i < limit; ++i)
            {
                d = std::numeric_limits<float>::max();
                data[i] = (d / pcg32::max()) * generator();
            }
            break;
        }
        case TypeIndex::Float64:
        {
            auto & data = typeid_cast<ColumnVector<Float64> &>(column).getData();
            data.resize(limit);
            double d = 1.0;
            for (UInt64 i = 0; i < limit; ++i)
            {
                d = std::numeric_limits<double>::max();
                data[i] = (d / pcg64::max()) * generator64();
            }
            break;
        }
        case TypeIndex::Date:
        {
            auto & data = typeid_cast<ColumnVector<UInt16> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<UInt16>(generator());
            }
            break;
        }
        case TypeIndex::DateTime:
        {
            auto & data = typeid_cast<ColumnVector<UInt32> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<UInt32>(generator());
            }
            break;
        }
        case TypeIndex::DateTime64:
        {
            UInt32 scale;
            if (auto * ptype = typeid_cast<const DataTypeDateTime64 *>(type.get()))
                scale = ptype->getScale();
            else
                throw Exception("Static cast to DataTypeDateTime64 failed ", ErrorCodes::BAD_TYPE_OF_FIELD);
            auto & data = typeid_cast<ColumnDecimal<Decimal64> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                UInt32 fractional = static_cast<UInt32>(generator()) % intExp10(scale);
                UInt32 whole = static_cast<UInt32>(generator());
                DateTime64 dt = DecimalUtils::decimalFromComponents<DateTime64>(whole, fractional, scale);
                data[i] = dt;
            }
            break;
        }
        case TypeIndex::String:
        {
            auto & column_string = typeid_cast<ColumnString &>(column);
            auto & offsets = column_string.getOffsets();
            auto & chars = column_string.getChars();

            UInt64 offset = 0;
            {
                offsets.resize(limit);
                for (UInt64 i = 0; i < limit; ++i)
                {
                    offset += 1 + static_cast<UInt64>(generator()) % max_string_length;
                    offsets[i] = offset;
                }
                chars.resize(offset);
                for (UInt64 i = 0; i < offset; ++i)
                {
                    if (offset - i > 5)
                    {
                        UInt32 r = generator();
                        chars[i] = 32 + (r & 0x7F) % 95;
                        chars[i+1] = 32 + ((r >> 7) & 0x7F) % 95;
                        chars[i+2] = 32 + ((r >> 14) & 0x7F) % 95;
                        chars[i+3] = 32 + ((r >> 21) & 0x7F) % 95;
                        chars[i+4] = 32 + (r >> 28);
                        i+=4;
                    }
                    else
                    {
                        UInt32 r = generator();
                        chars[i] = 32 + (r % 95);
                    }
                }
                // add terminating zero char
                for (auto & i : offsets)
                {
                    chars[i-1] = 0;
                }
            }
            break;
        }
        case TypeIndex::FixedString:
        {
            auto & column_string = typeid_cast<ColumnFixedString &>(column);
            size_t len = column_string.sizeOfValueIfFixed();
            auto & chars = column_string.getChars();

            UInt64 num_chars = static_cast<UInt64>(len) * limit;
            {
                chars.resize(num_chars);
                for (UInt64 i = 0; i < num_chars; ++i)
                {
                    chars[i] = static_cast<UInt8>(generator());
                }
            }
            break;
        }
        case TypeIndex::Enum8:
        {
            auto values = typeid_cast<const DataTypeEnum<Int8> *>(type.get())->getValues();
            auto & data = typeid_cast<ColumnVector<Int8> &>(column).getData();
            data.resize(limit);

            UInt8 size = values.size();
            UInt8 off;
            for (UInt64 i = 0; i < limit; ++i)
            {
                off = static_cast<UInt8>(generator()) % size;
                data[i] = values[off].second;
            }
            break;
        }
        case TypeIndex::Enum16:
        {
            auto values = typeid_cast<const DataTypeEnum<Int16> *>(type.get())->getValues();
            auto & data = typeid_cast<ColumnVector<Int16> &>(column).getData();
            data.resize(limit);

            UInt16 size = values.size();
            UInt8 off;
            for (UInt64 i = 0; i < limit; ++i)
            {
                off = static_cast<UInt16>(generator()) % size;
                data[i] = values[off].second;
            }
            break;
        }
        case TypeIndex::Decimal32:
        {
            auto & data = typeid_cast<ColumnDecimal<Decimal32> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                data[i] = static_cast<Int32>(generator());
            }
            break;
        }
        case TypeIndex::Decimal64:
        {
            auto & data = typeid_cast<ColumnDecimal<Decimal64> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                UInt64 a = static_cast<UInt64>(generator()) << 32 | static_cast<UInt64>(generator());
                data[i] = a;
            }
            break;
        }
        case TypeIndex::Decimal128:
        {
            auto & data = typeid_cast<ColumnDecimal<Decimal128> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                Int128 x = static_cast<Int128>(generator64()) << 64 | static_cast<Int128>(generator64());
                data[i] = x;
            }
        }
            break;
        case TypeIndex::UUID:
        {
            auto & data = typeid_cast<ColumnVector<UInt128> &>(column).getData();
            data.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                UInt64 a = static_cast<UInt64>(generator64());
                UInt64 b = static_cast<UInt64>(generator64());
                auto x = UInt128(a, b);
                data[i] = x;
            }
        }
            break;
        case TypeIndex::Array:
        {
            auto & column_array = typeid_cast<ColumnArray &>(column);
            auto nested_type = typeid_cast<const DataTypeArray *>(type.get())->getNestedType();

            auto & offsets = column_array.getOffsets();
            IColumn & data = column_array.getData();

            UInt64 offset = 0;
            {
                offsets.resize(limit);
                for (UInt64 i = 0; i < limit; ++i)
                {
                    offset += static_cast<UInt64>(generator()) % max_array_length;
                    offsets[i] = offset;
                }
            }
            fillColumnWithRandomData(data, nested_type, offset, max_array_length, max_string_length, generator, generator64);
            break;
        }
        case TypeIndex::Tuple:
        {
            auto &column_tuple = typeid_cast<ColumnTuple &>(column);
            auto elements = typeid_cast<const DataTypeTuple *>(type.get())->getElements();

            for (size_t i = 0; i < column_tuple.tupleSize(); ++i)
            {
                fillColumnWithRandomData(column_tuple.getColumn(i), elements[i], limit, max_array_length, max_string_length, generator, generator64);
            }
            break;
        }
        case TypeIndex::Set:
            throw Exception("Type 'Set' can not be stored in a table.", ErrorCodes::LOGICAL_ERROR);
        case TypeIndex::Interval:
            throw Exception("Type 'Interval' can not be stored in a table.", ErrorCodes::LOGICAL_ERROR);
        case TypeIndex::Nullable:
        {
            auto & column_nullable = typeid_cast<ColumnNullable &>(column);
            auto nested_type = typeid_cast<const DataTypeNullable *>(type.get())->getNestedType();

            auto & null_map = column_nullable.getNullMapData();
            IColumn & nested_column = column_nullable.getNestedColumn();

            fillColumnWithRandomData(nested_column, nested_type, limit, max_array_length, max_string_length, generator, generator64);

            null_map.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                null_map[i] = generator() < 1024;
            }
            break;
        }
        case TypeIndex::Function:
            throw Exception("Type 'Function' can not be stored in a table.", ErrorCodes::LOGICAL_ERROR);
        case TypeIndex::AggregateFunction:
            throw Exception("Random Generator not implemented for type 'AggregateFunction'.", ErrorCodes::NOT_IMPLEMENTED);
        case TypeIndex::LowCardinality:
            throw Exception("Random Generator not implemented for type 'LowCardinality'.", ErrorCodes::NOT_IMPLEMENTED);
    }
}

StorageGenerate::StorageGenerate(const StorageID & table_id_, const ColumnsDescription & columns_,
    UInt64 max_array_length_, UInt64 max_string_length_, UInt64 random_seed_)
    : IStorage(table_id_), max_array_length(max_array_length_), max_string_length(max_string_length_)
{
    random_seed = random_seed_ ? random_seed_ : randomSeed();
    setColumns(columns_);
}


class GenerateSource : public SourceWithProgress
{
public:
    GenerateSource(UInt64 block_size_, UInt64 max_array_length_, UInt64 max_string_length_, UInt64 random_seed_, Block block_header_)
        : SourceWithProgress(block_header_), block_size(block_size_), max_array_length(max_array_length_), max_string_length(max_string_length_)
        , block_header(block_header_), r32(random_seed_), r64(random_seed_) {}

    String getName() const override { return "Generate"; }

protected:
    Chunk generate() override
    {
        auto columns = block_header.cloneEmptyColumns();
        DataTypes types = block_header.getDataTypes();
        auto cur_type = types.cbegin();
        for (auto & col : columns)
        {
            fillColumnWithRandomData(col->assumeMutableRef(), *cur_type, block_size, max_array_length, max_string_length, r32, r64);
            ++cur_type;
        }
        return {std::move(columns), block_size};
    }

private:
    UInt64 block_size;
    UInt64 max_array_length;
    UInt64 max_string_length;
    Block block_header;

    pcg32 r32;
    pcg64_fast r64;

};


void registerStorageGenerate(StorageFactory & factory)
{
    factory.registerStorage("Generate", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() > 3)
            throw Exception("Storage Generate requires at most three arguments: "\
                        "max_array_length, max_string_length, random_seed.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 max_array_length_ = 10;
        UInt64 max_string_length_ = 10;
        UInt64 random_seed_ = 0; // zero for random

        /// Parsing second argument if present
        if (engine_args.size() >= 1)
            max_array_length_ = engine_args[0]->as<ASTLiteral &>().value.safeGet<UInt64>();

        if (engine_args.size() >= 2)
            max_string_length_ = engine_args[1]->as<ASTLiteral &>().value.safeGet<UInt64>();

        if (engine_args.size() == 3)
            random_seed_ = engine_args[2]->as<ASTLiteral &>().value.safeGet<UInt64>();

        return StorageGenerate::create(args.table_id, args.columns, max_array_length_, max_string_length_, random_seed_);
    });
}

Pipes StorageGenerate::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    check(column_names, true);

    Pipes pipes;
    pipes.reserve(num_streams);

    const ColumnsDescription & columns_ = getColumns();
    Block block_header;
    for (const auto & name : column_names)
    {
        const auto & name_type = columns_.get(name);
        MutableColumnPtr column = name_type.type->createColumn();
        block_header.insert({std::move(column), name_type.type, name_type.name});
    }

    pcg32 generate(random_seed);
    for (UInt64 i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<GenerateSource>(max_block_size, max_array_length, max_string_length, generate(), block_header));
    }
    return pipes;
}

}
