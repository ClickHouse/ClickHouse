#include <base/types.h>

#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/castColumn.h>
#include <Processors/Formats/Impl/Parquet/VariantEncoding.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/intExp.h>
#include <Common/intExp10.h>

#include <Interpreters/Context.h>

#include <Parsers/SecretArguments.h>

#include <bit>
#include <cstdio>
#include <cstdlib>

#include <fmt/format.h>

[[maybe_unused]] static const bool secret_arguments_finder_installed
    = (DB::setSecretArgumentsFinder(&DB::NoSecretArgumentsFinder::instance()), true);

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

ContextMutablePtr context;
extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();
    return 0;
}

namespace
{

/// The fuzz input is only a source of choices, so every input describes a valid column.
class Entropy
{
public:
    Entropy(const uint8_t * data_, size_t size_) : data(data_), size(size_) {}

    UInt64 bytes(size_t count)
    {
        UInt64 result = 0;
        for (size_t i = 0; i < count; ++i)
            result = (result << 8) | (pos < size ? data[pos++] : 0);
        return result;
    }

    UInt64 u64() { return bytes(8); }
    UInt64 uniform(UInt64 n) { return n <= 1 ? 0 : bytes(n <= 256 ? 1 : (n <= 0xFFFFFFFFULL ? 4 : 8)) % n; }
    bool oneIn(UInt64 n) { return uniform(n) == 0; }

private:
    const uint8_t * data;
    size_t size;
    size_t pos = 0;
};

constexpr size_t max_depth = 3;

String randomScalarTypeName(Entropy & e, bool & exact)
{
    switch (e.uniform(22))
    {
        case 0: return "Bool";
        case 1: return "Int8";
        case 2: return "Int16";
        case 3: return "Int32";
        case 4: return "Int64";
        case 5: return "UInt8";
        case 6: return "UInt16";
        case 7: return "UInt32";
        case 8: return "UInt64";
        case 9: return "Float32";
        case 10: return "Float64";
        case 11:
        {
            const UInt64 precision = 1 + e.uniform(38);
            return fmt::format("Decimal({}, {})", precision, e.uniform(precision + 1));
        }
        case 12: return "Date";
        case 13: return "Date32";
        case 14:
            exact = false;
            return e.oneIn(2) ? "DateTime" : "DateTime('UTC')";
        case 15:
        {
            const UInt64 scale = e.uniform(10);
            const bool with_time_zone = e.oneIn(2);
            exact = exact && with_time_zone && (scale == 6 || scale == 9);
            return with_time_zone ? fmt::format("DateTime64({}, 'UTC')", scale) : fmt::format("DateTime64({})", scale);
        }
        case 16:
        {
            const UInt64 scale = e.uniform(7);
            exact = exact && scale == 6;
            return fmt::format("Time64({})", scale);
        }
        case 17:
            exact = false;
            return "Time";
        case 18: return "String";
        case 19: return fmt::format("FixedString({})", 1 + e.uniform(70));
        case 20: return "Enum8('a' = 1, 'b' = 2)";
        default: return "UUID";
    }
}

String randomTypeName(Entropy & e, size_t depth, bool & exact)
{
    switch (depth < max_depth ? e.uniform(8) : 0)
    {
        case 1:
            return e.oneIn(2) ? "LowCardinality(String)" : "LowCardinality(Nullable(String))";
        case 2:
            return fmt::format("Nullable({})", randomScalarTypeName(e, exact));
        case 3:
            return fmt::format("Array({})", randomTypeName(e, depth + 1, exact));
        case 4:
        {
            exact = false;
            const size_t num_elements = 1 + e.uniform(3);
            const bool named = e.oneIn(2);
            String result = "Tuple(";
            for (size_t i = 0; i < num_elements; ++i)
            {
                if (i)
                    result += ", ";
                if (named)
                    result += fmt::format("e{} ", i);
                result += randomTypeName(e, depth + 1, exact);
            }
            return result + ")";
        }
        case 5:
            exact = false;
            return fmt::format("Map(String, {})", randomTypeName(e, depth + 1, exact));
        case 6:
            return "JSON";
        case 7:
            if (depth > 0)
                return "Dynamic";
            [[fallthrough]];
        default:
            return randomScalarTypeName(e, exact);
    }
}

String randomString(Entropy & e, size_t length)
{
    String result(length, '\0');
    for (auto & c : result)
        c = static_cast<char>(e.bytes(1));
    return result;
}

String randomKey(Entropy & e)
{
    String result;
    const size_t length = e.uniform(4);
    for (size_t i = 0; i < length; ++i)
        result += static_cast<char>('a' + e.uniform(3));
    return result;
}

Field randomJSONScalar(Entropy & e)
{
    switch (e.uniform(5))
    {
        case 0: return Int64(e.u64());
        case 1: return Float64(static_cast<Int32>(e.bytes(4))) / 4;
        case 2: return randomString(e, e.uniform(70));
        case 3: return e.oneIn(2);
        default:
        {
            Array array;
            const size_t length = e.uniform(4);
            for (size_t i = 0; i < length; ++i)
                array.push_back(Int64(e.u64()));
            return array;
        }
    }
}

void randomJSONObject(Entropy & e, const String & prefix, size_t depth, Object & object)
{
    const size_t num_keys = e.uniform(4);
    for (size_t i = 0; i < num_keys; ++i)
    {
        const String path = prefix + static_cast<char>('a' + i);
        if (depth < max_depth && e.oneIn(3))
            randomJSONObject(e, path + ".", depth + 1, object);
        else
            object[path] = randomJSONScalar(e);
    }
}

Int128 randomDecimalValue(Entropy & e, UInt32 precision)
{
    const Int128 bound = intExp10OfSize<Int128>(precision);
    const Int128 value = static_cast<Int128>((static_cast<UInt128>(e.u64()) << 64) | e.u64()) % bound;
    return value;
}

Field randomField(const IDataType & type, Entropy & e)
{
    switch (type.getTypeId())
    {
        case TypeIndex::Nullable:
            if (e.oneIn(4))
                return Null();
            return randomField(*assert_cast<const DataTypeNullable &>(type).getNestedType(), e);
        case TypeIndex::LowCardinality:
            return randomField(*assert_cast<const DataTypeLowCardinality &>(type).getDictionaryType(), e);
        case TypeIndex::UInt8:
            if (type.getName() == "Bool")
                return e.oneIn(2);
            return UInt64(e.bytes(1));
        case TypeIndex::UInt16: return UInt64(e.bytes(2));
        case TypeIndex::UInt32: return UInt64(e.bytes(4));
        case TypeIndex::UInt64: return UInt64(e.u64() >> 1);
        case TypeIndex::Int8: return Int64(static_cast<Int8>(e.bytes(1)));
        case TypeIndex::Int16: return Int64(static_cast<Int16>(e.bytes(2)));
        case TypeIndex::Int32: return Int64(static_cast<Int32>(e.bytes(4)));
        case TypeIndex::Int64: return Int64(e.u64());
        case TypeIndex::Float32: return Float64(std::bit_cast<Float32>(static_cast<UInt32>(e.bytes(4))));
        case TypeIndex::Float64: return std::bit_cast<Float64>(e.u64());
        case TypeIndex::Decimal32:
            return DecimalField<Decimal32>(static_cast<Int32>(randomDecimalValue(e, getDecimalPrecision(type))), getDecimalScale(type));
        case TypeIndex::Decimal64:
            return DecimalField<Decimal64>(static_cast<Int64>(randomDecimalValue(e, getDecimalPrecision(type))), getDecimalScale(type));
        case TypeIndex::Decimal128:
            return DecimalField<Decimal128>(randomDecimalValue(e, getDecimalPrecision(type)), getDecimalScale(type));
        case TypeIndex::Date: return UInt64(e.bytes(2));
        case TypeIndex::Date32: return Int64(-25567 + static_cast<Int64>(e.uniform(120530 + 25567)));
        case TypeIndex::DateTime: return UInt64(e.bytes(4));
        case TypeIndex::DateTime64:
        {
            const UInt32 scale = assert_cast<const DataTypeDateTime64 &>(type).getScale();
            const Int64 multiplier = static_cast<Int64>(intExp10(static_cast<int>(scale)));
            const UInt64 max_seconds = scale > 6 ? 9'000'000'000ULL : 10'413'792'000ULL;
            const Int64 seconds = -2208988800LL + static_cast<Int64>(e.uniform(max_seconds + 2208988800ULL));
            return DecimalField<DateTime64>(seconds * multiplier + static_cast<Int64>(e.uniform(static_cast<UInt64>(multiplier))), scale);
        }
        case TypeIndex::Time: return Int64(e.uniform(86400));
        case TypeIndex::Time64:
        {
            const UInt32 scale = assert_cast<const DataTypeTime64 &>(type).getScale();
            return DecimalField<Time64>(static_cast<Int64>(e.uniform(86400 * intExp10(static_cast<int>(scale)))), scale);
        }
        case TypeIndex::String: return randomString(e, e.oneIn(3) ? 64 + e.uniform(40) : e.uniform(64));
        case TypeIndex::FixedString: return randomString(e, assert_cast<const DataTypeFixedString &>(type).getN());
        case TypeIndex::Enum8: return Int64(1 + e.uniform(2));
        case TypeIndex::UUID: return UUID(UInt128(e.u64()) << 64 | e.u64());
        case TypeIndex::Array:
        {
            const auto & nested = *assert_cast<const DataTypeArray &>(type).getNestedType();
            Array array;
            const size_t length = e.uniform(5);
            for (size_t i = 0; i < length; ++i)
                array.push_back(randomField(nested, e));
            return array;
        }
        case TypeIndex::Tuple:
        {
            Tuple tuple;
            for (const auto & element : assert_cast<const DataTypeTuple &>(type).getElements())
                tuple.push_back(randomField(*element, e));
            return tuple;
        }
        case TypeIndex::Map:
        {
            const auto & value_type = *assert_cast<const DataTypeMap &>(type).getValueType();
            Map map;
            VectorWithMemoryTracking<String> keys;
            const size_t length = e.uniform(5);
            for (size_t i = 0; i < length; ++i)
            {
                String key = randomKey(e);
                if (std::find(keys.begin(), keys.end(), key) != keys.end())
                    continue;
                keys.push_back(key);
                map.push_back(Tuple{key, randomField(value_type, e)});
            }
            return map;
        }
        case TypeIndex::Object:
        {
            Object object;
            randomJSONObject(e, "", 0, object);
            return object;
        }
        case TypeIndex::Dynamic:
            return e.oneIn(5) ? Field(Null()) : randomJSONScalar(e);
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type {} in the Parquet variant fuzzer", type.getName());
    }
}

ColumnPtr randomColumn(const DataTypePtr & type, Entropy & e)
{
    auto column = type->createColumn();
    const size_t rows = 1 + e.uniform(4);
    for (size_t i = 0; i < rows; ++i)
        column->insert(randomField(*type, e));
    return column;
}

bool canBeInsideVariant(const String & name)
{
    return !name.starts_with("Nullable") && !name.starts_with("LowCardinality(Nullable");
}

ColumnPtr randomVariantColumn(Entropy & e, bool & exact, DataTypePtr & type)
{
    String first = randomTypeName(e, 1, exact);
    String second = randomTypeName(e, 1, exact);
    if (!canBeInsideVariant(first) || !canBeInsideVariant(second) || first == second || first == "Dynamic" || second == "Dynamic")
        return nullptr;

    type = DataTypeFactory::instance().get(fmt::format("Variant({}, {})", first, second));
    auto result = type->createColumn();
    for (const auto & name : {first, second})
    {
        const auto variant_type = DataTypeFactory::instance().get(name);
        const auto casted = castColumn({randomColumn(variant_type, e), variant_type, ""}, type);
        result->insertRangeFrom(*casted, 0, casted->size());
        if (e.oneIn(3))
            result->insertDefault();
    }
    return result;
}

[[noreturn]] void fail(const String & message)
{
    fprintf(stderr, "Parquet variant fuzzer: %s\n", message.c_str()); // NOLINT
    abort();
}

String rowText(const IColumn & column, const DataTypePtr & type, size_t row)
{
    WriteBufferFromOwnString out;
    type->getDefaultSerialization()->serializeText(column, row, out, FormatSettings{});
    return out.str();
}

struct Encoded
{
    MutableColumnPtr metadata = ColumnString::create();
    MutableColumnPtr value = ColumnString::create();
};

Encoded encode(const IColumn & column, const DataTypePtr & type, bool mark_nulls)
{
    Encoded result;
    PaddedPODArray<UInt8> null_map;
    Parquet::encodeVariantColumn(
        column, type, "v", Parquet::VariantSettings{}, assert_cast<ColumnString &>(*result.metadata), assert_cast<ColumnString &>(*result.value),
        mark_nulls ? &null_map : nullptr);
    return result;
}

MutableColumnPtr decode(const Encoded & encoded, const DataTypePtr & type)
{
    auto column = type->createColumn();
    Parquet::decodeVariantColumn(*encoded.metadata, *encoded.value, *column, type, "v", encoded.value->size(), Parquet::VariantSettings{});
    return column;
}

void checkSameValues(const IColumn & left, const IColumn & right, const DataTypePtr & type, const char * what)
{
    if (left.size() != right.size())
        fail(fmt::format("{}: {} rows vs {} rows", what, left.size(), right.size()));
    for (size_t row = 0; row < left.size(); ++row)
    {
        const String left_text = rowText(left, type, row);
        const String right_text = rowText(right, type, row);
        if (left_text != right_text)
            fail(fmt::format("{}: row {} is {} vs {}", what, row, left_text, right_text));
    }
}

void checkSameBytes(const Encoded & left, const Encoded & right)
{
    for (size_t row = 0; row < left.value->size(); ++row)
        if (left.metadata->getDataAt(row) != right.metadata->getDataAt(row) || left.value->getDataAt(row) != right.value->getDataAt(row))
            fail(fmt::format("encoding is not stable at row {}", row));
}

}

/// Every input describes a valid `Dynamic` column. Its encoding must decode; a column of types that the variant
/// encoding keeps as is must decode to the same values; and a second round trip must change neither values nor bytes.
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    total_memory_tracker.resetCounters();
    total_memory_tracker.setHardLimit(1_GiB);
    CurrentThread::get().memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

    Entropy e(data, size);
    const DataTypePtr type = DataTypeFactory::instance().get(
        e.oneIn(2) ? String("Dynamic") : fmt::format("Dynamic(max_types={})", 1 + e.uniform(4)));
    const bool mark_nulls = e.oneIn(2);

    bool exact = true;
    auto column = type->createColumn();
    const size_t num_chunks = 1 + e.uniform(4);
    for (size_t chunk = 0; chunk < num_chunks; ++chunk)
    {
        DataTypePtr chunk_type;
        ColumnPtr chunk_column;
        if (e.oneIn(4))
            chunk_column = randomVariantColumn(e, exact, chunk_type);
        if (!chunk_column)
        {
            chunk_type = DataTypeFactory::instance().get(randomTypeName(e, 0, exact));
            chunk_column = randomColumn(chunk_type, e);
        }
        const auto casted = castColumn({chunk_column, chunk_type, ""}, type);
        column->insertRangeFrom(*casted, 0, casted->size());
        if (e.oneIn(4))
            column->insertDefault();
    }

    try
    {
        const Encoded first_encoded = encode(*column, type, mark_nulls);
        const auto first_decoded = decode(first_encoded, type);
        if (exact)
            checkSameValues(*column, *first_decoded, type, "decoded values differ from the original");

        const Encoded second_encoded = encode(*first_decoded, type, mark_nulls);
        const auto second_decoded = decode(second_encoded, type);
        checkSameValues(*first_decoded, *second_decoded, type, "second round trip changed the values");
        checkSameBytes(second_encoded, encode(*second_decoded, type, mark_nulls));
    }
    catch (const Exception & ex)
    {
        if (ex.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            return 0;
        fail(fmt::format("unexpected exception for a valid column: {}", ex.message()));
    }

    return 0;
}
