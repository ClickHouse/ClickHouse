#include "NativeORCBlockInputFormat.h"

#if USE_ORC
#    include <Columns/ColumnDecimal.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/ColumnsCommon.h>
#    include <Columns/ColumnsDateTime.h>
#    include <Columns/ColumnsNumber.h>
#    include <Common/DateLUTImpl.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDate32.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeFixedString.h>
#    include <DataTypes/DataTypeIPv4andIPv6.h>
#    include <DataTypes/DataTypeLowCardinality.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNested.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/NestedUtils.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/SchemaInferenceUtils.h>
#    include <Formats/insertNullAsDefaultIfNeeded.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/SharedThreadPools.h>
#    include <IO/WriteHelpers.h>
#    include <IO/copyData.h>
#    include <Interpreters/Set.h>
#    include <Interpreters/castColumn.h>
#    include <Storages/MergeTree/KeyCondition.h>
#    include <Storages/ObjectStorage/HDFS/ReadBufferFromHDFS.h>
#    include <base/MemorySanitizer.h>
#    include <orc/MemoryPool.hh>
#    include <orc/Vector.hh>
#    include <Common/Allocator.h>
#    include <Common/logger_useful.h>
#    include <Common/quoteString.h>
#    include <Common/memory.h>

#    include "ArrowBufferedStreams.h"

#    include <boost/algorithm/string.hpp>


namespace
{

class MemoryPool : public orc::MemoryPool
{
public:
    char * malloc(uint64_t size) override
    {
        void * ptr = ::malloc(size);
        if (ptr)
        {
            AllocationTrace trace;
            size_t actual_size = Memory::trackMemory(size, trace);
            trace.onAlloc(ptr, actual_size);
        }

        /// For nullable columns some of the values will not be initialized.
        __msan_unpoison(ptr, size);
        return static_cast<char *>(ptr);
    }

    void free(char * ptr) override
    {
        AllocationTrace trace;
        size_t actual_size = Memory::untrackMemory(ptr, trace);
        trace.onFree(ptr, actual_size);
        ::free(ptr);
    }
};

}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TYPE;
extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
extern const int THERE_IS_NO_COLUMN;
extern const int INCORRECT_DATA;
extern const int ARGUMENT_OUT_OF_BOUND;
}


ORCInputStream::ORCInputStream(SeekableReadBuffer & in_, size_t file_size_, bool use_prefetch)
    : in(in_), file_size(file_size_), supports_read_at(use_prefetch && in_.supportsReadAt())
{
    if (supports_read_at)
        async_runner = threadPoolCallbackRunnerUnsafe<void>(getIOThreadPool().get(), "ORCFile");
}

UInt64 ORCInputStream::getLength() const
{
    return file_size;
}

UInt64 ORCInputStream::getNaturalReadSize() const
{
    return 128 * 1024;
}

void ORCInputStream::read(void * buf, UInt64 length, UInt64 offset)
{
    if (supports_read_at)
    {
        size_t bytes_read = 0;
        while (bytes_read < length)
        {
            size_t bytes_to_read = length - bytes_read;
            size_t n = in.readBigAt(reinterpret_cast<char *>(buf) + bytes_read, bytes_to_read, offset + bytes_read, nullptr);
            bytes_read += n;
        }
    }
    else
    {
        if (offset != static_cast<UInt64>(in.getPosition()))
            in.seek(offset, SEEK_SET);
        in.readStrict(reinterpret_cast<char *>(buf), length);
    }
}

std::future<void> ORCInputStream::readAsync(void * buf, uint64_t length, uint64_t offset)
{
    if (supports_read_at)
    {
        return async_runner(
            [this, buf, length, offset]
            {
                Stopwatch time;
                read(buf, length, offset);
                LOG_TEST(
                    getLogger("NativeORCBlockInputFormat"),
                    "Read {} bytes from {} offset in {} ms",
                    length,
                    offset,
                    time.elapsed() / 1000000);
            },
            Priority{});
    }
    else
    {
        read(buf, length, offset);
        std::promise<void> promise;
        promise.set_value();
        return promise.get_future();
    }
}

std::unique_ptr<orc::InputStream>
asORCInputStream(ReadBuffer & in, const FormatSettings & settings, bool use_prefetch, std::atomic<int> & is_cancelled)
{
    bool has_file_size = isBufferWithFileSize(in);
    auto * seekable_in = dynamic_cast<SeekableReadBuffer *>(&in);

    if (has_file_size && seekable_in && settings.seekable_read && seekable_in->checkIfActuallySeekable())
        return std::make_unique<ORCInputStream>(*seekable_in, getFileSizeFromReadBuffer(in), use_prefetch);

    /// Fallback to loading the entire file in memory
    return asORCInputStreamLoadIntoMemory(in, is_cancelled);
}

std::unique_ptr<orc::InputStream> asORCInputStreamLoadIntoMemory(ReadBuffer & in, std::atomic<int> & is_cancelled)
{
    size_t magic_size = strlen(ORC_MAGIC_BYTES);
    std::string file_data(magic_size, '\0');

    /// Avoid loading the whole file if it doesn't seem to even be in the correct format.
    size_t bytes_read = in.read(file_data.data(), magic_size);
    if (bytes_read < magic_size || file_data != ORC_MAGIC_BYTES)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not an ORC file");

    {
        WriteBufferFromString file_buffer(file_data, AppendModeTag{});
        copyData(in, file_buffer, is_cancelled);
    }

    size_t file_size = file_data.size();
    return std::make_unique<ORCInputStreamFromString>(std::move(file_data), file_size);
}

static const orc::Type * getORCTypeByName(const orc::Type & schema, const String & name, bool ignore_case)
{
    for (UInt64 i = 0; i != schema.getSubtypeCount(); ++i)
        if (boost::equals(schema.getFieldName(i), name) || (ignore_case && boost::iequals(schema.getFieldName(i), name)))
            return schema.getSubtype(i);
    return nullptr;
}

static bool isDictionaryEncoded(const orc::StripeInformation * stripe_info, const orc::Type * orc_type)
{
    if (!stripe_info)
        return false;

    auto encoding = stripe_info->getColumnEncoding(orc_type->getColumnId());
    return encoding == orc::ColumnEncodingKind_DICTIONARY || encoding == orc::ColumnEncodingKind_DICTIONARY_V2;
}

static DataTypePtr parseORCType(
    const orc::Type * orc_type,
    bool skip_columns_with_unsupported_types,
    bool dictionary_as_low_cardinality,
    const orc::StripeInformation * stripe_info,
    bool & skipped)
{
    assert(orc_type != nullptr);

    const int subtype_count = static_cast<int>(orc_type->getSubtypeCount());
    switch (orc_type->getKind())
    {
        case orc::TypeKind::BOOLEAN:
            return DataTypeFactory::instance().get("Bool");
        case orc::TypeKind::BYTE:
            return std::make_shared<DataTypeInt8>();
        case orc::TypeKind::SHORT:
            return std::make_shared<DataTypeInt16>();
        case orc::TypeKind::INT:
            return std::make_shared<DataTypeInt32>();
        case orc::TypeKind::LONG:
            return std::make_shared<DataTypeInt64>();
        case orc::TypeKind::FLOAT:
            return std::make_shared<DataTypeFloat32>();
        case orc::TypeKind::DOUBLE:
            return std::make_shared<DataTypeFloat64>();
        case orc::TypeKind::DATE:
            return std::make_shared<DataTypeDate32>();
        case orc::TypeKind::TIMESTAMP:
            return std::make_shared<DataTypeDateTime64>(9);
        case orc::TypeKind::TIMESTAMP_INSTANT:
            return std::make_shared<DataTypeDateTime64>(9, "UTC");
        case orc::TypeKind::CHAR:
        case orc::TypeKind::VARCHAR:
        case orc::TypeKind::BINARY:
        case orc::TypeKind::STRING: {
            DataTypePtr type;
            if (orc_type->getKind() == orc::TypeKind::CHAR)
                type = std::make_shared<DataTypeFixedString>(orc_type->getMaximumLength());
            else
                type = std::make_shared<DataTypeString>();

            /// Wrap type in LowCardinality if ORC column is dictionary encoded and dictionary_as_low_cardinality is true
            if (dictionary_as_low_cardinality && isDictionaryEncoded(stripe_info, orc_type))
                type = std::make_shared<DataTypeLowCardinality>(type);

            return type;
        }
        case orc::TypeKind::DECIMAL: {
            UInt64 precision = orc_type->getPrecision();
            UInt64 scale = orc_type->getScale();
            if (precision == 0)
            {
                // In HIVE 0.11/0.12 precision is set as 0, but means max precision
                return createDecimal<DataTypeDecimal>(38, 6);
            }
            return createDecimal<DataTypeDecimal>(precision, scale);
        }
        case orc::TypeKind::LIST: {
            if (subtype_count != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Orc List type {}", orc_type->toString());

            DataTypePtr nested_type = parseORCType(
                orc_type->getSubtype(0), skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped);
            if (skipped)
                return {};

            return std::make_shared<DataTypeArray>(nested_type);
        }
        case orc::TypeKind::MAP: {
            if (subtype_count != 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Orc Map type {}", orc_type->toString());

            DataTypePtr key_type = parseORCType(
                orc_type->getSubtype(0), skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped);
            if (skipped)
                return {};

            DataTypePtr value_type = parseORCType(
                orc_type->getSubtype(1), skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped);
            if (skipped)
                return {};

            return std::make_shared<DataTypeMap>(key_type, value_type);
        }
        case orc::TypeKind::STRUCT: {
            DataTypes nested_types;
            Strings nested_names;
            nested_types.reserve(subtype_count);
            nested_names.reserve(subtype_count);

            for (size_t i = 0; i < orc_type->getSubtypeCount(); ++i)
            {
                auto parsed_type = parseORCType(
                    orc_type->getSubtype(i), skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped);
                if (skipped)
                    return {};

                nested_types.push_back(parsed_type);
                nested_names.push_back(orc_type->getFieldName(i));
            }
            return std::make_shared<DataTypeTuple>(nested_types, nested_names);
        }
        default: {
            if (skip_columns_with_unsupported_types)
            {
                skipped = true;
                return {};
            }

            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported ORC type '{}'."
                "If you want to skip columns with unsupported types, "
                "you can enable setting input_format_orc_skip_columns_with_unsupported_types_in_schema_inference",
                orc_type->toString());
        }
    }
}

static std::optional<orc::PredicateDataType> convertORCTypeToPredicateType(const orc::Type & orc_type)
{
    switch (orc_type.getKind())
    {
        case orc::BOOLEAN:
            return orc::PredicateDataType::BOOLEAN;
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG:
            return orc::PredicateDataType::LONG;
        case orc::FLOAT:
        case orc::DOUBLE:
            return orc::PredicateDataType::FLOAT;
        case orc::VARCHAR:
        case orc::CHAR:
        case orc::STRING:
            return orc::PredicateDataType::STRING;
        case orc::DATE:
            return orc::PredicateDataType::DATE;
        case orc::TIMESTAMP:
            return orc::PredicateDataType::TIMESTAMP;
        case orc::DECIMAL:
            return orc::PredicateDataType::DECIMAL;
        default:
            return {};
    }
}

static String getColumnNameFromKeyCondition(const KeyCondition & key_condition, size_t indice)
{
    const auto & key_columns = key_condition.getKeyColumns();
    for (const auto & [name, i] : key_columns)
    {
        if (i == indice)
            return name;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't get column from KeyCondition with indice {}", indice);
}

static std::optional<orc::Literal>
convertFieldToORCLiteral(const orc::Type & orc_type, const Field & field, DataTypePtr type_hint = nullptr)
{
    try
    {
        /// We always fallback to return null if possible CH type hint not consistent with ORC type
        switch (orc_type.getKind())
        {
            case orc::BOOLEAN: {
                /// May throw exception
                auto val = field.safeGet<UInt64>();
                return orc::Literal(val != 0);
            }
            case orc::BYTE:
            case orc::SHORT:
            case orc::INT:
            case orc::LONG: {
                /// May throw exception.
                ///
                /// In particular, it'll throw if we request the column as unsigned, like this:
                ///   SELECT * FROM file('t.orc', ORC, 'x UInt8') WHERE x > 10
                /// We have to reject this, otherwise it would miss values > 127 (because
                /// they're treated as negative by ORC).
                auto val = field.safeGet<Int64>();
                return orc::Literal(val);
            }
            case orc::FLOAT:
            case orc::DOUBLE: {
                Float64 val;
                if (field.tryGet(val))
                    return orc::Literal(val);
                break;
            }
            case orc::VARCHAR:
            case orc::CHAR:
            case orc::STRING: {
                String str;
                if (field.tryGet(str))
                    return orc::Literal(str.data(), str.size());
                break;
            }
            case orc::DATE: {
                Int64 val;
                if (field.tryGet(val))
                    return orc::Literal(orc::PredicateDataType::DATE, val);
                break;
            }
            case orc::TIMESTAMP: {
                if (type_hint && isDateTime64(type_hint))
                {
                    const auto * datetime64_type = typeid_cast<const DataTypeDateTime64 *>(type_hint.get());
                    if (datetime64_type->getScale() != 9)
                        return std::nullopt;
                }

                DecimalField<Decimal64> ts;
                if (field.tryGet(ts))
                {
                    Int64 secs = (ts.getValue() / ts.getScaleMultiplier()).convertTo<Int64>();
                    Int32 nanos = (ts.getValue() - (ts.getValue() / ts.getScaleMultiplier()) * ts.getScaleMultiplier()).convertTo<Int32>();
                    return orc::Literal(secs, nanos);
                }
                break;
            }
            case orc::DECIMAL: {
                auto precision = orc_type.getPrecision();
                if (precision == 0)
                    precision = 38;

                if (precision <= DecimalUtils::max_precision<Decimal32>)
                {
                    DecimalField<Decimal32> val;
                    if (field.tryGet(val))
                    {
                        Int64 right = val.getValue().convertTo<Int64>();
                        return orc::Literal(
                            orc::Int128(right), static_cast<Int32>(orc_type.getPrecision()), static_cast<Int32>(orc_type.getScale()));
                    }
                }
                else if (precision <= DecimalUtils::max_precision<Decimal64>)
                {
                    DecimalField<Decimal64> val;
                    if (field.tryGet(val))
                    {
                        Int64 right = val.getValue().convertTo<Int64>();
                        return orc::Literal(
                            orc::Int128(right), static_cast<Int32>(orc_type.getPrecision()), static_cast<Int32>(orc_type.getScale()));
                    }
                }
                else if (precision <= DecimalUtils::max_precision<Decimal128>)
                {
                    DecimalField<Decimal128> val;
                    if (field.tryGet(val))
                    {
                        Int64 high = val.getValue().value.items[1];
                        UInt64 low = static_cast<UInt64>(val.getValue().value.items[0]);
                        return orc::Literal(
                            orc::Int128(high, low), static_cast<Int32>(orc_type.getPrecision()), static_cast<Int32>(orc_type.getScale()));
                    }
                }
                break;
            }
            default:
                break;
        }
        return std::nullopt;
    }
    catch (Exception &)
    {
        return std::nullopt;
    }
}

/// Attention: evaluateRPNElement is only invoked in buildORCSearchArgumentImpl.
/// So it is guaranteed that:
///     1. elem has no monotonic_functions_chains.
///     2. if elem function is FUNCTION_IN_RANGE/FUNCTION_NOT_IN_RANGE, `set_index` is not null and `set_index->getOrderedSet().size()` is 1.
///     3. elem function should be FUNCTION_IN_RANGE/FUNCTION_NOT_IN_RANGE/FUNCTION_IN_SET/FUNCTION_NOT_IN_SET/FUNCTION_IS_NULL/FUNCTION_IS_NOT_NULL
static bool evaluateRPNElement(const Field & field, const KeyCondition::RPNElement & elem)
{
    Range key_range(field);
    switch (elem.function)
    {
        case KeyCondition::RPNElement::FUNCTION_IN_RANGE:
        case KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE: {
            /// Rows with null values should never output when filters like ">=", ">", "<=", "<", '=' are applied
            if (field.isNull())
                return false;

            bool res = elem.range.intersectsRange(key_range);
            if (elem.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE)
                res = !res;
            return res;
        }
        case KeyCondition::RPNElement::FUNCTION_IN_SET:
        case KeyCondition::RPNElement::FUNCTION_NOT_IN_SET: {
            const auto & set_index = elem.set_index;
            const auto & ordered_set = set_index->getOrderedSet();
            const auto & set_column = ordered_set[0];

            bool res = false;
            for (size_t i = 0; i < set_column->size(); ++i)
            {
                if (Range::equals(field, (*set_column)[i]))
                {
                    res = true;
                    break;
                }
            }

            if (elem.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_SET)
                res = !res;
            return res;
        }
        case KeyCondition::RPNElement::FUNCTION_IS_NULL:
        case KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL: {
            if (field.isNull())
                return elem.function == KeyCondition::RPNElement::FUNCTION_IS_NULL;
            return elem.function == KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected RPNElement Function {}", elem.toString());
    }
}

static void buildORCSearchArgumentImpl(
    const KeyCondition & key_condition,
    const Block & header,
    const orc::Type & schema,
    KeyCondition::RPN & rpn_stack,
    orc::SearchArgumentBuilder & builder,
    const FormatSettings & format_settings)
{
    if (rpn_stack.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty rpn stack in buildORCSearchArgumentImpl");

    const auto & curr = rpn_stack.back();
    switch (curr.function)
    {
        case KeyCondition::RPNElement::FUNCTION_IN_RANGE:
        case KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE:
        case KeyCondition::RPNElement::FUNCTION_IN_SET:
        case KeyCondition::RPNElement::FUNCTION_NOT_IN_SET:
        case KeyCondition::RPNElement::FUNCTION_IS_NULL:
        case KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL: {
            const bool need_wrap_not = curr.function == KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL
                || curr.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE
                || curr.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_SET;
            const bool contains_is_null = curr.function == KeyCondition::RPNElement::FUNCTION_IS_NULL
                || curr.function == KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL;
            const bool contains_in_set = curr.function == KeyCondition::RPNElement::FUNCTION_IN_SET
                || curr.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_SET;
            const bool contains_in_range = curr.function == KeyCondition::RPNElement::FUNCTION_IN_RANGE
                || curr.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE;

            SCOPE_EXIT({ rpn_stack.pop_back(); });


            /// Key filter expressions like "func(col) > 100" are not supported for ORC filter push down
            if (!curr.monotonic_functions_chain.empty())
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            /// key filter expressions like "(a, b, c) in " or "(func(a), b) in " are not supported for ORC filter push down
            /// Only expressions like "a in " are supported currently, maybe we can improve it later.
            auto set_index = curr.set_index;
            if (contains_in_set)
            {
                if (!set_index || set_index->getOrderedSet().size() != 1 || set_index->hasMonotonicFunctionsChain())
                {
                    builder.literal(orc::TruthValue::YES_NO_NULL);
                    break;
                }
            }

            String column_name = getColumnNameFromKeyCondition(key_condition, curr.key_column);
            const auto * orc_type = getORCTypeByName(schema, column_name, format_settings.orc.case_insensitive_column_matching);
            if (!orc_type)
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            /// Make sure key column in header has exactly the same type with key column in ORC file schema
            /// Counter-example 1:
            ///     Column a has type "Nullable(Int64)" in ORC file, but in header column a has type "Int64", which is allowed in CH.
            ///     For queries with where condition like "a is null", if a column contains null value, pushing or not pushing down filters
            ///     would result in different outputs.
            /// Counter-example 2:
            ///     Column a has type "Nullable(Int64)" in ORC file, but in header column a has type "Nullable(UInt64)".
            ///     For queries with where condition like "a > 10", if a column contains negative values such as "-1", pushing or not pushing
            ///     down filters would result in different outputs.
            bool skipped = false;
            auto expect_type = makeNullableRecursively(parseORCType(orc_type, true, false, nullptr, skipped), format_settings);
            const ColumnWithTypeAndName * column = header.findByName(column_name, format_settings.orc.case_insensitive_column_matching);
            if (!expect_type || !column)
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            auto nested_type = removeNullable(recursiveRemoveLowCardinality(column->type));
            auto expect_nested_type = removeNullable(expect_type);
            if (!nested_type->equals(*expect_nested_type))
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            /// If null_as_default is true, the only difference is nullable, and the evaluations of current RPNElement based on default and null field
            /// have the same result, we still should push down current filter.
            if (format_settings.null_as_default && !column->type->isNullable() && !column->type->isLowCardinalityNullable())
            {
                bool match_if_null = evaluateRPNElement({}, curr);
                bool match_if_default = evaluateRPNElement(column->type->getDefault(), curr);
                if (match_if_default != match_if_null)
                {
                    builder.literal(orc::TruthValue::YES_NO_NULL);
                    break;
                }
            }

            auto predicate_type = convertORCTypeToPredicateType(*orc_type);
            if (!predicate_type.has_value())
            {
                builder.literal(orc::TruthValue::YES_NO_NULL);
                break;
            }

            if (need_wrap_not)
                builder.startNot();

            if (contains_is_null)
            {
                builder.isNull(orc_type->getColumnId(), *predicate_type);
            }
            else if (contains_in_range)
            {
                const auto & range = curr.range;
                bool has_left_bound = !range.left.isNegativeInfinity();
                bool has_right_bound = !range.right.isPositiveInfinity();
                if (!has_left_bound && !has_right_bound)
                {
                    /// Transform whole range orc::TruthValue::YES_NULL
                    builder.literal(orc::TruthValue::YES_NULL);
                }
                else if (has_left_bound && has_right_bound && range.left_included && range.right_included && range.left == range.right)
                {
                    /// Transform range with the same left bound and right bound to equal, which could utilize bloom filters in ORC
                    auto literal = convertFieldToORCLiteral(*orc_type, range.left);
                    if (literal.has_value())
                        builder.equals(orc_type->getColumnId(), *predicate_type, *literal);
                    else
                        builder.literal(orc::TruthValue::YES_NO_NULL);
                }
                else
                {
                    std::optional<orc::Literal> left_literal;
                    if (has_left_bound)
                        left_literal = convertFieldToORCLiteral(*orc_type, range.left);

                    std::optional<orc::Literal> right_literal;
                    if (has_right_bound)
                        right_literal = convertFieldToORCLiteral(*orc_type, range.right);

                    if (has_left_bound && has_right_bound)
                        builder.startAnd();

                    if (has_left_bound)
                    {
                        if (left_literal.has_value())
                        {
                            /// >= is transformed to not < and > is transformed to not <=
                            builder.startNot();
                            if (range.left_included)
                                builder.lessThan(orc_type->getColumnId(), *predicate_type, *left_literal);
                            else
                                builder.lessThanEquals(orc_type->getColumnId(), *predicate_type, *left_literal);
                            builder.end();
                        }
                        else
                            builder.literal(orc::TruthValue::YES_NO_NULL);
                    }

                    if (has_right_bound)
                    {
                        if (right_literal.has_value())
                        {
                            if (range.right_included)
                                builder.lessThanEquals(orc_type->getColumnId(), *predicate_type, *right_literal);
                            else
                                builder.lessThan(orc_type->getColumnId(), *predicate_type, *right_literal);
                        }
                        else
                            builder.literal(orc::TruthValue::YES_NO_NULL);
                    }

                    if (has_left_bound && has_right_bound)
                        builder.end();
                }
            }
            else if (contains_in_set)
            {
                /// Build literals from MergeTreeSetIndex
                const auto & ordered_set = set_index->getOrderedSet();
                const auto & set_column = ordered_set[0];

                bool fail = false;
                std::vector<orc::Literal> literals;
                literals.reserve(set_column->size());
                for (size_t i = 0; i < set_column->size(); ++i)
                {
                    auto literal = convertFieldToORCLiteral(*orc_type, (*set_column)[i]);
                    if (!literal.has_value())
                    {
                        fail = true;
                        break;
                    }

                    literals.emplace_back(*literal);
                }

                /// set has zero element
                if (literals.empty())
                    builder.literal(orc::TruthValue::YES);
                else if (fail)
                    builder.literal(orc::TruthValue::YES_NO_NULL);
                else
                    builder.in(orc_type->getColumnId(), *predicate_type, literals);
            }

            if (need_wrap_not)
                builder.end();

            break;
        }
        /// There is no optimization with space-filling curves for ORC.
        case KeyCondition::RPNElement::FUNCTION_ARGS_IN_HYPERRECTANGLE:
        /// There is no optimization with pointInPolygon for ORC.
        case KeyCondition::RPNElement::FUNCTION_POINT_IN_POLYGON:
        case KeyCondition::RPNElement::FUNCTION_UNKNOWN: {
            builder.literal(orc::TruthValue::YES_NO_NULL);
            rpn_stack.pop_back();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_NOT: {
            builder.startNot();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_AND: {
            builder.startAnd();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_OR: {
            builder.startOr();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::ALWAYS_FALSE: {
            builder.literal(orc::TruthValue::NO);
            rpn_stack.pop_back();
            break;
        }
        case KeyCondition::RPNElement::ALWAYS_TRUE: {
            builder.literal(orc::TruthValue::YES);
            rpn_stack.pop_back();
            break;
        }
    }
}

std::unique_ptr<orc::SearchArgument> buildORCSearchArgument(
    const KeyCondition & key_condition, const Block & header, const orc::Type & schema, const FormatSettings & format_settings)
{
    auto rpn_stack = key_condition.getRPN();
    if (rpn_stack.empty())
        return nullptr;

    auto builder = orc::SearchArgumentFactory::newBuilder();
    buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, *builder, format_settings);
    return builder->build();
}

static void getFileReader(
    ReadBuffer & in,
    std::unique_ptr<orc::Reader> & file_reader,
    orc::MemoryPool & pool,
    const FormatSettings & format_settings,
    bool use_prefetch,
    size_t min_bytes_for_seek,
    std::atomic<int> & is_stopped)
{
    if (is_stopped)
        return;

    orc::ReaderOptions options;
    options.setMemoryPool(pool);
    options.setCacheOptions(orc::CacheOptions{.holeSizeLimit = min_bytes_for_seek, .rangeSizeLimit = 10 * 1024 * 1024UL});

    auto input_stream = asORCInputStream(in, format_settings, use_prefetch, is_stopped);
    file_reader = orc::createReader(std::move(input_stream), options);
}

static const orc::Type *
traverseDownORCTypeByName(const std::string & target, const orc::Type * orc_type, DataTypePtr & type, bool ignore_case)
{
    if (target.empty())
        return orc_type;

    auto search_struct_field = [&](const std::string & target_, const orc::Type * type_) -> std::pair<std::string, const orc::Type *>
    {
        auto target_copy = target_;
        if (ignore_case)
            boost::to_lower(target_copy);

        for (size_t i = 0; i < type_->getSubtypeCount(); ++i)
        {
            auto field_name = type_->getFieldName(i);
            if (ignore_case)
                boost::to_lower(field_name);

            if (startsWith(target_copy, field_name) && (target_copy.size() == field_name.size() || target_copy[field_name.size()] == '.'))
            {
                return {target_copy.size() == field_name.size() ? "" : target_.substr(field_name.size() + 1), type_->getSubtype(i)};
            }
        }
        return {"", nullptr};
    };

    if (orc::STRUCT == orc_type->getKind())
    {
        const auto [next_target, next_orc_type] = search_struct_field(target, orc_type);
        return next_orc_type ? traverseDownORCTypeByName(next_target, next_orc_type, type, ignore_case) : nullptr;
    }
    if (orc::LIST == orc_type->getKind())
    {
        /// For cases in which header contains subcolumns flattened from nested columns.
        /// For example, "a Nested(x String, y Int64)" is flattened to "a.x Array(String), a.y Array(Int64)", and ORC file schema is still "a array<struct<x string, y long>>".
        /// In this case, we should skip possible array type and traverse down to its nested struct type.
        const auto * array_type = typeid_cast<const DataTypeArray *>(removeNullable(type).get());
        const auto * orc_nested_type = orc_type->getSubtype(0);
        if (array_type && orc::STRUCT == orc_nested_type->getKind())
        {
            auto next_type_and_target = search_struct_field(target, orc_nested_type);
            const auto & next_target = next_type_and_target.first;
            const auto * next_orc_type = next_type_and_target.second;
            if (next_orc_type)
            {
                /// Adjust CH type to avoid inconsistency between CH and ORC type brought by flattened Nested type.
                type = array_type->getNestedType();
                return traverseDownORCTypeByName(next_target, next_orc_type, type, ignore_case);
            }
        }
    }
    return nullptr;
}

static void
updateIncludeTypeIds(DataTypePtr type, const orc::Type * orc_type, bool ignore_case, std::unordered_set<UInt64> & include_typeids)
{
    /// For primitive types, directly append column id into result
    if (orc_type->getSubtypeCount() == 0)
    {
        include_typeids.insert(orc_type->getColumnId());
        return;
    }

    auto non_nullable_type = removeNullable(type);
    switch (orc_type->getKind())
    {
        case orc::LIST: {
            const auto * array_type = typeid_cast<const DataTypeArray *>(non_nullable_type.get());
            if (array_type)
            {
                updateIncludeTypeIds(array_type->getNestedType(), orc_type->getSubtype(0), ignore_case, include_typeids);
            }
            return;
        }
        case orc::MAP: {
            const auto * map_type = typeid_cast<const DataTypeMap *>(non_nullable_type.get());
            if (map_type)
            {
                updateIncludeTypeIds(map_type->getKeyType(), orc_type->getSubtype(0), ignore_case, include_typeids);
                updateIncludeTypeIds(map_type->getValueType(), orc_type->getSubtype(1), ignore_case, include_typeids);
            }
            return;
        }
        case orc::STRUCT: {
            /// To make sure tuple field pruning work fine, we should include only the fields of ORC struct type which are also contained in CH tuple types, instead of all fields of ORC struct type.
            /// For example, CH tupe type in header is "x Tuple(a String)", ORC struct type is "x struct<a:string, b:long>", then only type id of field "x.a" should be included.
            /// For tuple field pruning purpose, we should never include "x.b" for it is not required in format header.
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(non_nullable_type.get());
            if (tuple_type)
            {
                if (tuple_type->hasExplicitNames())
                {
                    std::unordered_map<String, size_t> orc_field_name_to_index;
                    orc_field_name_to_index.reserve(orc_type->getSubtypeCount());
                    for (size_t struct_i = 0; struct_i < orc_type->getSubtypeCount(); ++struct_i)
                    {
                        String field_name = orc_type->getFieldName(struct_i);
                        if (ignore_case)
                            boost::to_lower(field_name);

                        orc_field_name_to_index[field_name] = struct_i;
                    }

                    const auto & element_names = tuple_type->getElementNames();
                    for (size_t tuple_i = 0; tuple_i < element_names.size(); ++tuple_i)
                    {
                        String element_name = element_names[tuple_i];
                        if (ignore_case)
                            boost::to_lower(element_name);

                        if (orc_field_name_to_index.contains(element_name))
                        {
                            updateIncludeTypeIds(
                                tuple_type->getElement(tuple_i),
                                orc_type->getSubtype(orc_field_name_to_index[element_name]),
                                ignore_case,
                                include_typeids);
                        }
                    }
                }
                else
                {
                    for (size_t i = 0; i < tuple_type->getElements().size() && i < orc_type->getSubtypeCount(); ++i)
                        updateIncludeTypeIds(tuple_type->getElement(i), orc_type->getSubtype(i), ignore_case, include_typeids);
                }
            }
            return;
        }
        default:
            return;
    }
}

NativeORCBlockInputFormat::NativeORCBlockInputFormat(
    ReadBuffer & in_, Block header_, const FormatSettings & format_settings_, bool use_prefetch_, size_t min_bytes_for_seek_)
    : IInputFormat(std::move(header_), &in_)
    , memory_pool(std::make_unique<MemoryPool>())
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
    , skip_stripes(format_settings.orc.skip_stripes)
    , use_prefetch(use_prefetch_)
    , min_bytes_for_seek(min_bytes_for_seek_)
{
}

void NativeORCBlockInputFormat::prepareFileReader()
{
    getFileReader(*in, file_reader, *memory_pool, format_settings, use_prefetch, min_bytes_for_seek, is_stopped);
    if (is_stopped)
        return;

    std::unique_ptr<orc::StripeInformation> stripe_info;
    if (file_reader->getNumberOfStripes())
        stripe_info = file_reader->getStripe(0);

    orc_column_to_ch_column = std::make_unique<ORCColumnToCHColumn>(
        getPort().getHeader(),
        format_settings.orc.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.orc.case_insensitive_column_matching,
        format_settings.orc.dictionary_as_low_cardinality);

    const bool ignore_case = format_settings.orc.case_insensitive_column_matching;
    const auto & header = getPort().getHeader();
    const auto & file_schema = file_reader->getType();
    std::unordered_set<UInt64> include_typeids;
    for (const auto & column : header)
    {
        auto adjusted_type = column.type;
        const auto * orc_type = traverseDownORCTypeByName(column.name, &file_schema, adjusted_type, ignore_case);
        if (orc_type)
            updateIncludeTypeIds(adjusted_type, orc_type, ignore_case, include_typeids);
    }
    include_indices.assign(include_typeids.begin(), include_typeids.end());

    if (format_settings.orc.filter_push_down && key_condition && !sargs)
        sargs = buildORCSearchArgument(*key_condition, getPort().getHeader(), file_reader->getType(), format_settings);

    selected_stripes = calculateSelectedStripes(static_cast<int>(file_reader->getNumberOfStripes()), skip_stripes);
    read_iterator = 0;
    prefetch_iterator = 0;

    if (use_prefetch)
        prefetchStripes();
}

void NativeORCBlockInputFormat::prefetchStripes()
{
    if (prefetch_iterator >= selected_stripes.size())
        return;

    size_t total_stripe_size = 0;
    std::vector<uint32_t> stripes;
    while (prefetch_iterator < selected_stripes.size() && total_stripe_size < min_bytes_for_seek)
    {
        int stripe = selected_stripes[prefetch_iterator];
        stripes.push_back(stripe);

        total_stripe_size += file_reader->getStripe(stripe)->getLength();
        ++prefetch_iterator;
    }

    Stopwatch time;
    file_reader->preBuffer(stripes, include_indices);
    LOG_TEST(
        getLogger("NativeORCBlockInputFormat"),
        "Prefetch {} stripes with {} columns and {} bytes takes {} ms",
        stripes.size(),
        include_indices.size(),
        total_stripe_size,
        time.elapsedMilliseconds());
}

std::vector<int> NativeORCBlockInputFormat::calculateSelectedStripes(int num_stripes, const std::unordered_set<int> & skip_stripes)
{
    std::vector<int> result;
    result.reserve(std::max<ssize_t>(num_stripes - skip_stripes.size(), 0));
    for (int stripe = 0; stripe < num_stripes; ++stripe)
    {
        if (skip_stripes.contains(stripe))
            continue;

        result.push_back(stripe);
    }
    return result;
}

bool NativeORCBlockInputFormat::prepareStripeReader()
{
    assert(file_reader);

    if (read_iterator >= selected_stripes.size())
        return false;

    int current_stripe = selected_stripes[read_iterator];
    current_stripe_info = file_reader->getStripe(current_stripe);
    if (!current_stripe_info->getNumberOfRows())
        throw Exception(ErrorCodes::INCORRECT_DATA, "ORC stripe {} has no rows", current_stripe);

    orc::RowReaderOptions row_reader_options;
    row_reader_options.setEnableLazyDecoding(format_settings.orc.dictionary_as_low_cardinality);
    row_reader_options.includeTypes(include_indices);
    row_reader_options.setTimezoneName(format_settings.orc.reader_time_zone_name);
    row_reader_options.range(current_stripe_info->getOffset(), current_stripe_info->getLength());
    if (format_settings.orc.filter_push_down && sargs)
    {
        row_reader_options.searchArgument(sargs);
    }
    stripe_reader = file_reader->createRowReader(row_reader_options);
    ++read_iterator;

    if (use_prefetch)
    {
        /// Release outdated buffer before boundary to avoid OOM
        file_reader->releaseBuffer(current_stripe_info->getOffset());

        /// Prefetch next selected stripe
        prefetchStripes();
    }

    return true;
}

Chunk NativeORCBlockInputFormat::read()
{
    block_missing_values.clear();

    if (!file_reader)
        prepareFileReader();

    if (need_only_count)
    {
        if (read_iterator >= selected_stripes.size())
            return {};

        int current_stripe = selected_stripes[read_iterator];
        ++read_iterator;
        return getChunkForCount(file_reader->getStripe(current_stripe)->getNumberOfRows());
    }

    if (!stripe_reader)
    {
        if (!prepareStripeReader())
            return {};
    }

    if (is_stopped)
        return {};

    /// TODO: figure out why reuse batch would cause asan fatals in https://s3.amazonaws.com/clickhouse-test-reports/55330/be39d23af2d7e27f5ec7f168947cf75aeaabf674/stateless_tests__asan__[4_4].html
    /// Not sure if it is a false positive case. Notice that reusing batch will speed up reading ORC by 1.15x.
    Stopwatch time;
    auto batch = stripe_reader->createRowBatch(format_settings.orc.row_batch_size);
    while (true)
    {
        bool ok = stripe_reader->next(*batch);
        if (ok)
            break;

        /// No more rows to read in current stripe, continue to prepare reading next stripe
        if (!prepareStripeReader())
            return {};
    }

    Chunk res;
    size_t num_rows = batch->numElements;
    LOG_TEST(getLogger("NativeORCBlockInputFormat"), "Read {} rows take {} ms", num_rows, time.elapsedMilliseconds());

    time.restart();
    const auto & schema = stripe_reader->getSelectedType();
    orc_column_to_ch_column->orcTableToCHChunk(res, &schema, batch.get(), num_rows, &block_missing_values);
    LOG_TEST(getLogger("NativeORCBlockInputFormat"), "Convert {} rows take {} ms", num_rows, time.elapsedMilliseconds());

    approx_bytes_read_for_chunk = num_rows * current_stripe_info->getLength() / current_stripe_info->getNumberOfRows();
    return res;
}

void NativeORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    stripe_reader.reset();
    include_indices.clear();
    sargs.reset();
    block_missing_values.clear();
}

const BlockMissingValues * NativeORCBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}

NativeORCSchemaReader::NativeORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList NativeORCSchemaReader::readSchema()
{
    std::unique_ptr<orc::Reader> file_reader;
    std::atomic<int> is_stopped = 0;
    MemoryPool memory_pool;
    getFileReader(in, file_reader, memory_pool, format_settings, false, 0, is_stopped);

    const auto & schema = file_reader->getType();
    Block header;
    std::unique_ptr<orc::StripeInformation> stripe_info;
    if (file_reader->getNumberOfStripes())
        stripe_info = file_reader->getStripe(0);

    for (size_t i = 0; i < schema.getSubtypeCount(); ++i)
    {
        const std::string & name = schema.getFieldName(i);
        const orc::Type * orc_type = schema.getSubtype(i);

        bool skipped = false;
        DataTypePtr type = parseORCType(
            orc_type,
            format_settings.orc.skip_columns_with_unsupported_types_in_schema_inference,
            format_settings.orc.dictionary_as_low_cardinality,
            stripe_info.get(),
            skipped);
        if (!skipped)
            header.insert(ColumnWithTypeAndName{type, name});
    }

    if (format_settings.schema_inference_make_columns_nullable == 1)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

ORCColumnToCHColumn::ORCColumnToCHColumn(
    const Block & header_,
    bool allow_missing_columns_,
    bool null_as_default_,
    bool case_insensitive_matching_,
    bool dictionary_as_low_cardinality_)
    : header(header_)
    , allow_missing_columns(allow_missing_columns_)
    , null_as_default(null_as_default_)
    , case_insensitive_matching(case_insensitive_matching_)
    , dictionary_as_low_cardinality(dictionary_as_low_cardinality_)
{
}

void ORCColumnToCHColumn::orcTableToCHChunk(
    Chunk & res, const orc::Type * schema, const orc::ColumnVectorBatch * table, size_t num_rows, BlockMissingValues * block_missing_values)
{
    const auto * struct_batch = dynamic_cast<const orc::StructVectorBatch *>(table);
    if (!struct_batch)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table must be StructVectorBatch but is {}", struct_batch->toString());

    if (schema->getSubtypeCount() != struct_batch->fields.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "ORC table has {} fields but schema has {}", struct_batch->fields.size(), schema->getSubtypeCount());

    size_t field_num = struct_batch->fields.size();
    NameToColumnPtr name_to_column_ptr;
    for (size_t i = 0; i < field_num; ++i)
    {
        auto name = schema->getFieldName(i);
        const auto * field = struct_batch->fields[i];
        if (!field)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table field {} is null", name);

        if (case_insensitive_matching)
            boost::to_lower(name);

        name_to_column_ptr[std::move(name)] = {field, schema->getSubtype(i)};
    }

    orcColumnsToCHChunk(res, name_to_column_ptr, num_rows, block_missing_values);
}

/// Creates a null bytemap from ORC's not-null bytemap
static ColumnPtr readByteMapFromORCColumn(const orc::ColumnVectorBatch * orc_column)
{
    if (!orc_column->hasNulls)
        return ColumnUInt8::create(orc_column->numElements, 0);

    auto nullmap_column = ColumnUInt8::create();
    PaddedPODArray<UInt8> & bytemap_data = assert_cast<ColumnVector<UInt8> &>(*nullmap_column).getData();
    bytemap_data.resize(orc_column->numElements);

    for (size_t i = 0; i < orc_column->numElements; ++i)
        bytemap_data[i] = 1 - orc_column->notNull[i];
    return nullmap_column;
}


static const orc::ColumnVectorBatch * getNestedORCColumn(const orc::ListVectorBatch * orc_column)
{
    return orc_column->elements.get();
}

template <typename BatchType>
static ColumnPtr readOffsetsFromORCListColumn(const BatchType * orc_column)
{
    auto offsets_column = ColumnUInt64::create();
    ColumnArray::Offsets & offsets_data = assert_cast<ColumnVector<UInt64> &>(*offsets_column).getData();
    offsets_data.reserve(orc_column->numElements);

    for (size_t i = 0; i < orc_column->numElements; ++i)
        offsets_data.push_back(orc_column->offsets[i + 1]);

    return offsets_column;
}

static ColumnWithTypeAndName
readColumnWithBooleanData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    const auto * orc_bool_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);
    auto internal_type = DataTypeFactory::instance().get("Bool");
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnVector<UInt8> &>(*internal_column).getData();
    column_data.reserve(orc_bool_column->numElements);

    for (size_t i = 0; i < orc_bool_column->numElements; ++i)
        column_data.push_back(static_cast<UInt8>(orc_bool_column->data[i]));

    return {std::move(internal_column), internal_type, column_name};
}

/// Inserts numeric data right into internal column data to reduce an overhead
template <typename NumericType, typename BatchType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName
readColumnWithNumericData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(orc_column->numElements);

    const auto * orc_int_column = dynamic_cast<const BatchType *>(orc_column);
    column_data.insert_assume_reserved(orc_int_column->data.data(), orc_int_column->data.data() + orc_int_column->numElements);

    return {std::move(internal_column), internal_type, column_name};
}

template <typename NumericType, typename BatchType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName
readColumnWithNumericDataCast(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(orc_column->numElements);

    const auto * orc_int_column = dynamic_cast<const BatchType *>(orc_column);
    for (size_t i = 0; i < orc_int_column->numElements; ++i)
        column_data.push_back(static_cast<NumericType>(orc_int_column->data[i]));

    return {std::move(internal_column), internal_type, column_name};
}

template <bool fixed_string>
static ColumnWithTypeAndName readColumnWithEncodedStringOrFixedStringData(
    const orc::ColumnVectorBatch * orc_column, const orc::Type * orc_type, const String & column_name, bool nullable)
{
    /// Fill CH holder_column with ORC dictionary
    /// Note that holder_column is always a ColumnString or ColumnFixedstring whether nullable is true or false, because ORC dictionary doesn't contain null values.
    DataTypePtr holder_type;
    if constexpr (fixed_string)
        holder_type = std::make_shared<DataTypeFixedString>(orc_type->getMaximumLength());
    else
        holder_type = std::make_shared<DataTypeString>();

    DataTypePtr nested_type = nullable ? std::make_shared<DataTypeNullable>(holder_type) : holder_type;
    auto internal_type = std::make_shared<DataTypeLowCardinality>(std::move(nested_type));

    const auto & orc_str_column = dynamic_cast<const orc::EncodedStringVectorBatch &>(*orc_column);
    size_t rows = orc_str_column.numElements;
    const auto & orc_dict = *orc_str_column.dictionary;
    if (orc_dict.dictionaryOffset.size() <= 1)
        return {internal_type->createColumn(), internal_type, column_name};

    size_t dict_size = orc_dict.dictionaryOffset.size() - 1;
    auto holder_column = holder_type->createColumn();
    if constexpr (fixed_string)
    {
        const size_t n = orc_type->getMaximumLength();
        auto & concrete_holder_column = assert_cast<ColumnFixedString &>(*holder_column);
        PaddedPODArray<UInt8> & column_chars_t = concrete_holder_column.getChars();
        size_t reserve_size = dict_size * n;
        column_chars_t.resize_exact(reserve_size);
        size_t curr_offset = 0;
        for (size_t i = 0; i < dict_size; ++i)
        {
            const auto * buf = orc_dict.dictionaryBlob.data() + orc_dict.dictionaryOffset[i];
            size_t buf_size = orc_dict.dictionaryOffset[i + 1] - orc_dict.dictionaryOffset[i];
            memcpy(&column_chars_t[curr_offset], buf, buf_size);
            curr_offset += n;
        }
    }
    else
    {
        auto & concrete_holder_column = assert_cast<ColumnString &>(*holder_column);
        PaddedPODArray<UInt8> & column_chars_t = concrete_holder_column.getChars();
        PaddedPODArray<UInt64> & column_offsets = concrete_holder_column.getOffsets();

        size_t reserve_size = orc_dict.dictionaryBlob.size() + dict_size;
        column_chars_t.resize_exact(reserve_size);
        column_offsets.resize_exact(dict_size);
        size_t curr_offset = 0;
        for (size_t i = 0; i < dict_size; ++i)
        {
            const auto * buf = orc_dict.dictionaryBlob.data() + orc_dict.dictionaryOffset[i];
            size_t buf_size = orc_dict.dictionaryOffset[i + 1] - orc_dict.dictionaryOffset[i];
            memcpy(&column_chars_t[curr_offset], buf, buf_size);
            curr_offset += buf_size;

            column_chars_t[curr_offset] = 0;
            ++curr_offset;

            column_offsets[i] = curr_offset;
        }
    }

    /// Insert CH dictionary_column from holder_column
    auto tmp_internal_column = internal_type->createColumn();
    auto dictionary_column = IColumn::mutate(assert_cast<ColumnLowCardinality *>(tmp_internal_column.get())->getDictionaryPtr());
    auto index_column
        = dynamic_cast<IColumnUnique *>(dictionary_column.get())->uniqueInsertRangeFrom(*holder_column, 0, holder_column->size());

    /// Fill index_column and wrap it with LowCardinality
    auto call_by_type = [&](auto index_type) -> MutableColumnPtr
    {
        using IndexType = decltype(index_type);
        const ColumnVector<IndexType> * concrete_index_column = checkAndGetColumn<ColumnVector<IndexType>>(index_column.get());
        if (!concrete_index_column)
            return nullptr;

        const auto & index_data = concrete_index_column->getData();
        auto new_index_column = ColumnVector<IndexType>::create(rows);
        auto & new_index_data = dynamic_cast<ColumnVector<IndexType> &>(*new_index_column).getData();

        if (!orc_str_column.hasNulls)
        {
            for (size_t i = 0; i < rows; ++i)
            {
                /// First map row index to ORC dictionary index, then map ORC dictionary index to CH dictionary index
                new_index_data[i] = index_data[orc_str_column.index[i]];
            }
        }
        else
        {
            for (size_t i = 0; i < rows; ++i)
            {
                /// Set index 0 if we meet null value. If dictionary_column is nullable, 0 represents null value.
                /// Otherwise 0 represents default string value, it is reasonable because null values are converted to default values when casting nullable column to non-nullable.
                new_index_data[i] = orc_str_column.notNull[i] ? index_data[orc_str_column.index[i]] : 0;
            }
        }

        return ColumnLowCardinality::create(std::move(dictionary_column), std::move(new_index_column));
    };

    MutableColumnPtr internal_column;
    if (!internal_column)
        internal_column = call_by_type(UInt8());
    if (!internal_column)
        internal_column = call_by_type(UInt16());
    if (!internal_column)
        internal_column = call_by_type(UInt32());
    if (!internal_column)
        internal_column = call_by_type(UInt64());
    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName
readColumnWithStringData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeString>();
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
    size_t reserver_size = 0;
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
            reserver_size += orc_str_column->length[i];
        reserver_size += 1;
    }

    column_chars_t.resize_exact(reserver_size);
    column_offsets.resize_exact(orc_str_column->numElements);

    size_t curr_offset = 0;
    if (!orc_str_column->hasNulls)
    {
        for (size_t i = 0; i < orc_str_column->numElements; ++i)
        {
            const auto * buf = orc_str_column->data[i];
            size_t buf_size = orc_str_column->length[i];
            memcpy(&column_chars_t[curr_offset], buf, buf_size);
            curr_offset += buf_size;

            column_chars_t[curr_offset] = 0;
            ++curr_offset;

            column_offsets[i] = curr_offset;
        }
    }
    else
    {
        for (size_t i = 0; i < orc_str_column->numElements; ++i)
        {
            if (orc_str_column->notNull[i])
            {
                const auto * buf = orc_str_column->data[i];
                size_t buf_size = orc_str_column->length[i];
                memcpy(&column_chars_t[curr_offset], buf, buf_size);
                curr_offset += buf_size;
            }

            column_chars_t[curr_offset] = 0;
            ++curr_offset;

            column_offsets[i] = curr_offset;
        }
    }
    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName
readColumnWithFixedStringData(const orc::ColumnVectorBatch * orc_column, const orc::Type * orc_type, const String & column_name)
{
    size_t fixed_len = orc_type->getMaximumLength();
    auto internal_type = std::make_shared<DataTypeFixedString>(fixed_len);
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnFixedString &>(*internal_column).getChars();
    column_chars_t.reserve(orc_column->numElements * fixed_len);

    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
            column_chars_t.insert_assume_reserved(orc_str_column->data[i], orc_str_column->data[i] + orc_str_column->length[i]);
        else
            column_chars_t.resize_fill(column_chars_t.size() + fixed_len);
    }

    return {std::move(internal_column), internal_type, column_name};
}


template <typename DecimalType, typename BatchType, typename VectorType = ColumnDecimal<DecimalType>>
static ColumnWithTypeAndName readColumnWithDecimalDataCast(
    const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name, DataTypePtr internal_type)
{
    using NativeType = typename DecimalType::NativeType;
    static_assert(std::is_same_v<BatchType, orc::Decimal128VectorBatch> || std::is_same_v<BatchType, orc::Decimal64VectorBatch>);

    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(orc_column->numElements);

    const auto * orc_decimal_column = dynamic_cast<const BatchType *>(orc_column);
    for (size_t i = 0; i < orc_decimal_column->numElements; ++i)
    {
        DecimalType decimal_value;
        if constexpr (std::is_same_v<BatchType, orc::Decimal128VectorBatch>)
        {
            Int128 int128_value;
            int128_value.items[0] = orc_decimal_column->values[i].getLowBits();
            int128_value.items[1] = orc_decimal_column->values[i].getHighBits();
            decimal_value.value = static_cast<NativeType>(int128_value);
        }
        else
            decimal_value.value = static_cast<NativeType>(orc_decimal_column->values[i]);

        column_data.push_back(std::move(decimal_value));
    }

    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName
readIPv6ColumnFromBinaryData(const orc::ColumnVectorBatch * orc_column, const orc::Type * orc_type, const String & column_name)
{
    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);

    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        /// If at least one value size is not 16 bytes, fallback to reading String column and further cast to IPv6.
        if ((!orc_str_column->hasNulls || orc_str_column->notNull[i]) && orc_str_column->length[i] != sizeof(IPv6))
            return readColumnWithStringData(orc_column, orc_type, column_name);
    }

    auto internal_type = std::make_shared<DataTypeIPv6>();
    auto internal_column = internal_type->createColumn();
    auto & ipv6_column = assert_cast<ColumnIPv6 &>(*internal_column);
    ipv6_column.reserve(orc_str_column->numElements);

    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
            ipv6_column.insertData(orc_str_column->data[i], orc_str_column->length[i]);
        else
            ipv6_column.insertDefault();
    }

    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName
readIPv4ColumnWithInt32Data(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    const auto * orc_int_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);

    auto internal_type = std::make_shared<DataTypeIPv4>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnIPv4 &>(*internal_column).getData();
    column_data.reserve(orc_int_column->numElements);

    for (size_t i = 0; i < orc_int_column->numElements; ++i)
        column_data.push_back(static_cast<UInt32>(orc_int_column->data[i]));

    return {std::move(internal_column), internal_type, column_name};
}

template <typename ColumnType>
static ColumnWithTypeAndName readColumnWithBigNumberFromBinaryData(
    const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name, const DataTypePtr & column_type)
{
    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);

    auto internal_column = column_type->createColumn();
    auto & integer_column = assert_cast<ColumnType &>(*internal_column);
    integer_column.reserve(orc_str_column->numElements);

    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
        {
            if (sizeof(typename ColumnType::ValueType) != orc_str_column->length[i])
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "ValueType size {} of column {} is not equal to size of binary data {}",
                    sizeof(typename ColumnType::ValueType),
                    integer_column.getName(),
                    orc_str_column->length[i]);

            integer_column.insertData(orc_str_column->data[i], orc_str_column->length[i]);
        }
        else
        {
            integer_column.insertDefault();
        }
    }
    return {std::move(internal_column), column_type, column_name};
}

static ColumnWithTypeAndName readColumnWithDateData(
    const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name, const DataTypePtr & type_hint)
{
    DataTypePtr internal_type;
    bool check_date_range = false;
    /// Make result type Date32 when requested type is actually Date32 or when we use schema inference
    if (!type_hint || (type_hint && isDate32(*type_hint)))
    {
        internal_type = std::make_shared<DataTypeDate32>();
        check_date_range = true;
    }
    else
    {
        internal_type = std::make_shared<DataTypeInt32>();
    }

    const auto * orc_int_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<Int32> & column_data = assert_cast<ColumnVector<Int32> &>(*internal_column).getData();
    column_data.reserve(orc_int_column->numElements);

    for (size_t i = 0; i < orc_int_column->numElements; ++i)
    {
        if (!orc_int_column->hasNulls || orc_int_column->notNull[i])
        {
            Int32 days_num = static_cast<Int32>(orc_int_column->data[i]);
            if (check_date_range && (days_num > DATE_LUT_MAX_EXTEND_DAY_NUM || days_num < -DAYNUM_OFFSET_EPOCH))
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                    "Input value {} of a column \"{}\" exceeds the range of type Date32",
                    days_num,
                    column_name);

            column_data.push_back(days_num);
        }
        else
        {
            /// ORC library doesn't guarantee that orc_int_column->data[i] is initialized to zero when orc_int_column->notNull[i] is false since https://github.com/ClickHouse/ClickHouse/pull/69473
            column_data.push_back(0);
        }
    }

    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName
readColumnWithTimestampData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    const auto * orc_ts_column = dynamic_cast<const orc::TimestampVectorBatch *>(orc_column);

    auto internal_type = std::make_shared<DataTypeDateTime64>(9);
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnDateTime64 &>(*internal_column).getData();
    column_data.reserve(orc_ts_column->numElements);

    constexpr Int64 multiplier = 1e9L;
    for (size_t i = 0; i < orc_ts_column->numElements; ++i)
    {
        Decimal64 decimal64;
        decimal64.value = orc_ts_column->data[i] * multiplier + orc_ts_column->nanoseconds[i];
        column_data.emplace_back(std::move(decimal64));
    }
    return {std::move(internal_column), internal_type, column_name};
}

ColumnWithTypeAndName ORCColumnToCHColumn::readColumnFromORCColumn(
    const orc::ColumnVectorBatch * orc_column,
    const orc::Type * orc_type,
    const std::string & column_name,
    bool inside_nullable,
    DataTypePtr type_hint) const
{
    bool skipped = false;

    if (!inside_nullable && (orc_column->hasNulls || (type_hint && type_hint->isNullable())) && !orc_column->isEncoded
        && (orc_type->getKind() != orc::LIST && orc_type->getKind() != orc::MAP && orc_type->getKind() != orc::STRUCT))
    {
        DataTypePtr nested_type_hint;
        if (type_hint)
            nested_type_hint = removeNullable(type_hint);

        auto nested_column = readColumnFromORCColumn(orc_column, orc_type, column_name, true, nested_type_hint);

        auto nullmap_column = readByteMapFromORCColumn(orc_column);
        auto nullable_type = std::make_shared<DataTypeNullable>(std::move(nested_column.type));
        auto nullable_column = ColumnNullable::create(nested_column.column, nullmap_column);
        return {nullable_column, nullable_type, column_name};
    }

    switch (orc_type->getKind())
    {
        case orc::STRING:
        case orc::BINARY:
        case orc::VARCHAR: {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    case TypeIndex::IPv6:
                        return readIPv6ColumnFromBinaryData(orc_column, orc_type, column_name);
                    /// ORC format outputs big integers as binary column, because there is no fixed binary in ORC.
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(orc_column, orc_type, column_name, type_hint);
                    /// ORC doesn't support Decimal256 as separate type. We read and write it as binary data.
                    case TypeIndex::Decimal256:
                        return readColumnWithBigNumberFromBinaryData<ColumnDecimal<Decimal256>>(
                            orc_column, orc_type, column_name, type_hint);
                    default:;
                }
            }

            if (orc_column->isEncoded && dictionary_as_low_cardinality)
            {
                bool nullable = type_hint ? isNullableOrLowCardinalityNullable(type_hint) : true;
                return readColumnWithEncodedStringOrFixedStringData<false>(orc_column, orc_type, column_name, nullable);
            }
            else
                return readColumnWithStringData(orc_column, orc_type, column_name);
        }
        case orc::CHAR: {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(orc_column, orc_type, column_name, type_hint);
                    default:;
                }
            }

            if (orc_column->isEncoded && dictionary_as_low_cardinality)
            {
                bool nullable = type_hint ? isNullableOrLowCardinalityNullable(type_hint) : true;
                return readColumnWithEncodedStringOrFixedStringData<true>(orc_column, orc_type, column_name, nullable);
            }
            else
                return readColumnWithFixedStringData(orc_column, orc_type, column_name);
        }
        case orc::BOOLEAN:
            return readColumnWithBooleanData(orc_column, orc_type, column_name);
        case orc::BYTE:
            return readColumnWithNumericDataCast<Int8, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::SHORT:
            return readColumnWithNumericDataCast<Int16, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::INT: {
            /// ORC format doesn't have unsigned integers and we output IPv4 as Int32.
            /// We should allow to read it back from Int32.
            if (type_hint && isIPv4(type_hint))
                return readIPv4ColumnWithInt32Data(orc_column, orc_type, column_name);
            return readColumnWithNumericDataCast<Int32, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        }
        case orc::LONG:
            return readColumnWithNumericData<Int64, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::FLOAT:
            return readColumnWithNumericDataCast<Float32, orc::DoubleVectorBatch>(orc_column, orc_type, column_name);
        case orc::DOUBLE:
            return readColumnWithNumericData<Float64, orc::DoubleVectorBatch>(orc_column, orc_type, column_name);
        case orc::DATE:
            return readColumnWithDateData(orc_column, orc_type, column_name, type_hint);
        case orc::TIMESTAMP:
            [[fallthrough]];
        case orc::TIMESTAMP_INSTANT:
            return readColumnWithTimestampData(orc_column, orc_type, column_name);
        case orc::DECIMAL: {
            auto interal_type = parseORCType(orc_type, false, false, nullptr, skipped);

            auto precision = orc_type->getPrecision();
            if (precision == 0)
                precision = 38;

            if (precision <= DecimalUtils::max_precision<Decimal32>)
                return readColumnWithDecimalDataCast<Decimal32, orc::Decimal64VectorBatch>(orc_column, orc_type, column_name, interal_type);
            if (precision <= DecimalUtils::max_precision<Decimal64>)
                return readColumnWithDecimalDataCast<Decimal64, orc::Decimal64VectorBatch>(orc_column, orc_type, column_name, interal_type);
            if (precision <= DecimalUtils::max_precision<Decimal128>)
                return readColumnWithDecimalDataCast<Decimal128, orc::Decimal128VectorBatch>(
                    orc_column, orc_type, column_name, interal_type);
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Decimal precision {} in ORC type {} is out of bound", precision, orc_type->toString());
        }
        case orc::MAP: {
            DataTypePtr key_type_hint;
            DataTypePtr value_type_hint;
            if (type_hint)
            {
                const auto * map_type_hint = typeid_cast<const DataTypeMap *>(type_hint.get());
                if (map_type_hint)
                {
                    key_type_hint = map_type_hint->getKeyType();
                    value_type_hint = map_type_hint->getValueType();
                }
            }

            const auto * orc_map_column = dynamic_cast<const orc::MapVectorBatch *>(orc_column);
            const auto * orc_key_column = orc_map_column->keys.get();
            const auto * orc_value_column = orc_map_column->elements.get();
            const auto * orc_key_type = orc_type->getSubtype(0);
            const auto * orc_value_type = orc_type->getSubtype(1);

            auto key_column = readColumnFromORCColumn(orc_key_column, orc_key_type, "key", false, key_type_hint);
            if (key_type_hint && !key_type_hint->equals(*key_column.type))
            {
                /// Cast key column to target type, because it can happen
                /// that parsed type cannot be ClickHouse Map key type.
                key_column.column = castColumn(key_column, key_type_hint);
                key_column.type = key_type_hint;
            }

            auto value_column = readColumnFromORCColumn(orc_value_column, orc_value_type, "value", false, value_type_hint);
            if (skipped)
                return {};

            auto offsets_column = readOffsetsFromORCListColumn(orc_map_column);
            auto map_column = ColumnMap::create(key_column.column, value_column.column, offsets_column);
            auto map_type = std::make_shared<DataTypeMap>(key_column.type, value_column.type);
            return {map_column, map_type, column_name};
        }
        case orc::LIST: {
            DataTypePtr nested_type_hint;
            if (type_hint)
            {
                const auto * array_type_hint = typeid_cast<const DataTypeArray *>(type_hint.get());
                if (array_type_hint)
                    nested_type_hint = array_type_hint->getNestedType();
            }

            const auto * orc_list_column = dynamic_cast<const orc::ListVectorBatch *>(orc_column);
            const auto * orc_nested_column = getNestedORCColumn(orc_list_column);
            const auto * orc_nested_type = orc_type->getSubtype(0);
            auto nested_column = readColumnFromORCColumn(orc_nested_column, orc_nested_type, column_name, false, nested_type_hint);

            auto offsets_column = readOffsetsFromORCListColumn(orc_list_column);
            auto array_column = ColumnArray::create(nested_column.column, offsets_column);
            DataTypePtr array_type;
            /// If type hint is Nested, we should return Nested type,
            /// because we differentiate Nested and simple Array(Tuple)
            if (type_hint && isNested(type_hint))
            {
                const auto & tuple_type = assert_cast<const DataTypeTuple &>(*nested_column.type);
                array_type = createNested(tuple_type.getElements(), tuple_type.getElementNames());
            }
            else
            {
                array_type = std::make_shared<DataTypeArray>(nested_column.type);
            }
            return {array_column, array_type, column_name};
        }
        case orc::STRUCT: {
            Columns tuple_elements;
            DataTypes tuple_types;
            std::vector<String> tuple_names;

            const auto * tuple_type_hint = type_hint ? typeid_cast<const DataTypeTuple *>(type_hint.get()) : nullptr;
            const auto * orc_struct_column = dynamic_cast<const orc::StructVectorBatch *>(orc_column);
            for (size_t i = 0; i < orc_type->getSubtypeCount(); ++i)
            {
                auto field_name = orc_type->getFieldName(i);

                DataTypePtr nested_type_hint;
                if (tuple_type_hint)
                {
                    if (tuple_type_hint->hasExplicitNames())
                    {
                        auto pos = tuple_type_hint->tryGetPositionByName(field_name, case_insensitive_matching);
                        if (pos)
                        {
                            nested_type_hint = tuple_type_hint->getElement(*pos);
                            field_name = tuple_type_hint->getNameByPosition(*pos + 1);
                        }
                    }
                    else if (i < tuple_type_hint->getElements().size())
                        nested_type_hint = tuple_type_hint->getElement(i);
                }

                const auto * nested_orc_column = orc_struct_column->fields[i];
                const auto * nested_orc_type = orc_type->getSubtype(i);
                auto element = readColumnFromORCColumn(nested_orc_column, nested_orc_type, field_name, false, nested_type_hint);

                tuple_elements.emplace_back(std::move(element.column));
                tuple_types.emplace_back(std::move(element.type));
                tuple_names.emplace_back(std::move(element.name));
            }

            ColumnPtr tuple_column;
            if (tuple_elements.empty())
                tuple_column = ColumnTuple::create(orc_column->numElements);
            else
                tuple_column = ColumnTuple::create(std::move(tuple_elements));
            auto tuple_type = std::make_shared<DataTypeTuple>(std::move(tuple_types), std::move(tuple_names));
            return {tuple_column, tuple_type, column_name};
        }
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE, "Unsupported ORC type {} while reading column {}.", orc_type->toString(), column_name);
    }
}

void ORCColumnToCHColumn::orcColumnsToCHChunk(
    Chunk & res, NameToColumnPtr & name_to_column_ptr, size_t num_rows, BlockMissingValues * block_missing_values)
{
    Columns columns_list;
    columns_list.reserve(header.columns());
    std::unordered_map<String, std::pair<BlockPtr, std::shared_ptr<NestedColumnExtractHelper>>> nested_tables;
    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);

        auto search_column_name = header_column.name;
        if (case_insensitive_matching)
            boost::to_lower(search_column_name);

        ColumnWithTypeAndName column;
        if (!name_to_column_ptr.contains(search_column_name))
        {
            bool read_from_nested = false;

            /// Check if it's a column from nested table.
            String nested_table_name = Nested::extractTableName(header_column.name);
            String search_nested_table_name = nested_table_name;
            if (case_insensitive_matching)
                boost::to_lower(search_nested_table_name);
            if (name_to_column_ptr.contains(search_nested_table_name))
            {
                if (!nested_tables.contains(search_nested_table_name))
                {
                    NamesAndTypesList nested_columns;
                    for (const auto & name_and_type : header.getNamesAndTypesList())
                    {
                        if (name_and_type.name.starts_with(nested_table_name + "."))
                            nested_columns.push_back(name_and_type);
                    }
                    auto nested_table_type = Nested::collect(nested_columns).front().type;

                    auto orc_column_with_type = name_to_column_ptr[search_nested_table_name];
                    ColumnsWithTypeAndName cols = {readColumnFromORCColumn(
                        orc_column_with_type.first, orc_column_with_type.second, nested_table_name, false, nested_table_type)};
                    BlockPtr block_ptr = std::make_shared<Block>(cols);
                    auto column_extractor = std::make_shared<NestedColumnExtractHelper>(*block_ptr, case_insensitive_matching);
                    nested_tables[search_nested_table_name] = {block_ptr, column_extractor};
                }

                auto nested_column = nested_tables[search_nested_table_name].second->extractColumn(search_column_name);
                if (nested_column)
                {
                    column = *nested_column;
                    if (case_insensitive_matching)
                        column.name = header_column.name;
                    read_from_nested = true;
                }
            }

            if (!read_from_nested)
            {
                if (!allow_missing_columns)
                    throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name};

                column.name = header_column.name;
                column.type = header_column.type;
                column.column = header_column.column->cloneResized(num_rows);
                columns_list.push_back(std::move(column.column));
                if (block_missing_values)
                    block_missing_values->setBits(column_i, num_rows);
                continue;
            }
        }
        else
        {
            auto orc_column_with_type = name_to_column_ptr[search_column_name];
            column = readColumnFromORCColumn(
                orc_column_with_type.first, orc_column_with_type.second, header_column.name, false, header_column.type);
        }

        if (null_as_default)
            insertNullAsDefaultIfNeeded(column, header_column, column_i, block_missing_values);

        try
        {
            column.column = castColumn(column, header_column.type);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format(
                "while converting column {} from type {} to type {}",
                backQuote(header_column.name),
                column.type->getName(),
                header_column.type->getName()));
            throw;
        }

        column.type = header_column.type;
        columns_list.push_back(std::move(column.column));
    }

    res.setColumns(columns_list, num_rows);
}

}

#endif
