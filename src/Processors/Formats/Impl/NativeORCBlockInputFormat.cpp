#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>

#if USE_ORC

#include <Common/checkStackSize.h>
#include <Core/Defines.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Formats/insertNullAsDefaultIfNeeded.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <IO/WithFileSize.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Set.h>
#include <Interpreters/castColumn.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <orc/Vector.hh>
#include <orc/Exceptions.hh>
#include <Common/DateLUTImpl.h>
#include <Common/setThreadName.h>
#include <Common/Allocator.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <base/arithmeticOverflow.h>
#include <Common/memory.h>
#include <Common/AllocationInterceptors.h>

#include <Processors/Formats/Impl/ArrowBufferedStreams.h>

#include <boost/algorithm/string.hpp>

#include <unordered_set>

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
extern const int TOO_DEEP_RECURSION;
}


ORCInputStream::ORCInputStream(SeekableReadBuffer & in_, size_t file_size_, bool use_prefetch)
    : in(in_)
    , file_size(file_size_)
    , use_offset_based_read(in_.supportsReadAt())
    , use_async_prefetch(use_prefetch && use_offset_based_read)
{
    if (use_async_prefetch)
        async_runner = threadPoolCallbackRunnerUnsafe<void>(getIOThreadPool().get(), ThreadName::ORC_FILE);
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
    if (use_offset_based_read)
    {
        size_t bytes_read = 0;
        while (bytes_read < length)
        {
            size_t bytes_to_read = length - bytes_read;
            size_t n = in.readBigAt(reinterpret_cast<char *>(buf) + bytes_read, bytes_to_read, offset + bytes_read, nullptr);
            if (n == 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Truncated or corrupted ORC input: readBigAt returned 0 bytes at offset {} ({} bytes remaining of {} requested from base offset {})",
                    offset + bytes_read,
                    bytes_to_read,
                    length,
                    offset);
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
    if (use_async_prefetch)
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
    bool & skipped,
    size_t max_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH,
    size_t depth = 0)
{
    chassert(orc_type != nullptr);

    /// ORC LIST/MAP/STRUCT types can be nested arbitrarily deep and the ORC library does not bound
    /// the nesting, so reject deep nesting early (before building the type) with an explicit limit.
    /// This keeps schema inference cheap and interruptible instead of recursing over (and later
    /// walking) a pathologically deep type. checkStackSize is a last-resort backstop.
    /// max_depth == 0 means unlimited (matching the SQL parser), leaving only checkStackSize.
    if (max_depth != 0 && depth > max_depth)
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Too deep recursion while parsing the ORC schema: the nesting depth exceeds the limit ({}). "
            "It can be raised with the setting 'max_parser_depth', but a very deep schema is rarely intentional",
            max_depth);
    checkStackSize();

    const int subtype_count = static_cast<int>(orc_type->getSubtypeCount());

    /// ORC union maps to the ClickHouse Variant type. It is handled before the switch below so the
    /// switch stays non-exhaustive (its default handles unsupported types). Variant sorts and
    /// de-duplicates its nested types, but ORC keeps a separate physical stream per branch, so two
    /// branches with identical types cannot be represented as a Variant; reject them explicitly
    /// instead of silently squashing them. A branch that is itself a union is rejected too: it
    /// would map to a Variant, which Variant does not allow to nest.
    if (orc_type->getKind() == orc::TypeKind::UNION)
    {
        /// A union with more branches than Variant can hold (ColumnVariant::MAX_NESTED_COLUMNS)
        /// is valid ORC but not representable; reject it through the normal unsupported-type /
        /// skip path before any branch is parsed - otherwise the DataTypeVariant constructor
        /// throws a generic BAD_ARGUMENTS and the skip setting is never consulted.
        if (static_cast<size_t>(subtype_count) > ColumnVariant::MAX_NESTED_COLUMNS)
        {
            if (skip_columns_with_unsupported_types)
            {
                skipped = true;
                return {};
            }
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "ORC union type with {} branches is not supported: Variant supports at most {} nested types",
                subtype_count, ColumnVariant::MAX_NESTED_COLUMNS);
        }

        DataTypes nested_types;
        std::unordered_set<String> seen_type_names;
        nested_types.reserve(subtype_count);
        for (int i = 0; i < subtype_count; ++i)
        {
            const auto * subtype = orc_type->getSubtype(i);

            /// A union branch that is itself a union would map to a Variant, and Variant does not
            /// allow a nested Variant. Reject it through the normal unsupported-type / skip path
            /// here, before the outer DataTypeVariant is constructed - otherwise its constructor
            /// throws a confusing BAD_ARGUMENTS ("Nested Variant types are not allowed") and the
            /// skip setting is never consulted because the inner union has already been parsed.
            if (subtype->getKind() == orc::TypeKind::UNION)
            {
                if (skip_columns_with_unsupported_types)
                {
                    skipped = true;
                    return {};
                }
                throw Exception(
                    ErrorCodes::UNKNOWN_TYPE,
                    "ORC union type '{}' has a nested union branch, which is not supported",
                    orc_type->toString());
            }

            auto parsed_type = parseORCType(
                subtype, skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped, max_depth, depth + 1);
            if (skipped)
                return {};

            /// Branch identity must not depend on the stripe's physical encoding: a
            /// dictionary-encoded branch parses as LowCardinality(...), which would let e.g.
            /// uniontype<string,string> with one dictionary-encoded branch slip past this check
            /// (and the read would then fail on any stripe where the encodings agree).
            if (!seen_type_names.insert(recursiveRemoveLowCardinality(parsed_type)->getName()).second)
            {
                if (skip_columns_with_unsupported_types)
                {
                    skipped = true;
                    return {};
                }
                throw Exception(
                    ErrorCodes::UNKNOWN_TYPE,
                    "ORC union type '{}' has branches with identical types, which is not supported",
                    orc_type->toString());
            }
            nested_types.push_back(parsed_type);
        }
        return std::make_shared<DataTypeVariant>(nested_types);
    }

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
                orc_type->getSubtype(0), skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped, max_depth, depth + 1);
            if (skipped)
                return {};

            return std::make_shared<DataTypeArray>(nested_type);
        }
        case orc::TypeKind::MAP: {
            if (subtype_count != 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Orc Map type {}", orc_type->toString());

            DataTypePtr key_type = parseORCType(
                orc_type->getSubtype(0), skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped, max_depth, depth + 1);
            if (skipped)
                return {};

            DataTypePtr value_type = parseORCType(
                orc_type->getSubtype(1), skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped, max_depth, depth + 1);
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
                    orc_type->getSubtype(i), skip_columns_with_unsupported_types, dictionary_as_low_cardinality, stripe_info, skipped, max_depth, depth + 1);
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
                Float64 val = 0;
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
                Int64 val = 0;
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
        case KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL:
        {
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

            String column_name = getColumnNameFromKeyCondition(key_condition, curr.getKeyColumn());
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
            auto expect_type = makeNullableRecursively(parseORCType(orc_type, true, false, nullptr, skipped, format_settings.max_parser_depth), format_settings);
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
        case KeyCondition::RPNElement::FUNCTION_UNKNOWN:
        {
            builder.literal(orc::TruthValue::YES_NO_NULL);
            rpn_stack.pop_back();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_NOT:
        {
            builder.startNot();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_AND:
        {
            builder.startAnd();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::FUNCTION_OR:
        {
            builder.startOr();
            rpn_stack.pop_back();
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            buildORCSearchArgumentImpl(key_condition, header, schema, rpn_stack, builder, format_settings);
            builder.end();
            break;
        }
        case KeyCondition::RPNElement::ALWAYS_FALSE:
        {
            builder.literal(orc::TruthValue::NO);
            rpn_stack.pop_back();
            break;
        }
        case KeyCondition::RPNElement::ALWAYS_TRUE:
        {
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
    const FormatSettings & format_settings,
    bool use_prefetch,
    size_t min_bytes_for_seek,
    std::atomic<int> & is_stopped)
{
    if (is_stopped)
        return;

    orc::ReaderOptions options;
    /// ORC library requires rangeSizeLimit > holeSizeLimit.
    static constexpr uint64_t default_range_size_limit = 10 * 1024 * 1024UL;
    /// Clamp to avoid overflow when computing holeSizeLimit + 1.
    uint64_t hole_size_limit = std::min<uint64_t>(min_bytes_for_seek, std::numeric_limits<uint64_t>::max() - 1);
    uint64_t range_size_limit = std::max(default_range_size_limit, hole_size_limit + 1);
    options.setCacheOptions(orc::CacheOptions{.holeSizeLimit = hole_size_limit, .rangeSizeLimit = range_size_limit});

    auto input_stream = asORCInputStream(in, format_settings, use_prefetch, is_stopped);
    file_reader = orc::createReader(std::move(input_stream), options);
}

static const orc::Type *
traverseDownORCTypeByName(const std::string & target, const orc::Type * orc_type, DataTypePtr & type, bool ignore_case)
{
    /// Recurses the file-controlled ORC type tree. The matching CH type (and the requested column
    /// name) bound the depth in practice, but keep a stack backstop here too, since this runs in
    /// prepareFileReader before the readColumnFromORCColumn guard is reached.
    checkStackSize();

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

/// Forward declarations: updateIncludeTypeIds needs the union branch-hint machinery (defined
/// below, near readColumnFromORCColumn, which is where it is also used) to prune struct-branch
/// fields the same way the read path does. See computeOrcUnionBranchHints.
static bool orcUnionBranchMatchesType(const orc::Type * orc_branch_type, const DataTypePtr & target_type, bool case_insensitive);
static bool orcUnionBranchPrefersType(const orc::Type * orc_branch_type, const DataTypePtr & target_type);
static DataTypes computeOrcUnionBranchHints(const orc::Type * orc_type, const DataTypePtr & type_hint, bool case_insensitive_matching);

static void
updateIncludeTypeIds(DataTypePtr type, const orc::Type * orc_type, bool ignore_case, std::unordered_set<UInt64> & include_typeids)
{
    /// Recurses the file-controlled ORC type tree in lockstep with the (parser-bounded) CH type.
    /// Keep a stack backstop here too: this runs in prepareFileReader, before the
    /// readColumnFromORCColumn guard is reached, also for explicit-schema reads.
    checkStackSize();

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
        case orc::UNION: {
            /// ORC union maps to the ClickHouse Variant type. A branch that gets a forced type hint
            /// (the same way readColumnFromORCColumn computes it - see computeOrcUnionBranchHints)
            /// is selected by recursing into the hint, so a STRUCT branch keeps the named-tuple
            /// field pruning above: a field the hint's tuple does not reference (e.g. an
            /// unsupported or corrupt one) is never added to include_typeids. A branch without a
            /// forced hint is read in full, matching readColumnFromORCColumn's "no guess is ever
            /// made" policy, so its entire subtree is selected. Every branch always contributes at
            /// least one id, so ORC's ColumnSelector::selectParents never falls back to its "fully
            /// select every branch or none" override for partial branch selection, and the union's
            /// own tag stream is selected as the automatic parent of the selected branches.
            const DataTypes branch_hints = computeOrcUnionBranchHints(orc_type, non_nullable_type, ignore_case);
            for (size_t i = 0; i < orc_type->getSubtypeCount(); ++i)
            {
                const auto * branch_orc_type = orc_type->getSubtype(i);
                if (branch_hints[i])
                    updateIncludeTypeIds(branch_hints[i], branch_orc_type, ignore_case, include_typeids);
                else
                    include_typeids.insert(branch_orc_type->getColumnId());
            }
            return;
        }
        default:
            return;
    }
}

NativeORCBlockInputFormat::NativeORCBlockInputFormat(
    ReadBuffer & in_,
    SharedHeader header_,
    const FormatSettings & format_settings_,
    bool use_prefetch_,
    size_t min_bytes_for_seek_,
    FormatFilterInfoPtr format_filter_info_)
    : IInputFormat(std::move(header_), &in_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
    , skip_stripes(format_settings.orc.skip_stripes)
    , use_prefetch(use_prefetch_)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , format_filter_info(std::move(format_filter_info_))
{
}

void NativeORCBlockInputFormat::prepareFileReader()
{
    getFileReader(*in, file_reader, format_settings, use_prefetch, min_bytes_for_seek, is_stopped);
    if (is_stopped)
        return;

    if (format_filter_info)
        format_filter_info->initKeyConditionOnce(getPort().getHeader());

    std::unique_ptr<orc::StripeInformation> stripe_info;
    if (file_reader->getNumberOfStripes())
        stripe_info = file_reader->getStripe(0);

    orc_column_to_ch_column = std::make_unique<ORCColumnToCHColumn>(
        getPort().getHeader(),
        format_settings.orc.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.orc.case_insensitive_column_matching,
        format_settings.orc.dictionary_as_low_cardinality,
        format_settings.date_time_overflow_behavior);

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

    if (format_settings.orc.filter_push_down && format_filter_info && format_filter_info->key_condition && !sargs)
        sargs = buildORCSearchArgument(*format_filter_info->key_condition, getPort().getHeader(), file_reader->getType(), format_settings);

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
    chassert(file_reader);

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
        bool ok = false;
        try
        {
            ok = stripe_reader->next(*batch);
        }
        catch (const orc::ParseError & e)
        {
            /// The ORC library throws ParseError when the encoded data of a stripe is corrupt (for
            /// example, a union tag that is out of range for the union's branches). Surface it as
            /// INCORRECT_DATA instead of letting it propagate as a generic std::exception.
            throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to read ORC data: {}", e.what());
        }
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

void NativeORCSchemaReader::initializeIfNeeded()
{
    if (initialized)
        return;
    getFileReader(in, file_reader, format_settings, false, 0, is_stopped);
    initialized = true;
}

NamesAndTypesList NativeORCSchemaReader::readSchema()
{
    initializeIfNeeded();

    const auto & schema = file_reader->getType();
    Block header;
    std::unique_ptr<orc::StripeInformation> stripe_info;
    if (file_reader->getNumberOfStripes())
        stripe_info = file_reader->getStripe(0);

    try
    {
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
                skipped,
                format_settings.max_parser_depth);
            if (!skipped)
                header.insert(ColumnWithTypeAndName{type, name});
        }
    }
    catch (const orc::ParseError & e)
    {
        /// The ORC library throws ParseError when the stripe footer is corrupt (for example, a
        /// column id that has no matching column encoding, which is consulted here to detect
        /// dictionary encoding). Surface it as INCORRECT_DATA instead of letting it propagate as a
        /// generic std::exception.
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to read ORC schema: {}", e.what());
    }

    /// ORC doesn't have non-nullable data types.
    if (format_settings.schema_inference_make_columns_nullable != 0)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

std::optional<size_t> NativeORCSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return file_reader->getNumberOfRows();
}

ORCColumnToCHColumn::ORCColumnToCHColumn(
    const Block & header_,
    bool allow_missing_columns_,
    bool null_as_default_,
    bool case_insensitive_matching_,
    bool dictionary_as_low_cardinality_,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior_)
    : header(header_)
    , allow_missing_columns(allow_missing_columns_)
    , null_as_default(null_as_default_)
    , case_insensitive_matching(case_insensitive_matching_)
    , dictionary_as_low_cardinality(dictionary_as_low_cardinality_)
    , date_time_overflow_behavior(date_time_overflow_behavior_)
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
        return ColumnUInt8::create(orc_column->numElements, static_cast<UInt8>(0));

    auto nullmap_column = ColumnUInt8::create();
    PaddedPODArray<UInt8> & bytemap_data = assert_cast<ColumnVector<UInt8> &>(*nullmap_column).getData();
    bytemap_data.resize(orc_column->numElements);

    for (size_t i = 0; i < orc_column->numElements; ++i)
        bytemap_data[i] = 1 - orc_column->notNull[i];
    return nullmap_column;
}


/// A branch of an ORC union can be non-null at the union level yet select a null payload; ORC
/// leaves a placeholder (a default value) at such positions. When the branch is later cast to an
/// explicit Variant alternative, a value-checking cast (e.g. Int8 -> Enum8, or the same inside a
/// Tuple field) would reject those placeholders even though the rows are never read (they become
/// the Variant NULL discriminator). Overwrite every null-payload row with a copy of a non-null row
/// before casting; the copied values are discarded downstream. If every row is a null payload there
/// is nothing to read, so a null is returned and the caller uses a default-valued column instead.
static ColumnPtr replaceNullPayloadRowsWithValidRow(const ColumnPtr & column, const NullMap & null_map)
{
    size_t valid_row = column->size();
    for (size_t row = 0; row < column->size(); ++row)
        if (!null_map[row])
        {
            valid_row = row;
            break;
        }

    if (valid_row == column->size())
        return nullptr;

    auto result = column->cloneEmpty();
    result->reserve(column->size());
    for (size_t row = 0; row < column->size(); ++row)
        result->insertFrom(*column, null_map[row] ? valid_row : row);
    return result;
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
readColumnWithBooleanData(const orc::ColumnVectorBatch * orc_column, const String & column_name)
{
    const auto * orc_bool_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);
    auto internal_type = DataTypeFactory::instance().get("Bool");
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnVector<UInt8> &>(*internal_column).getData();

    column_data.reserve(orc_bool_column->numElements);
    for (size_t i = 0; i < orc_bool_column->numElements; ++i)
    {
        if (!orc_bool_column->hasNulls || orc_bool_column->notNull[i])
            column_data.push_back(static_cast<UInt8>(orc_bool_column->data[i]));
        else
            column_data.push_back(static_cast<UInt8>(0));
    }

    return {std::move(internal_column), internal_type, column_name};
}


template <typename NumericType, typename BatchType>
static ColumnWithTypeAndName
readColumnWithNumericData(const orc::ColumnVectorBatch * orc_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<ColumnVector<NumericType> &>(*internal_column).getData();
    const auto * orc_int_column = dynamic_cast<const BatchType *>(orc_column);

    column_data.reserve(orc_int_column->numElements);
    for (size_t i = 0; i < orc_int_column->numElements; ++i)
    {
        if (!orc_int_column->hasNulls || orc_int_column->notNull[i])
            column_data.push_back(NumericType(orc_int_column->data[i]));
        else
            column_data.push_back(NumericType{});
    }

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
    {
        auto result_column = internal_type->createColumn();
        result_column->insertManyDefaults(rows);
        return {std::move(result_column), internal_type, column_name};
    }

    size_t dict_size = orc_dict.dictionaryOffset.size() - 1;
    auto holder_column = holder_type->createColumn();
    if constexpr (fixed_string)
    {
        const size_t n = orc_type->getMaximumLength();
        auto & concrete_holder_column = assert_cast<ColumnFixedString &>(*holder_column);
        PaddedPODArray<UInt8> & column_chars = concrete_holder_column.getChars();
        size_t reserve_size = dict_size * n;
        column_chars.resize_exact(reserve_size);
        size_t curr_offset = 0;
        for (size_t i = 0; i < dict_size; ++i)
        {
            const auto * buf = orc_dict.dictionaryBlob.data() + orc_dict.dictionaryOffset[i];
            size_t buf_size = orc_dict.dictionaryOffset[i + 1] - orc_dict.dictionaryOffset[i];
            if (buf_size > n)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "ORC dictionary entry {} has size {} that exceeds the declared FixedString length {}",
                    i, buf_size, n);
            memcpy(&column_chars[curr_offset], buf, buf_size);
            /// resize_exact does not zero-initialize, so pad shorter entries to keep FixedString values
            /// deterministic and to avoid leaking uninitialized heap memory.
            if (buf_size < n)
                memset(&column_chars[curr_offset + buf_size], 0, n - buf_size);
            curr_offset += n;
        }
    }
    else
    {
        auto & concrete_holder_column = assert_cast<ColumnString &>(*holder_column);
        PaddedPODArray<UInt8> & column_chars = concrete_holder_column.getChars();
        PaddedPODArray<UInt64> & column_offsets = concrete_holder_column.getOffsets();

        column_chars.resize_exact(orc_dict.dictionaryBlob.size());
        column_offsets.resize_exact(dict_size);
        size_t curr_offset = 0;
        for (size_t i = 0; i < dict_size; ++i)
        {
            const auto * buf = orc_dict.dictionaryBlob.data() + orc_dict.dictionaryOffset[i];
            size_t buf_size = orc_dict.dictionaryOffset[i + 1] - orc_dict.dictionaryOffset[i];
            memcpy(&column_chars[curr_offset], buf, buf_size);
            curr_offset += buf_size;
            column_offsets[i] = curr_offset;
        }
    }

    /// Insert CH dictionary_column from holder_column
    auto tmp_internal_column = internal_type->createColumn();
    auto dictionary_column = IColumn::mutate(assert_cast<ColumnLowCardinality *>(tmp_internal_column.get())->getDictionaryPtr());
    auto index_column
        = dynamic_cast<IColumnUnique *>(dictionary_column.get())->uniqueInsertRangeFrom(*holder_column, 0, holder_column->size());

    auto check_index = [&](Int64 orc_index, size_t row) -> Int64
    {
        if (orc_index < 0 || static_cast<size_t>(orc_index) >= dict_size)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "ORC dictionary index {} at row {} is out of range [0, {})",
                orc_index, row, dict_size);
        return orc_index;
    };

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
                new_index_data[i] = index_data[check_index(orc_str_column.index[i], i)];
            }
        }
        else
        {
            for (size_t i = 0; i < rows; ++i)
            {
                /// Set index 0 if we meet null value. If dictionary_column is nullable, 0 represents null value.
                /// Otherwise 0 represents default string value, it is reasonable because null values are converted to default values when casting nullable column to non-nullable.
                new_index_data[i] = orc_str_column.notNull[i] ? index_data[check_index(orc_str_column.index[i], i)] : 0;
            }
        }

        return ColumnLowCardinality::create(std::move(dictionary_column), std::move(new_index_column), /*is_shared=*/false);
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
readColumnWithStringData(const orc::ColumnVectorBatch * orc_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeString>();
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars = assert_cast<ColumnString &>(*internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
    size_t reserve_size = 0;
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
            reserve_size += orc_str_column->length[i];
    }

    column_chars.resize_exact(reserve_size);
    column_offsets.resize_exact(orc_str_column->numElements);

    size_t curr_offset = 0;
    if (!orc_str_column->hasNulls)
    {
        for (size_t i = 0; i < orc_str_column->numElements; ++i)
        {
            const auto * buf = orc_str_column->data[i];
            size_t buf_size = orc_str_column->length[i];
            memcpy(&column_chars[curr_offset], buf, buf_size);
            curr_offset += buf_size;
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
                memcpy(&column_chars[curr_offset], buf, buf_size);
                curr_offset += buf_size;
            }

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
    PaddedPODArray<UInt8> & column_chars = assert_cast<ColumnFixedString &>(*internal_column).getChars();
    column_chars.reserve(orc_column->numElements * fixed_len);

    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->hasNulls || orc_str_column->notNull[i])
        {
            const Int64 length = orc_str_column->length[i];
            if (length < 0 || static_cast<size_t>(length) > fixed_len)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "ORC string value at row {} has size {} that doesn't fit into FixedString({})",
                    i, length, fixed_len);

            column_chars.insert_assume_reserved(orc_str_column->data[i], orc_str_column->data[i] + length);
            /// Zero-pad shorter values to the fixed width to keep the FixedString layout consistent.
            if (static_cast<size_t>(length) < fixed_len)
                column_chars.resize_fill(column_chars.size() + (fixed_len - static_cast<size_t>(length)));
        }
        else
            column_chars.resize_fill(column_chars.size() + fixed_len);
    }

    return {std::move(internal_column), internal_type, column_name};
}


template <typename DecimalType, typename BatchType>
static ColumnWithTypeAndName readColumnWithDecimalDataCast(
    const orc::ColumnVectorBatch * orc_column, const String & column_name, DataTypePtr internal_type)
{
    using NativeType = typename DecimalType::NativeType;
    static_assert(std::is_same_v<BatchType, orc::Decimal128VectorBatch> || std::is_same_v<BatchType, orc::Decimal64VectorBatch>);

    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<ColumnDecimal<DecimalType> &>(*internal_column).getData();
    column_data.reserve(orc_column->numElements);

    const auto * orc_decimal_column = dynamic_cast<const BatchType *>(orc_column);
    for (size_t i = 0; i < orc_decimal_column->numElements; ++i)
    {
        if (!orc_decimal_column->hasNulls || orc_decimal_column->notNull[i])
        {
            DecimalType decimal_value{};
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
        else
            column_data.push_back(DecimalType{});
    }

    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName
readIPv6ColumnFromBinaryData(const orc::ColumnVectorBatch * orc_column, const String & column_name)
{
    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);

    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        /// If at least one value size is not 16 bytes, fallback to reading String column and further cast to IPv6.
        if ((!orc_str_column->hasNulls || orc_str_column->notNull[i]) && orc_str_column->length[i] != sizeof(IPv6))
            return readColumnWithStringData(orc_column, column_name);
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
readIPv4ColumnWithInt32Data(const orc::ColumnVectorBatch * orc_column, const String & column_name)
{
    const auto * orc_int_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);

    auto internal_type = std::make_shared<DataTypeIPv4>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnIPv4 &>(*internal_column).getData();
    column_data.reserve(orc_int_column->numElements);

    for (size_t i = 0; i < orc_int_column->numElements; ++i)
    {
        if (!orc_int_column->hasNulls || orc_int_column->notNull[i])
            column_data.push_back(static_cast<UInt32>(orc_int_column->data[i]));
        else
            column_data.push_back(0);
    }

    return {std::move(internal_column), internal_type, column_name};
}

template <typename ColumnType>
static ColumnWithTypeAndName readColumnWithBigNumberFromBinaryData(
    const orc::ColumnVectorBatch * orc_column, const String & column_name, const DataTypePtr & column_type)
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
    const orc::ColumnVectorBatch * orc_column, const String & column_name, const DataTypePtr & type_hint)
{
    DataTypePtr internal_type;
    bool check_date32_range = false;
    bool check_date_range = false;

    /// Make result type Date32 when requested type is actually Date32 or when we use schema inference
    if (!type_hint || isDate32(*type_hint))
    {
        internal_type = std::make_shared<DataTypeDate32>();
        check_date32_range = true;
    }
    else if (isDate(*type_hint))
    {
        /// Date type is not supported in ORC format according to the docs
        /// ORC date type is INT32, which can represent dates outside the Date
        /// type range [0, 65535]. Validate the range and throw an error
        internal_type = std::make_shared<DataTypeInt32>();
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
            if (check_date32_range && (days_num > DATE_LUT_MAX_EXTEND_DAY_NUM || days_num < -DAYNUM_OFFSET_EPOCH))
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                    "Input value {} of a column \"{}\" exceeds the range of type Date32, which is [{}, {}]",
                    days_num,
                    column_name,
                    -DAYNUM_OFFSET_EPOCH,
                    DATE_LUT_MAX_EXTEND_DAY_NUM);

            if (check_date_range && (days_num > DATE_LUT_MAX_DAY_NUM || days_num < 0))
                throw Exception(
                    ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                    "Input value {} of a column \"{}\" exceeds the range of type Date, which is [0, {}]",
                    days_num,
                    column_name,
                    DATE_LUT_MAX_DAY_NUM);

            column_data.push_back(days_num);
        }
        else
        {
            column_data.push_back(0);
        }
    }

    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName readColumnWithTimestampData(
    const orc::ColumnVectorBatch * orc_column,
    const String & column_name,
    const DataTypePtr & type_hint,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior)
{
    const auto * orc_ts_column = dynamic_cast<const orc::TimestampVectorBatch *>(orc_column);

    /// ORC stores timestamps as (seconds, nanoseconds). ClickHouse used to convert them through a fixed
    /// DateTime64(9) intermediate (seconds * 1e9 + nanoseconds), which overflows Int64 for timestamps beyond
    /// ~year 2262 even when the requested DateTime64 scale can represent the value (e.g. Iceberg's DateTime64(6)).
    /// Read directly at the requested scale so such values round-trip like Parquet, and only handle overflow when
    /// even the target scale does not fit Int64.
    UInt32 scale = 9;
    if (type_hint)
    {
        const auto * dt64_hint = typeid_cast<const DataTypeDateTime64 *>(removeNullable(recursiveRemoveLowCardinality(type_hint)).get());
        if (dt64_hint)
            scale = dt64_hint->getScale();
    }

    auto internal_type = std::make_shared<DataTypeDateTime64>(scale);
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnDateTime64 &>(*internal_column).getData();
    column_data.reserve(orc_ts_column->numElements);

    /// Factor to go from nanoseconds down to the target scale (scale is in [0, 9]).
    const Int128 divisor = DecimalUtils::scaleMultiplier<Int128>(9 - scale);
    const Int128 int64_min = std::numeric_limits<Int64>::min();
    const Int128 int64_max = std::numeric_limits<Int64>::max();

    for (size_t i = 0; i < orc_ts_column->numElements; ++i)
    {
        if (!orc_ts_column->hasNulls || orc_ts_column->notNull[i])
        {
            const Int64 seconds = orc_ts_column->data[i];
            const Int64 nanoseconds = orc_ts_column->nanoseconds[i];

            /// seconds * 1e9 + nanoseconds cannot overflow Int128 (|seconds| < 2^63, product < ~9.2e27 << Int128 max).
            const Int128 nanos_total = static_cast<Int128>(seconds) * 1'000'000'000 + nanoseconds;
            /// Integer division truncates toward zero, matching the DateTime64(9) -> DateTime64(scale) cast that ran afterwards.
            Int128 value = nanos_total / divisor;

            if (value < int64_min || value > int64_max)
            {
                if (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate)
                    value = value < int64_min ? int64_min : int64_max;
                else
                    /// Throw for both `throw` and `ignore` (the default): keep the historical behavior and never silently corrupt data.
                    throw Exception(
                        ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Timestamp value in column \"{}\" is out of range for DateTime64({}): seconds={}, nanoseconds={}",
                        column_name,
                        scale,
                        seconds,
                        nanoseconds);
            }

            column_data.push_back(DateTime64(static_cast<Int64>(value)));
        }
        else
            column_data.push_back(0);
    }
    return {std::move(internal_column), internal_type, column_name};
}

/// Whether a DateTime/DateTime64 target carries an explicit timezone - schema inference produces one
/// (UTC) for the ORC `TIMESTAMP_INSTANT` ("timestamp with local timezone") kind, but an explicit
/// schema may name any timezone. Used to tell the two ORC timestamp kinds apart when matching union
/// branches (see `orcUnionBranchMatchesType`).
static bool orcTimestampTargetHasExplicitTimeZone(const DataTypePtr & type)
{
    const auto * timezone = dynamic_cast<const TimezoneMixin *>(type.get());
    return timezone && timezone->hasExplicitTimeZone();
}

/// Whether a ClickHouse numeric type is a widening of an ORC integer branch whose values occupy
/// `orc_size` bytes: a strictly wider integer, or a float that represents every value of that width
/// exactly. The ordinary (non-union) ORC column path gets these conversions for free from the final
/// cast in `orcColumnsToCHChunk`; a union branch has to request them as a per-branch hint, because
/// the final cast of a union column is a `Variant` -> `Variant` cast, which only extends the set of
/// alternatives by exact type name and cannot widen an alternative. Equal-width targets are listed
/// explicitly by the caller (they include the writer's own mappings, such as the unsigned and `Enum`
/// types), so only strictly wider integers are accepted here.
static bool orcIntegerBranchWidensTo(const DataTypePtr & type, size_t orc_size)
{
    const WhichDataType which(type);
    if (which.isFloat32())
        return orc_size <= 2;
    if (which.isFloat64())
        return orc_size <= 4;
    return which.isInteger() && type->getSizeOfValueInMemory() > orc_size;
}

/// Whether a ClickHouse type can serve as the per-branch type hint for an ORC union branch of the
/// given ORC type. The correspondence is structural (up to Nullable and LowCardinality wrappers),
/// extended with the explicit-schema conversions the reader supports: the integer targets the
/// ordinary column path accepts through the final cast (the unsigned and Enum types of the
/// matching width - they are what the ORC writer maps to these ORC types - int -> IPv4, and the
/// widenings of `orcIntegerBranchWidensTo`, e.g. `Variant(Int64, String)` over
/// `uniontype<int,string>`) and
/// the special binary readers (binary -> IPv6/Int128/UInt128/Int256/UInt256/Decimal256,
/// char -> big integers and Decimal256). Custom type names matter here: `Bool` is a custom-named
/// `UInt8`, but the writer maps it to BOOLEAN while a plain `UInt8` goes to BYTE, so only BOOLEAN
/// accepts `Bool` - otherwise `Variant(Bool, UInt8)` over `uniontype<boolean,tinyint>` would leave
/// both branches ambiguous. The two ORC timestamp kinds need the same care: both read back as plain
/// `DateTime64(9)`, but `TIMESTAMP` is inferred as `DateTime64(9)` and `TIMESTAMP_INSTANT` as
/// `DateTime64(9, 'UTC')`, so `TIMESTAMP_INSTANT` matches only an explicitly time-zoned target while
/// `TIMESTAMP` stays permissive - otherwise `uniontype<timestamp,timestamp with local timezone>`,
/// inferred as `Variant(DateTime64(9), DateTime64(9, 'UTC'))`, would leave both branches ambiguous,
/// collapse them to identical `DateTime64(9)`, and be rejected by the duplicate-branch check. Any
/// explicit timezone is accepted (not only `UTC`) so that an explicit schema like
/// `Variant(DateTime64(9), DateTime64(9, 'Europe/Berlin'))` is not rejected; the singles-elimination
/// step decides whether the assignment is forced, and the repair cast relabels the branch to the
/// target timezone (value-preserving for `DateTime64`). Variant sorts its nested types, so the
/// positional correspondence between ORC union branches and Variant alternatives is lost; this
/// predicate is used to reconstruct it.
///
/// STRUCT branches follow the same named-tuple rules as the non-union ORC struct path: a target
/// tuple with explicit names matches by field name and may project and reorder the ORC struct's
/// fields (the per-branch repair cast then projects/reorders the read tuple to exactly the target),
/// while an unnamed target tuple is matched positionally with the same arity. `case_insensitive`
/// mirrors `input_format_orc_case_insensitive_column_matching` for that name-based matching.
static bool orcUnionBranchMatchesType(const orc::Type * orc_branch_type, const DataTypePtr & target_type, bool case_insensitive)
{
    checkStackSize();

    const DataTypePtr type = removeLowCardinality(removeNullableOrLowCardinalityNullable(target_type));
    const WhichDataType which(type);

    switch (orc_branch_type->getKind())
    {
        case orc::TypeKind::BOOLEAN:
            /// A boolean holds only 0 and 1, so every integer type is a widening of it.
            return which.isUInt8() || which.isEnum8() || orcIntegerBranchWidensTo(type, 0);
        case orc::TypeKind::BYTE:
            return which.isInt8() || (which.isUInt8() && !isBool(type)) || which.isEnum8() || orcIntegerBranchWidensTo(type, 1);
        case orc::TypeKind::SHORT:
            return which.isInt16() || which.isUInt16() || which.isEnum16() || orcIntegerBranchWidensTo(type, 2);
        case orc::TypeKind::INT:
            return which.isInt32() || which.isUInt32() || which.isIPv4() || orcIntegerBranchWidensTo(type, 4);
        case orc::TypeKind::LONG:
            return which.isInt64() || which.isUInt64() || orcIntegerBranchWidensTo(type, 8);
        case orc::TypeKind::FLOAT:
            return which.isFloat32() || which.isFloat64();
        case orc::TypeKind::DOUBLE:
            return which.isFloat64();
        case orc::TypeKind::DATE:
            return which.isDate32() || which.isDate();
        case orc::TypeKind::TIMESTAMP:
            /// Permissive: matches any DateTime/DateTime64 alternative. The repair cast relabels the
            /// branch to the alternative's timezone, which is value-preserving for DateTime64.
            return which.isDateTime64() || which.isDateTime();
        case orc::TypeKind::TIMESTAMP_INSTANT:
            /// Matches only an explicitly time-zoned target so it stays distinct from a TIMESTAMP
            /// branch in the same union (both read back as DateTime64(9)); see the comment above the
            /// function.
            return (which.isDateTime64() || which.isDateTime()) && orcTimestampTargetHasExplicitTimeZone(type);
        case orc::TypeKind::DECIMAL:
            return which.isDecimal();
        case orc::TypeKind::STRING:
        case orc::TypeKind::VARCHAR:
            return which.isStringOrFixedString();
        case orc::TypeKind::BINARY:
            return which.isStringOrFixedString() || which.isIPv6() || which.isInt128() || which.isUInt128() || which.isInt256()
                || which.isUInt256() || which.isDecimal256();
        case orc::TypeKind::CHAR:
            return which.isStringOrFixedString() || which.isInt128() || which.isUInt128() || which.isInt256() || which.isUInt256()
                || which.isDecimal256();
        case orc::TypeKind::LIST:
            return which.isArray() && orc_branch_type->getSubtypeCount() == 1
                && orcUnionBranchMatchesType(
                    orc_branch_type->getSubtype(0), assert_cast<const DataTypeArray &>(*type).getNestedType(), case_insensitive);
        case orc::TypeKind::MAP:
        {
            if (!which.isMap() || orc_branch_type->getSubtypeCount() != 2)
                return false;
            const auto & map_type = assert_cast<const DataTypeMap &>(*type);
            return orcUnionBranchMatchesType(orc_branch_type->getSubtype(0), map_type.getKeyType(), case_insensitive)
                && orcUnionBranchMatchesType(orc_branch_type->getSubtype(1), map_type.getValueType(), case_insensitive);
        }
        case orc::TypeKind::STRUCT:
        {
            if (!which.isTuple())
                return false;
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
            const auto & elements = tuple_type.getElements();
            if (tuple_type.hasExplicitNames())
            {
                /// Match by field name, like the non-union ORC struct path: the target tuple may be
                /// a subset of the ORC struct's fields, in any order. Each target field must map to
                /// an ORC field with a recursively matching type; extra ORC fields are projected out
                /// by the repair cast. Build a name -> ORC subtype map for the lookup.
                std::unordered_map<String, const orc::Type *> orc_field_by_name;
                orc_field_by_name.reserve(orc_branch_type->getSubtypeCount());
                for (size_t i = 0; i < orc_branch_type->getSubtypeCount(); ++i)
                {
                    String field_name = orc_branch_type->getFieldName(i);
                    if (case_insensitive)
                        boost::to_lower(field_name);
                    orc_field_by_name.emplace(std::move(field_name), orc_branch_type->getSubtype(i));
                }

                const auto & element_names = tuple_type.getElementNames();
                for (size_t i = 0; i < elements.size(); ++i)
                {
                    String element_name = element_names[i];
                    if (case_insensitive)
                        boost::to_lower(element_name);
                    auto it = orc_field_by_name.find(element_name);
                    if (it == orc_field_by_name.end() || !orcUnionBranchMatchesType(it->second, elements[i], case_insensitive))
                        return false;
                }
                return true;
            }

            /// Unnamed tuple: positional, same arity (no ORC field is silently dropped).
            if (elements.size() != orc_branch_type->getSubtypeCount())
                return false;
            for (size_t i = 0; i < elements.size(); ++i)
                if (!orcUnionBranchMatchesType(orc_branch_type->getSubtype(i), elements[i], case_insensitive))
                    return false;
            return true;
        }
        case orc::TypeKind::UNION:
            return which.isVariant();
    }

    return false;
}

/// The natural inference pairing of the string-like ORC kinds, used to tie-break their union-branch
/// matching: CHAR is inferred as FixedString of the same length and STRING/VARCHAR as String, but
/// all of them can be *read* as either (see orcUnionBranchMatchesType), so e.g.
/// uniontype<char(1),string> with the inferred Variant(FixedString(1), String) leaves both branches
/// with two candidates. Without a hint each branch would materialize per the current stripe's
/// physical encoding (a dictionary-encoded stripe comes back as LowCardinality), making a supported
/// schema fail depending on per-stripe encoding choices; preferring the natural pairing keeps the
/// assignment stable. The target is inspected with the LowCardinality/Nullable wrappers stripped,
/// so a LowCardinality-wrapped alternative (inferred from a dictionary-encoded stripe) pairs the
/// same way. BINARY deliberately has no preference: it is the conversion-rich kind (IPv6, big
/// integers, Decimal256), so a preference could steal a String alternative from a STRING branch.
///
/// The numeric kinds are tie-broken the same way, for the same reason: they match their widenings
/// too (see `orcIntegerBranchWidensTo`), so e.g. `uniontype<tinyint,bigint>` with the explicit
/// schema `Variant(Int8, Int64)` leaves the `BYTE` branch with two candidates. Preferring the
/// natural pairing keeps the exact-width alternative with its own branch and lets the wider
/// alternative go to the branch that needs it.
static bool orcUnionBranchPrefersType(const orc::Type * orc_branch_type, const DataTypePtr & target_type)
{
    const DataTypePtr type = removeLowCardinality(removeNullableOrLowCardinalityNullable(target_type));
    const WhichDataType which(type);

    switch (orc_branch_type->getKind())
    {
        case orc::TypeKind::BOOLEAN:
            return isBool(type);
        case orc::TypeKind::BYTE:
            return which.isInt8();
        case orc::TypeKind::SHORT:
            return which.isInt16();
        case orc::TypeKind::INT:
            return which.isInt32();
        case orc::TypeKind::LONG:
            return which.isInt64();
        case orc::TypeKind::FLOAT:
            return which.isFloat32();
        case orc::TypeKind::DOUBLE:
            return which.isFloat64();
        case orc::TypeKind::CHAR:
            return which.isFixedString()
                && assert_cast<const DataTypeFixedString &>(*type).getN() == orc_branch_type->getMaximumLength();
        case orc::TypeKind::STRING:
        case orc::TypeKind::VARCHAR:
            return which.isString();
        default:
            return false;
    }
}

/// Variant sorts its nested types, so the positional correspondence between ORC union branches
/// and the alternatives of an explicit (or inferred) Variant type hint is lost. Reconstruct it
/// structurally: collect the alternatives each branch can be read as (orcUnionBranchMatchesType),
/// then keep only the forced assignments (a branch takes an alternative when it is its only
/// remaining candidate), tie-breaking the string-like branches by their natural inference pairing
/// (orcUnionBranchPrefersType). A branch whose correspondence stays ambiguous is left without a
/// hint (nullptr in the result), so no guess is ever made. Shared between
/// readColumnFromORCColumn (drives the branch conversions) and updateIncludeTypeIds (prunes a
/// hinted STRUCT branch's fields the same way the read path does), so the two stay in sync.
static DataTypes computeOrcUnionBranchHints(const orc::Type * orc_type, const DataTypePtr & type_hint, bool case_insensitive_matching)
{
    const size_t num_children = orc_type->getSubtypeCount();
    DataTypes branch_hints(num_children);
    const auto * variant_hint = type_hint ? typeid_cast<const DataTypeVariant *>(type_hint.get()) : nullptr;
    if (!variant_hint)
        return branch_hints;

    const auto & alternatives = variant_hint->getVariants();
    std::vector<std::vector<size_t>> candidates(num_children);
    for (size_t i = 0; i < num_children; ++i)
        for (size_t a = 0; a < alternatives.size(); ++a)
            if (orcUnionBranchMatchesType(orc_type->getSubtype(i), alternatives[a], case_insensitive_matching))
                candidates[i].push_back(a);

    std::vector<bool> alternative_taken(alternatives.size(), false);
    const auto preferred_candidates = [&](size_t i)
    {
        std::vector<size_t> preferred;
        for (const size_t a : candidates[i])
            if (orcUnionBranchPrefersType(orc_type->getSubtype(i), alternatives[a]))
                preferred.push_back(a);
        return preferred;
    };
    bool changed = true;
    while (changed)
    {
        changed = false;
        for (size_t i = 0; i < num_children; ++i)
        {
            if (branch_hints[i] || candidates[i].size() != 1)
                continue;
            const size_t a = candidates[i].front();
            if (alternative_taken[a])
            {
                /// Another branch already took this alternative; leave this branch without a hint.
                candidates[i].clear();
                continue;
            }
            alternative_taken[a] = true;
            branch_hints[i] = alternatives[a];
            changed = true;
            for (size_t j = 0; j < num_children; ++j)
                if (j != i)
                    std::erase(candidates[j], a);
        }

        if (changed)
            continue;

        /// The forced assignments are exhausted. Tie-break the string-like branches by their
        /// natural inference pairing (see orcUnionBranchPrefersType): a branch takes the single
        /// alternative it prefers among its remaining candidates, unless another unassigned
        /// branch uniquely prefers the same one - a contested preference is still ambiguous and no
        /// guess is ever made. One assignment at a time, then back to the forced-assignment loop
        /// to propagate it.
        for (size_t i = 0; i < num_children && !changed; ++i)
        {
            if (branch_hints[i] || candidates[i].size() < 2)
                continue;
            const auto preferred = preferred_candidates(i);
            if (preferred.size() != 1)
                continue;
            const size_t a = preferred.front();
            bool contested = false;
            for (size_t j = 0; j < num_children && !contested; ++j)
            {
                if (j == i || branch_hints[j])
                    continue;
                const auto preferred_j = preferred_candidates(j);
                contested = preferred_j.size() == 1 && preferred_j.front() == a;
            }
            if (contested)
                continue;
            alternative_taken[a] = true;
            branch_hints[i] = alternatives[a];
            changed = true;
            for (size_t j = 0; j < num_children; ++j)
                if (j != i)
                    std::erase(candidates[j], a);
        }
    }

    return branch_hints;
}

ColumnWithTypeAndName ORCColumnToCHColumn::readColumnFromORCColumn(
    const orc::ColumnVectorBatch * orc_column,
    const orc::Type * orc_type,
    const std::string & column_name,
    bool inside_nullable,
    DataTypePtr type_hint) const
{
    /// Reading a nested LIST/MAP/STRUCT recurses through this function over the ORC type tree, whose
    /// depth is attacker-controlled, so guard the native stack on the data-read path as well.
    checkStackSize();

    bool skipped = false;

    if (!inside_nullable && (orc_column->hasNulls || (type_hint && isNullableOrLowCardinalityNullable(type_hint))) && !orc_column->isEncoded
        && (orc_type->getKind() != orc::LIST && orc_type->getKind() != orc::MAP && orc_type->getKind() != orc::UNION))
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

    /// ORC union maps to the ClickHouse Variant type. Handled before the switch below so the switch
    /// stays non-exhaustive (its default handles unsupported types).
    if (orc_type->getKind() == orc::UNION)
    {
        const auto * orc_union_column = dynamic_cast<const orc::UnionVectorBatch *>(orc_column);
        if (!orc_union_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC column for union type must be a UnionVectorBatch");

        const size_t num_children = orc_type->getSubtypeCount();
        const size_t num_rows = orc_union_column->numElements;

        /// Mirror of the schema-inference guard: a union with more branches than Variant can hold
        /// (ColumnVariant::MAX_NESTED_COLUMNS) is rejected explicitly instead of letting the
        /// DataTypeVariant below throw a confusing BAD_ARGUMENTS. Only reachable with an explicit
        /// structure, since schema inference already rejects oversized unions.
        if (num_children > ColumnVariant::MAX_NESTED_COLUMNS)
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "ORC union type with {} branches is not supported (Variant supports at most {} nested types), while reading column {}",
                num_children, ColumnVariant::MAX_NESTED_COLUMNS, column_name);

        /// The hints enable the explicit-schema conversions the scalar readers support (e.g.
        /// binary -> IPv6) and keep the branch types aligned with the hinted Variant (e.g.
        /// Array(Nullable(...)) produced by schema inference). See computeOrcUnionBranchHints for
        /// how a branch's forced alternative is reconstructed; updateIncludeTypeIds calls the same
        /// function so a hinted STRUCT branch's field pruning matches what is actually read here.
        const DataTypes branch_hints = computeOrcUnionBranchHints(orc_type, type_hint, case_insensitive_matching);

        /// Read each ORC union branch into its own column. ORC keeps a separate physical batch per
        /// branch. Variant branches are non-nullable, and an ORC union row can be non-null yet
        /// select a branch whose payload is null - such a row is represented by the Variant NULL
        /// discriminator (see the row loop below). The branch null map is taken directly from the
        /// ORC batch rather than from a Nullable result column: complex (LIST/MAP) and
        /// dictionary-encoded branches never come back as ColumnNullable, yet their payload can
        /// still be null. inside_nullable is set because nulls are handled here rather than by
        /// wrapping the branch value in Nullable.
        DataTypes branch_types;
        Columns branch_columns;
        std::vector<ColumnPtr> branch_null_map_columns(num_children); /// keeps the null maps alive
        std::vector<const NullMap *> branch_null_maps(num_children, nullptr);
        branch_types.reserve(num_children);
        branch_columns.reserve(num_children);
        for (size_t i = 0; i < num_children; ++i)
        {
            /// A union branch that is itself a union would map to a nested Variant, which Variant
            /// forbids; reject it explicitly (matching the schema-inference path) instead of letting
            /// the DataTypeVariant below throw a confusing BAD_ARGUMENTS. Only reachable with an
            /// explicit structure, since schema inference already rejects nested unions.
            if (orc_type->getSubtype(i)->getKind() == orc::UNION)
                throw Exception(
                    ErrorCodes::UNKNOWN_TYPE,
                    "ORC union type '{}' has a nested union branch, which is not supported, while reading column {}",
                    orc_type->toString(), column_name);

            auto branch = readColumnFromORCColumn(
                orc_union_column->children[i], orc_type->getSubtype(i), column_name, /*inside_nullable=*/true, branch_hints[i]);

            if (orc_union_column->children[i]->hasNulls)
            {
                branch_null_map_columns[i] = readByteMapFromORCColumn(orc_union_column->children[i]);
                branch_null_maps[i] = &assert_cast<const ColumnUInt8 &>(*branch_null_map_columns[i]).getData();
            }

            /// A dictionary-encoded branch read without a hint comes back as
            /// LowCardinality(Nullable(...)); Variant alternatives cannot be nullable, so strip the
            /// inner Nullable (the nulls are tracked by the branch null map above).
            ColumnWithTypeAndName branch_non_nullable{
                removeNullableOrLowCardinalityNullable(branch.column),
                removeNullableOrLowCardinalityNullable(branch.type),
                branch.name};

            /// With a per-branch hint the branch must end up as exactly that alternative, so that
            /// the resulting Variant type equals the hinted one (e.g. a branch of a
            /// non-dictionary-encoded file read for a LowCardinality(String) alternative comes
            /// back as plain String). Compared by name, not equals: Variant identity is name-based
            /// and e.g. equals cannot tell the custom-named Bool from a plain UInt8, which would
            /// skip the Bool -> UInt8 repair cast for a boolean branch hinted as UInt8.
            if (branch_hints[i] && branch_hints[i]->getName() != branch_non_nullable.type->getName())
            {
                if (branch_null_map_columns[i])
                {
                    /// A value-checking cast (e.g. Int8 -> Enum8, or the same on a nested Tuple
                    /// field) must not inspect the placeholder values ORC leaves at null-payload
                    /// positions - those rows become the Variant NULL discriminator below and are
                    /// never read. Replace them with a valid, castable row before casting.
                    /// Wrapping the branch in Nullable is not enough: a Nullable(Tuple) cast would
                    /// still descend into and reject a null nested field.
                    if (auto safe = replaceNullPayloadRowsWithValidRow(branch_non_nullable.column, *branch_null_maps[i]))
                        branch_non_nullable.column
                            = castColumn({safe, branch_non_nullable.type, branch_non_nullable.name}, branch_hints[i]);
                    else
                        /// Every row is a null payload and none is read, so a default-valued column
                        /// of the target type with the same number of rows is enough.
                        branch_non_nullable.column
                            = branch_hints[i]->createColumn()->cloneResized(branch_non_nullable.column->size());
                }
                else
                {
                    branch_non_nullable.column = castColumn(branch_non_nullable, branch_hints[i]);
                }
                branch_non_nullable.type = branch_hints[i];
            }

            branch_types.push_back(branch_non_nullable.type);
            branch_columns.push_back(std::move(branch_non_nullable.column));
        }

        /// ORC keeps one physical stream per branch, so branches with identical types cannot be
        /// represented as a Variant (which de-duplicates types); reject them explicitly. The
        /// identity is compared with LowCardinality stripped, so it does not depend on the current
        /// stripe's physical encoding (a dictionary-encoded branch materializes as LowCardinality),
        /// mirroring the schema-inference check in parseORCType.
        std::unordered_set<String> seen_type_names;
        for (const auto & branch_type : branch_types)
            if (!seen_type_names.insert(recursiveRemoveLowCardinality(branch_type)->getName()).second)
                throw Exception(
                    ErrorCodes::UNKNOWN_TYPE,
                    "ORC union type '{}' has branches with identical types, which is not supported, while reading column {}",
                    orc_type->toString(), column_name);

        auto variant_type = std::make_shared<DataTypeVariant>(branch_types);
        const auto & global_variants = assert_cast<const DataTypeVariant &>(*variant_type).getVariants();

        /// Variant stores its branches sorted by type name, so remap ORC tags to global (sorted)
        /// discriminators. The Variant sub-columns are built compactly (each must contain exactly
        /// the values referenced by its discriminator, in appended order), so they are cloned empty
        /// from the ORC branch columns and filled row by row below rather than placed wholesale.
        std::unordered_map<String, ColumnVariant::Discriminator> type_name_to_global;
        for (size_t g = 0; g < global_variants.size(); ++g)
            type_name_to_global[global_variants[g]->getName()] = static_cast<ColumnVariant::Discriminator>(g);

        MutableColumns variant_columns(global_variants.size());
        std::vector<ColumnVariant::Discriminator> tag_to_global(num_children);
        for (size_t i = 0; i < num_children; ++i)
        {
            auto global = type_name_to_global.at(branch_types[i]->getName());
            tag_to_global[i] = global;
            variant_columns[global] = branch_columns[i]->cloneEmpty();
        }

        auto local_discriminators = ColumnVariant::ColumnDiscriminators::create();
        auto & discriminators_data = local_discriminators->getData();
        discriminators_data.resize_exact(num_rows);

        auto offsets = ColumnVariant::ColumnOffsets::create();
        auto & offsets_data = offsets->getData();
        offsets_data.resize_exact(num_rows);

        const unsigned char * tags = orc_union_column->tags.data();
        const uint64_t * orc_offsets = orc_union_column->offsets.data();
        const char * not_null = orc_union_column->hasNulls ? orc_union_column->notNull.data() : nullptr;

        for (size_t row = 0; row < num_rows; ++row)
        {
            if (not_null && !not_null[row])
            {
                discriminators_data[row] = ColumnVariant::NULL_DISCRIMINATOR;
                offsets_data[row] = 0;
                continue;
            }

            const size_t tag = tags[row];
            if (tag >= num_children)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Invalid ORC union tag {} for union with {} branches while reading column {}",
                    tag, num_children, column_name);

            const size_t offset = orc_offsets[row];
            /// A malformed file can point past the selected branch's values; reject it rather than
            /// reading out of bounds (the branch column and its null map share this size).
            if (offset >= branch_columns[tag]->size())
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Invalid ORC union offset {} into branch {} of size {} while reading column {}",
                    offset, tag, branch_columns[tag]->size(), column_name);

            /// A non-null union row can still select a branch whose payload is null (ORC keeps the
            /// union-level null count at 0 in that case). Variant branches are non-nullable, so
            /// represent such a row as a Variant NULL rather than the branch's nested default value.
            if (branch_null_maps[tag] && (*branch_null_maps[tag])[offset])
            {
                discriminators_data[row] = ColumnVariant::NULL_DISCRIMINATOR;
                offsets_data[row] = 0;
                continue;
            }

            const auto global = tag_to_global[tag];
            auto & variant_column = *variant_columns[global];
            discriminators_data[row] = global;
            offsets_data[row] = variant_column.size();
            variant_column.insertFrom(*branch_columns[tag], offset);
        }

        auto variant_column = ColumnVariant::create(std::move(local_discriminators), std::move(offsets), std::move(variant_columns));
        return {std::move(variant_column), variant_type, column_name};
    }

    switch (orc_type->getKind())
    {
        case orc::STRING:
        case orc::BINARY:
        case orc::VARCHAR:
        {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    case TypeIndex::IPv6:
                        return readIPv6ColumnFromBinaryData(orc_column, column_name);
                    /// ORC format outputs big integers as binary column, because there is no fixed binary in ORC.
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(orc_column, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(orc_column, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(orc_column, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(orc_column, column_name, type_hint);
                    /// ORC doesn't support Decimal256 as separate type. We read and write it as binary data.
                    case TypeIndex::Decimal256:
                        return readColumnWithBigNumberFromBinaryData<ColumnDecimal<Decimal256>>(orc_column, column_name, type_hint);
                    default:;
                }
            }

            if (orc_column->isEncoded && dictionary_as_low_cardinality)
            {
                bool nullable = type_hint ? isNullableOrLowCardinalityNullable(type_hint) : true;
                return readColumnWithEncodedStringOrFixedStringData<false>(orc_column, orc_type, column_name, nullable);
            }
            else
                return readColumnWithStringData(orc_column, column_name);
        }
        case orc::CHAR:
        {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(orc_column, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(orc_column, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(orc_column, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(orc_column, column_name, type_hint);
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
            return readColumnWithBooleanData(orc_column, column_name);
        case orc::BYTE:
            return readColumnWithNumericData<Int8, orc::LongVectorBatch>(orc_column, column_name);
        case orc::SHORT:
            return readColumnWithNumericData<Int16, orc::LongVectorBatch>(orc_column, column_name);
        case orc::INT:
        {
            /// ORC format doesn't have unsigned integers and we output IPv4 as Int32.
            /// We should allow to read it back from Int32.
            if (type_hint && isIPv4(type_hint))
                return readIPv4ColumnWithInt32Data(orc_column, column_name);
            return readColumnWithNumericData<Int32, orc::LongVectorBatch>(orc_column, column_name);
        }
        case orc::LONG:
            return readColumnWithNumericData<Int64, orc::LongVectorBatch>(orc_column, column_name);
        case orc::FLOAT:
            return readColumnWithNumericData<Float32, orc::DoubleVectorBatch>(orc_column, column_name);
        case orc::DOUBLE:
            return readColumnWithNumericData<Float64, orc::DoubleVectorBatch>(orc_column, column_name);
        case orc::DATE:
            return readColumnWithDateData(orc_column, column_name, type_hint);
        case orc::TIMESTAMP: [[fallthrough]];
        case orc::TIMESTAMP_INSTANT:
            return readColumnWithTimestampData(orc_column, column_name, type_hint, date_time_overflow_behavior);
        case orc::DECIMAL:
        {
            auto interal_type = parseORCType(orc_type, false, false, nullptr, skipped);

            auto precision = orc_type->getPrecision();
            if (precision == 0)
                precision = 38;

            if (precision <= DecimalUtils::max_precision<Decimal32>)
                return readColumnWithDecimalDataCast<Decimal32, orc::Decimal64VectorBatch>(orc_column, column_name, interal_type);
            if (precision <= DecimalUtils::max_precision<Decimal64>)
                return readColumnWithDecimalDataCast<Decimal64, orc::Decimal64VectorBatch>(orc_column, column_name, interal_type);
            if (precision <= DecimalUtils::max_precision<Decimal128>)
                return readColumnWithDecimalDataCast<Decimal128, orc::Decimal128VectorBatch>(orc_column, column_name, interal_type);
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Decimal precision {} in ORC type {} is out of bound", precision, orc_type->toString());
        }
        case orc::MAP:
        {
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
        case orc::LIST:
        {
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
            DataTypePtr array_type;
            ColumnPtr array_data_column = nested_column.column;
            /// If type hint is Nested and the element is a named Tuple, return the Nested type
            /// so that `Nested::flatten` can decompose it into separate arrays.
            /// When the element is Nullable(Tuple(...)), unwrap it and propagate the struct null
            /// map to each element via `unwrapNullableTuple`.
            const auto * tuple_type = type_hint && isNested(type_hint)
                ? typeid_cast<const DataTypeTuple *>(removeNullable(nested_column.type).get())
                : nullptr;
            if (tuple_type)
            {
                auto unwrapped = Nested::unwrapNullableTuple({array_data_column, nested_column.type, column_name});
                array_data_column = unwrapped.column;
                const auto & result_tuple = assert_cast<const DataTypeTuple &>(*unwrapped.type);
                array_type = createNested(result_tuple.getElements(), result_tuple.getElementNames());
            }
            else
            {
                array_type = std::make_shared<DataTypeArray>(nested_column.type);
            }
            auto array_column = ColumnArray::create(array_data_column, offsets_column);
            return {array_column, array_type, column_name};
        }
        case orc::STRUCT:
        {
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

void registerInputFormatORC(FormatFactory & factory);
void registerInputFormatORC(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "ORC",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & read_settings,
           bool is_remote_fs,
           FormatParserSharedResourcesPtr,
           FormatFilterInfoPtr format_filter_info)
        {
            const bool has_file_size = isBufferWithFileSize(buf);
            auto * seekable_in = dynamic_cast<SeekableReadBuffer *>(&buf);
            const bool use_prefetch = is_remote_fs && read_settings.remote_fs_settings.prefetch && has_file_size && seekable_in
                && seekable_in->checkIfActuallySeekable() && seekable_in->supportsReadAt() && settings.seekable_read;
            const size_t min_bytes_for_seek = use_prefetch ? read_settings.remote_fs_settings.min_bytes_for_seek : 0;
            return std::make_shared<NativeORCBlockInputFormat>(
                buf, std::make_shared<const Block>(sample), settings, use_prefetch, min_bytes_for_seek, format_filter_info);
        });
    factory.markFormatSupportsSubsetOfColumns("ORC");

    factory.setDocumentation("ORC", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache ORC](https://orc.apache.org/) is a columnar storage format widely used in the [Hadoop](https://hadoop.apache.org/) ecosystem.

## Data types matching {#data-types-matching-orc}

The table below compares supported ORC data types and their corresponding ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| ORC data type (`INSERT`)              | ClickHouse data type                                                                                              | ORC data type (`SELECT`) |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|--------------------------|
| `Boolean`                             | [Bool](/sql-reference/data-types/boolean.md)                                                              | `Boolean`                |
| `Tinyint`                             | [Int8/UInt8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)    | `Tinyint`                |
| `Smallint`                            | [Int16/UInt16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) | `Smallint`               |
| `Int`                                 | [Int32/UInt32](/sql-reference/data-types/int-uint.md)                                                     | `Int`                    |
| `Bigint`                              | [Int64/UInt64](/sql-reference/data-types/int-uint.md)                                                     | `Bigint`                 |
| `Float`                               | [Float32](/sql-reference/data-types/float.md)                                                             | `Float`                  |
| `Double`                              | [Float64](/sql-reference/data-types/float.md)                                                             | `Double`                 |
| `Decimal`                             | [Decimal](/sql-reference/data-types/decimal.md)                                                           | `Decimal`                |
| `Date`                                | [Date32](/sql-reference/data-types/date32.md)                                                             | `Date`                   |
| `Timestamp`                           | [DateTime64](/sql-reference/data-types/datetime64.md)                                                     | `Timestamp`              |
| `String`, `Varchar`, `Binary`         | [String](/sql-reference/data-types/string.md)                                                             | `String`                 |
| `Char`                                | [FixedString](/sql-reference/data-types/fixedstring.md)                                                   | `String`                 |
| `List`                                | [Array](/sql-reference/data-types/array.md)                                                               | `List`                   |
| `Struct`                              | [Tuple](/sql-reference/data-types/tuple.md)                                                               | `Struct`                 |
| `Map`                                 | [Map](/sql-reference/data-types/map.md)                                                                   | `Map`                    |
| `Int`                                 | [IPv4](/sql-reference/data-types/int-uint.md)                                                             | `Int`                    |
| `Binary`                              | [IPv6](/sql-reference/data-types/ipv6.md)                                                                 | `Binary`                 |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                                    | `Binary`                 |
| `Binary`                              | [Decimal256](/sql-reference/data-types/decimal.md)                                                        | `Binary`                 |
| `Union`                               | [Variant](/sql-reference/data-types/variant.md)                                                           | `Union`                  |

- Other types are not supported.
- An ORC `Union` column is read as a [Variant](/sql-reference/data-types/variant.md) over the union's branch types, and a `Variant` column is written as an ORC `Union` over its branch types. Note that `Variant` sorts its branch types, so the branch order may differ from the ORC file. Unions with duplicate branch types (e.g. `uniontype<int,int>`) are not supported.
- Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.
- The data types of ClickHouse table columns do not have to match the corresponding ORC data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to the data type set for the ClickHouse table column.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using an ORC file with the following data, named as `football.orc`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

### Reading data {#reading-data}

Read data using the `ORC` format:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output ORC files.
:::

## Format settings {#format-settings}

| Setting                                                                                                                                                                                                      | Description                                                                            | Default |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|---------|
| [`output_format_orc_string_as_string`](/reference/settings/formats/output-format#output_format_orc_string_as_string)                                                                                 | Use ORC String type instead of Binary for String columns.                              | `true`  |
| [`output_format_orc_compression_method`](/reference/settings/formats/output-format#output_format_orc_compression_method)                                                                             | Compression method used in output ORC format. Default value                            | `zstd`  |
| [`input_format_orc_case_insensitive_column_matching`](/reference/settings/formats/input-format#input_format_orc_case_insensitive_column_matching)                                                   | Ignore case when matching ORC columns with ClickHouse columns.                         | `false` |
| [`input_format_orc_allow_missing_columns`](/reference/settings/formats/input-format#input_format_orc_allow_missing_columns)                                                                         | Allow missing columns while reading ORC data.                                          | `true`  |
| [`input_format_orc_skip_columns_with_unsupported_types_in_schema_inference`](/reference/settings/formats/input-format#input_format_orc_skip_columns_with_unsupported_types_in_schema_inference)     | Allow skipping columns with unsupported types while schema inference for ORC format.   | `false` |

To exchange data with Hadoop, you can use [HDFS table engine](/reference/engines/table-engines/integrations/hdfs).
)DOCS_MD"});
}

void registerORCSchemaReader(FormatFactory & factory);
void registerORCSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "ORC",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<NativeORCSchemaReader>(buf, settings);
        }
        );

    factory.registerAdditionalInfoForSchemaCacheGetter("ORC", [](const FormatSettings & settings)
    {
        return fmt::format(
            "schema_inference_make_columns_nullable={};schema_inference_allow_nullable_tuple_type={};"
            "dictionary_as_low_cardinality={};skip_columns_with_unsupported_types={};max_parser_depth={}",
            settings.schema_inference_make_columns_nullable,
            settings.schema_inference_allow_nullable_tuple_type,
            settings.orc.dictionary_as_low_cardinality,
            settings.orc.skip_columns_with_unsupported_types_in_schema_inference,
            settings.max_parser_depth);
    });
}

}

#else

namespace DB
{
    class FormatFactory;

    void registerInputFormatORC(FormatFactory &);
    void registerORCSchemaReader(FormatFactory &);

    void registerInputFormatORC(FormatFactory &)
    {
    }

    void registerORCSchemaReader(FormatFactory &)
    {
    }
}

#endif
