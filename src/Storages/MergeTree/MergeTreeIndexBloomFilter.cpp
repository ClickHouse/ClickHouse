#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationBool.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Functions/FunctionConversionMatching.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/array/emptyArrayToSingle.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/BloomFilterHash.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Common/DateLUTImpl.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/SipHash.h>

#include <array>
#include <bit>
#include <cstring>
#include <type_traits>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

namespace Setting
{
    extern const SettingsBool validate_enum_literals_in_operators;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_QUERY;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleBloomFilter::MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, size_t index_columns_)
    : bits_per_row(bits_per_row_), hash_functions(hash_functions_), bloom_filters(index_columns_)
{
    total_rows = 0;
    for (size_t column = 0; column < index_columns_; ++column)
        bloom_filters[column] = std::make_shared<BloomFilter>(bits_per_row, hash_functions, 0);
}

MergeTreeIndexGranuleBloomFilter::MergeTreeIndexGranuleBloomFilter(
    size_t bits_per_row_, size_t hash_functions_, const std::vector<HashSet<UInt64>>& column_hashes_)
        : bits_per_row(bits_per_row_), hash_functions(hash_functions_), bloom_filters(column_hashes_.size())
{
    if (column_hashes_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule_index_blocks empty or total_rows is zero.");

    size_t bloom_filter_max_size = 0;
    for (const auto & column_hash : column_hashes_)
        bloom_filter_max_size = std::max(bloom_filter_max_size, column_hash.size());

    static size_t atom_size = 8;

    // If multiple columns are given, we will initialize all the bloom filters
    // with the size of the highest-cardinality one. This is done for compatibility with
    // existing binary serialization format
    total_rows = bloom_filter_max_size;
    size_t bytes_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;

    for (size_t column = 0, columns = column_hashes_.size(); column < columns; ++column)
    {
        bloom_filters[column] = std::make_shared<BloomFilter>(bytes_size, hash_functions, 0);
        fillingBloomFilter(bloom_filters[column], column_hashes_[column]);
    }
}

bool MergeTreeIndexGranuleBloomFilter::empty() const
{
    return !total_rows;
}

size_t MergeTreeIndexGranuleBloomFilter::memoryUsageBytes() const
{
    size_t sum = 0;
    for (const auto & bloom_filter : bloom_filters)
        sum += bloom_filter->memoryUsageBytes();
    return sum;
}

void MergeTreeIndexGranuleBloomFilter::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    readVarUInt(total_rows, istr);

    static size_t atom_size = 8;
    size_t bytes_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;
    size_t read_size = bytes_size;
    for (auto & filter : bloom_filters)
    {
        filter->resize(bytes_size);
        /// Big-endian granules hold whole `BloomFilter::UnderType` words, so only the payload size
        /// differs there. The read itself is unconditional: guarding it too leaves the filter zeroed.
        if constexpr (std::endian::native == std::endian::big)
            read_size = filter->getFilter().size() * sizeof(BloomFilter::UnderType);
        istr.readStrict(reinterpret_cast<char *>(filter->getFilter().data()), read_size);
    }
}

void MergeTreeIndexGranuleBloomFilter::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty bloom filter index.");

    writeVarUInt(total_rows, ostr);

    static size_t atom_size = 8;
    size_t write_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;
    for (const auto & bloom_filter : bloom_filters)
    {
        /// Mirrors `deserializeBinary`: the size is byte-order dependent, the write is not.
        if constexpr (std::endian::native == std::endian::big)
            write_size = bloom_filter->getFilter().size() * sizeof(BloomFilter::UnderType);
        ostr.write(reinterpret_cast<const char *>(bloom_filter->getFilter().data()), write_size);
    }
}

void MergeTreeIndexGranuleBloomFilter::fillingBloomFilter(BloomFilterPtr & bf, const HashSet<UInt64> &hashes) const
{
    for (const auto & bf_base_hash : hashes)
        for (size_t i = 0; i < hash_functions; ++i)
            bf->addHashWithSeed(bf_base_hash.getKey(), BloomFilterHash::bf_hash_seed[i]);
}

namespace
{

/// True when the index column is an `Array` and the set holds an empty array. A set column that is
/// not a `ColumnArray` cannot be inspected, so it counts as holding one.
bool setHasEmptyArray(const DataTypePtr & index_type, const ColumnPtr & set_column, size_t row_size)
{
    if (!WhichDataType(index_type).isArray())
        return false;

    const auto * array_column = checkAndGetColumn<ColumnArray>(set_column.get());
    if (!array_column)
        return true;

    for (size_t row = 0; row < row_size; ++row)
        if (array_column->getSize(row) == 0)
            return true;

    return false;
}

ColumnWithTypeAndName getPreparedSetInfo(const ConstSetPtr & prepared_set)
{
    if (prepared_set->getDataTypes().size() == 1)
        return {prepared_set->getSetElements()[0], prepared_set->getElementsTypes()[0], "dummy"};

    Columns set_elements;
    for (auto & set_element : prepared_set->getSetElements())

        set_elements.emplace_back(set_element->convertToFullColumnIfConst());

    return {ColumnTuple::create(set_elements), std::make_shared<DataTypeTuple>(prepared_set->getElementsTypes()), "dummy"};
}

enum class LambdaInputKind : UInt8
{
    Argument,
    Capture,
};

struct LambdaInputSlot
{
    LambdaInputKind kind;
    size_t index;

    bool operator==(const LambdaInputSlot &) const = default;
};

struct LambdaBodyBinding
{
    std::unordered_map<const ActionsDAG::Node *, LambdaInputSlot> query_inputs;
    std::unordered_map<const ActionsDAG::Node *, LambdaInputSlot> index_inputs;
};

struct ExpressionNodePair
{
    const ActionsDAG::Node * query_node;
    const ActionsDAG::Node * index_node;
    bool is_lambda_carrier = false;
    bool inside_lambda_body = false;
    std::shared_ptr<const LambdaBodyBinding> body_binding;

    bool operator==(const ExpressionNodePair &) const = default;
};

struct ExpressionNodePairHash
{
    size_t operator()(const ExpressionNodePair & pair) const
    {
        const auto query_hash = std::hash<const ActionsDAG::Node *>{}(pair.query_node);
        const auto index_hash = std::hash<const ActionsDAG::Node *>{}(pair.index_node);
        const auto pair_hash = query_hash ^ (index_hash + 0x9e3779b9 + (query_hash << 6) + (query_hash >> 2));
        const auto lambda_hash = pair_hash ^ (static_cast<size_t>(pair.is_lambda_carrier) + 0x9e3779b9 + (pair_hash << 6) + (pair_hash >> 2));
        const auto body_hash = lambda_hash ^ (static_cast<size_t>(pair.inside_lambda_body) + 0x9e3779b9 + (lambda_hash << 6) + (lambda_hash >> 2));
        const auto binding_hash = std::hash<const LambdaBodyBinding *>{}(pair.body_binding.get());
        return body_hash ^ (binding_hash + 0x9e3779b9 + (body_hash << 6) + (body_hash >> 2));
    }
};

struct ExpressionMatchBudget
{
    static constexpr size_t max_work = 4096;
    static constexpr size_t max_constant_bytes = 4096;
    static constexpr size_t max_array_elements = 256;

    size_t & remaining;

    explicit ExpressionMatchBudget(size_t & remaining_) : remaining(remaining_) {}

    bool consume(size_t amount = 1)
    {
        if (amount > remaining)
            return false;
        remaining -= amount;
        return true;
    }
};

bool isNativeTypeTree(const IDataType & type, ExpressionMatchBudget & budget, size_t depth = 0)
{
    if (depth > 32 || !budget.consume())
        return false;

    const auto * raw = &type;
    auto is_leaf = [&]
    {
        return typeid(*raw) == typeid(DataTypeNothing) || typeid(*raw) == typeid(DataTypeString)
            || typeid(*raw) == typeid(DataTypeFixedString) || typeid(*raw) == typeid(DataTypeDate) || typeid(*raw) == typeid(DataTypeDate32)
            || typeid(*raw) == typeid(DataTypeDateTime) || typeid(*raw) == typeid(DataTypeDateTime64)
            || typeid(*raw) == typeid(DataTypeEnum8) || typeid(*raw) == typeid(DataTypeEnum16) || typeid(*raw) == typeid(DataTypeUUID)
            || typeid(*raw) == typeid(DataTypeIPv4) || typeid(*raw) == typeid(DataTypeIPv6) || typeid(*raw) == typeid(DataTypeUInt8)
            || typeid(*raw) == typeid(DataTypeUInt16) || typeid(*raw) == typeid(DataTypeUInt32) || typeid(*raw) == typeid(DataTypeUInt64)
            || typeid(*raw) == typeid(DataTypeUInt128) || typeid(*raw) == typeid(DataTypeUInt256) || typeid(*raw) == typeid(DataTypeInt8)
            || typeid(*raw) == typeid(DataTypeInt16) || typeid(*raw) == typeid(DataTypeInt32) || typeid(*raw) == typeid(DataTypeInt64)
            || typeid(*raw) == typeid(DataTypeInt128) || typeid(*raw) == typeid(DataTypeInt256) || typeid(*raw) == typeid(DataTypeBFloat16)
            || typeid(*raw) == typeid(DataTypeFloat32) || typeid(*raw) == typeid(DataTypeFloat64)
            || typeid(*raw) == typeid(DataTypeDecimal32) || typeid(*raw) == typeid(DataTypeDecimal64)
            || typeid(*raw) == typeid(DataTypeDecimal128) || typeid(*raw) == typeid(DataTypeDecimal256);
    };

    if (is_leaf())
        return !raw->getCustomName() && !raw->getCustomSerialization();
    if (typeid(*raw) == typeid(DataTypeArray))
    {
        if (raw->getCustomName() || raw->getCustomSerialization())
            return false;
        return isNativeTypeTree(*static_cast<const DataTypeArray &>(*raw).getNestedType(), budget, depth + 1);
    }
    if (typeid(*raw) == typeid(DataTypeNullable))
    {
        if (raw->getCustomName() || raw->getCustomSerialization())
            return false;
        return isNativeTypeTree(*static_cast<const DataTypeNullable &>(*raw).getNestedType(), budget, depth + 1);
    }
    if (typeid(*raw) == typeid(DataTypeLowCardinality))
    {
        if (raw->getCustomName() || raw->getCustomSerialization())
            return false;
        return isNativeTypeTree(*static_cast<const DataTypeLowCardinality &>(*raw).getDictionaryType(), budget, depth + 1);
    }
    if (typeid(*raw) == typeid(DataTypeTuple))
    {
        if (raw->getCustomName() || raw->getCustomSerialization())
            return false;
        const auto & elements = static_cast<const DataTypeTuple &>(*raw).getElements();
        if (!budget.consume(elements.size()))
            return false;
        for (const auto & element : elements)
            if (!element || !isNativeTypeTree(*element, budget, depth + 1))
                return false;
        return true;
    }
    if (typeid(*raw) == typeid(DataTypeMap))
    {
        if (raw->getCustomName() || raw->getCustomSerialization() || !budget.consume(2))
            return false;
        const auto & map = static_cast<const DataTypeMap &>(*raw);
        return map.getKeyType() && map.getValueType() && isNativeTypeTree(*map.getKeyType(), budget, depth + 1)
            && isNativeTypeTree(*map.getValueType(), budget, depth + 1);
    }

    /// Dynamic, Variant, and custom/opaque types can hide a conversion-sensitive concrete value.
    return false;
}

bool isNativePrimitiveNumericType(const IDataType & type)
{
    return typeid(type) == typeid(DataTypeUInt8) || typeid(type) == typeid(DataTypeUInt16) || typeid(type) == typeid(DataTypeUInt32)
        || typeid(type) == typeid(DataTypeUInt64) || typeid(type) == typeid(DataTypeUInt128) || typeid(type) == typeid(DataTypeUInt256)
        || typeid(type) == typeid(DataTypeInt8) || typeid(type) == typeid(DataTypeInt16) || typeid(type) == typeid(DataTypeInt32)
        || typeid(type) == typeid(DataTypeInt64) || typeid(type) == typeid(DataTypeInt128) || typeid(type) == typeid(DataTypeInt256)
        || typeid(type) == typeid(DataTypeBFloat16) || typeid(type) == typeid(DataTypeFloat32) || typeid(type) == typeid(DataTypeFloat64);
}

bool isNativeDecimalType(const IDataType & type)
{
    return typeid(type) == typeid(DataTypeDecimal32) || typeid(type) == typeid(DataTypeDecimal64)
        || typeid(type) == typeid(DataTypeDecimal128) || typeid(type) == typeid(DataTypeDecimal256);
}

bool isNativeEnumType(const IDataType & type)
{
    return typeid(type) == typeid(DataTypeEnum8) || typeid(type) == typeid(DataTypeEnum16);
}

bool isSameNativeStringType(const IDataType & source, const IDataType & destination)
{
    if (typeid(source) != typeid(destination))
        return false;
    if (typeid(source) == typeid(DataTypeString))
        return true;
    if (typeid(source) == typeid(DataTypeFixedString))
        return static_cast<const DataTypeFixedString &>(source).getN() == static_cast<const DataTypeFixedString &>(destination).getN();
    return false;
}

bool isSafeNativeCastLeafPair(const IDataType & source, const IDataType & destination)
{
    if (isNativePrimitiveNumericType(source) && isNativePrimitiveNumericType(destination))
        return true;

    if ((isNativePrimitiveNumericType(source) || isNativeDecimalType(source)) && typeid(destination) == typeid(DataTypeString))
        return true;

    if ((typeid(source) == typeid(DataTypeString) || typeid(source) == typeid(DataTypeFixedString)) && isNativeEnumType(destination))
        return true;

    const bool narrow_unsigned_source
        = typeid(source) == typeid(DataTypeUInt8) || typeid(source) == typeid(DataTypeUInt16) || typeid(source) == typeid(DataTypeUInt32);
    const bool datetime_destination
        = typeid(destination) == typeid(DataTypeDateTime) || typeid(destination) == typeid(DataTypeDateTime64);
    return (narrow_unsigned_source && datetime_destination) || isSameNativeStringType(source, destination);
}

bool isContextStableNativeCastTypePair(const DataTypePtr & source, const DataTypePtr & destination, ExpressionMatchBudget & budget)
{
    struct PendingPair
    {
        const IDataType * source;
        const IDataType * destination;
        size_t depth;
    };

    if (!source || !destination)
        return false;
    std::vector<PendingPair> pending{{source.get(), destination.get(), 0}};

    while (!pending.empty())
    {
        if (!budget.consume())
            return false;
        const auto [left, right, depth] = pending.back();
        pending.pop_back();
        if (!left || !right || depth > 32 || !isNativeTypeTree(*left, budget, depth) || !isNativeTypeTree(*right, budget, depth))
            return false;

        auto enqueue = [&](const IDataType * left_child, const IDataType * right_child)
        {
            if (!budget.consume())
                return false;
            pending.push_back({left_child, right_child, depth + 1});
            return true;
        };

        if (typeid(*left) == typeid(DataTypeArray) || typeid(*right) == typeid(DataTypeArray))
        {
            if (typeid(*left) != typeid(DataTypeArray) || typeid(*right) != typeid(DataTypeArray)
                || !enqueue(
                    static_cast<const DataTypeArray &>(*left).getNestedType().get(),
                    static_cast<const DataTypeArray &>(*right).getNestedType().get()))
                return false;
            continue;
        }
        if (typeid(*left) == typeid(DataTypeNullable) || typeid(*right) == typeid(DataTypeNullable))
        {
            if (typeid(*left) != typeid(DataTypeNullable) || typeid(*right) != typeid(DataTypeNullable)
                || !enqueue(
                    static_cast<const DataTypeNullable &>(*left).getNestedType().get(),
                    static_cast<const DataTypeNullable &>(*right).getNestedType().get()))
                return false;
            continue;
        }
        if (typeid(*left) == typeid(DataTypeLowCardinality) || typeid(*right) == typeid(DataTypeLowCardinality))
        {
            if (typeid(*left) != typeid(DataTypeLowCardinality) || typeid(*right) != typeid(DataTypeLowCardinality)
                || !enqueue(
                    static_cast<const DataTypeLowCardinality &>(*left).getDictionaryType().get(),
                    static_cast<const DataTypeLowCardinality &>(*right).getDictionaryType().get()))
                return false;
            continue;
        }
        if (typeid(*left) == typeid(DataTypeTuple) || typeid(*right) == typeid(DataTypeTuple))
        {
            if (typeid(*left) != typeid(DataTypeTuple) || typeid(*right) != typeid(DataTypeTuple))
                return false;
            const auto & left_tuple = static_cast<const DataTypeTuple &>(*left);
            const auto & right_tuple = static_cast<const DataTypeTuple &>(*right);
            const auto & left_elements = left_tuple.getElements();
            const auto & right_elements = right_tuple.getElements();
            if (left_elements.size() != right_elements.size() || !budget.consume(left_elements.size()))
                return false;

            /// CAST maps tuples by name when both sides have explicit names and at least one name
            /// overlaps. Only identical name order is known to preserve the positional leaf pairs
            /// checked here. Other named remappings (and schema-evolution defaults) fail closed.
            if (left_tuple.hasExplicitNames() && right_tuple.hasExplicitNames())
            {
                const auto & left_names = left_tuple.getElementNames();
                const auto & right_names = right_tuple.getElementNames();
                if (left_names.size() != left_elements.size() || right_names.size() != right_elements.size()
                    || !budget.consume(left_names.size()))
                    return false;
                for (size_t i = 0; i < left_names.size(); ++i)
                    if (!budget.consume(left_names[i].size()) || !budget.consume(right_names[i].size()) || left_names[i] != right_names[i])
                        return false;
            }

            for (size_t i = 0; i < left_elements.size(); ++i)
                if (!enqueue(left_elements[i].get(), right_elements[i].get()))
                    return false;
            continue;
        }
        if (typeid(*left) == typeid(DataTypeMap) || typeid(*right) == typeid(DataTypeMap))
        {
            if (typeid(*left) != typeid(DataTypeMap) || typeid(*right) != typeid(DataTypeMap)
                || !enqueue(
                    static_cast<const DataTypeMap &>(*left).getKeyType().get(), static_cast<const DataTypeMap &>(*right).getKeyType().get())
                || !enqueue(
                    static_cast<const DataTypeMap &>(*left).getValueType().get(),
                    static_cast<const DataTypeMap &>(*right).getValueType().get()))
                return false;
            continue;
        }

        if (!isSafeNativeCastLeafPair(*left, *right))
            return false;
    }

    return true;
}

bool isStableSourceForDateTimeModeConversion(const DataTypePtr & source, ExpressionMatchBudget & budget)
{
    if (!source || !isNativeTypeTree(*source, budget))
        return false;

    const IDataType * current = source.get();
    for (size_t depth = 0; depth <= 32; ++depth)
    {
        if (!budget.consume())
            return false;

        if (typeid(*current) == typeid(DataTypeNullable))
        {
            current = static_cast<const DataTypeNullable &>(*current).getNestedType().get();
            if (!current)
                return false;
            continue;
        }
        if (typeid(*current) == typeid(DataTypeLowCardinality))
        {
            current = static_cast<const DataTypeLowCardinality &>(*current).getDictionaryType().get();
            if (!current)
                return false;
            continue;
        }

        /// Only narrow unsigned integers have been established as stable across conversion modes.
        /// Other numeric inputs fail closed until their range and overflow behavior is separately established.
        return typeid(*current) == typeid(DataTypeUInt8) || typeid(*current) == typeid(DataTypeUInt16)
            || typeid(*current) == typeid(DataTypeUInt32);
    }

    return false;
}

bool sameType(const DataTypePtr & left, const DataTypePtr & right, ExpressionMatchBudget & budget);

bool sameNativeTypeTree(const DataTypePtr & left, const DataTypePtr & right, ExpressionMatchBudget & budget)
{
    /// Authenticate both trees before sameType reads custom-name/serialization or child accessors.
    /// A lambda capture and its outer expression commonly own distinct instances of the same type.
    return left && right && isNativeTypeTree(*left, budget) && isNativeTypeTree(*right, budget) && sameType(left, right, budget);
}

bool isBloomIndexExpressionContextStable(const ActionsDAG & indexed_expression_dag)
{
    size_t work_remaining = ExpressionMatchBudget::max_work;
    ExpressionMatchBudget budget(work_remaining);
    std::vector<const ActionsDAG *> pending_dags{&indexed_expression_dag};
    std::vector<const ActionsDAG::Node *> pending_nodes;
    std::unordered_set<const ActionsDAG *> visited_dags;
    std::unordered_set<const ActionsDAG::Node *> visited_nodes;
    const auto * canonical_cast = FunctionFactory::instance().tryGetCanonicalCreator("CAST");
    const auto * canonical_internal_cast = FunctionFactory::instance().tryGetCanonicalCreator("_CAST");
    const auto * canonical_accurate_cast = FunctionFactory::instance().tryGetCanonicalCreator("accurateCast");
    const auto * canonical_accurate_cast_or_null = FunctionFactory::instance().tryGetCanonicalCreator("accurateCastOrNull");
    const auto * canonical_to_datetime = FunctionFactory::instance().tryGetCanonicalCreator("toDateTime");
    const auto * canonical_to_datetime32 = FunctionFactory::instance().tryGetCanonicalCreator("toDateTime32");
    const auto * canonical_to_datetime64 = FunctionFactory::instance().tryGetCanonicalCreator("toDateTime64");
    const auto * canonical_to_unix_timestamp = FunctionFactory::instance().tryGetCanonicalCreator("toUnixTimestamp");
    if (!canonical_cast || !canonical_internal_cast || !canonical_accurate_cast || !canonical_accurate_cast_or_null
        || !canonical_to_datetime || !canonical_to_datetime32 || !canonical_to_datetime64 || !canonical_to_unix_timestamp)
        return false;

    while (!pending_dags.empty() || !pending_nodes.empty())
    {
        if (!pending_dags.empty())
        {
            if (!budget.consume())
                return false;
            const auto * dag = pending_dags.back();
            pending_dags.pop_back();
            if (!dag || !visited_dags.insert(dag).second)
                continue;

            const auto & outputs = dag->getOutputs();
            if (outputs.empty() || !budget.consume(outputs.size()))
                return false;
            pending_nodes.insert(pending_nodes.end(), outputs.begin(), outputs.end());
            continue;
        }

        if (!budget.consume())
            return false;
        const ActionsDAG::Node * node = pending_nodes.back();
        pending_nodes.pop_back();
        if (!node)
            return false;
        if (!visited_nodes.insert(node).second)
            continue;
        if (!budget.consume(node->children.size()))
            return false;
        pending_nodes.insert(pending_nodes.end(), node->children.begin(), node->children.end());
        const auto * node_result_type = node->result_type.get();

        if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            if (!node->function_base || node->is_function_compiled)
                return false;
            const auto * function_base = node->function_base.get();

            /// The factory handle is a non-virtual identity check. Authenticate native CASTs by
            /// exact final implementation identity before reading their types. A non-native
            /// function carrying any canonical CAST-family handle is malformed and fails closed.
            const auto * handle = node->function_base->getFactoryHandle();
            const bool is_native_cast = isNativeCastForBloomFilterExpressionMatching(*function_base);
            const bool claims_native_cast = handle == canonical_cast || handle == canonical_internal_cast
                || handle == canonical_accurate_cast || handle == canonical_accurate_cast_or_null;
            const bool is_native_datetime_conversion
                = isNativeDateTimeModeDependentConversionForBloomFilterExpressionMatching(*function_base);
            const bool is_native_settings_conversion
                = isNativeSettingsCapturingConversionForBloomFilterExpressionMatching(*function_base);
            const bool claims_native_datetime_conversion = handle == canonical_to_datetime || handle == canonical_to_datetime32
                || handle == canonical_to_datetime64 || handle == canonical_to_unix_timestamp;
            if (!is_native_cast && claims_native_cast)
                return false;
            if (!is_native_datetime_conversion && claims_native_datetime_conversion)
                return false;
            if ((is_native_cast || is_native_datetime_conversion) && !is_native_settings_conversion)
                return false;
            if (is_native_settings_conversion)
            {
                const auto & argument_types = node->function_base->getArgumentTypes();
                const auto & result_type = node->function_base->getResultType();
                if (argument_types.empty() || argument_types.size() != node->children.size() || !result_type
                    || node->result_type.get() != result_type.get() || !budget.consume(argument_types.size()))
                    return false;

                for (size_t i = 0; i < argument_types.size(); ++i)
                    if (!argument_types[i] || !node->children[i] || !node->children[i]->result_type
                        || argument_types[i].get() != node->children[i]->result_type.get()
                        || !isNativeTypeTree(*argument_types[i], budget))
                        return false;
                if (!isNativeTypeTree(*result_type, budget))
                    return false;

                if (is_native_cast)
                {
                    /// Exact final FunctionCast identity admits internal/no-handle variants too;
                    /// every native CAST must pass the closed paired leaf policy.
                    if (argument_types.size() != 2 || !isContextStableNativeCastTypePair(argument_types[0], result_type, budget))
                        return false;
                }
                else if (is_native_datetime_conversion)
                {
                    /// This exact subset has a separate range proof for UInt8/16/32 sources.
                    if (!isStableSourceForDateTimeModeConversion(argument_types[0], budget))
                        return false;
                }
                else if (!areNativeConversionsEquivalentForBloomFilterExpressionMatching(*function_base, *function_base))
                {
                    /// The self-comparison is an exact native semantic whitelist for context-independent
                    /// integer->UInt8/UInt64, numeric->Decimal and numeric/Decimal->String. FromString templates and every
                    /// unproved settings-capturing conversion fail closed here.
                    return false;
                }
            }

            if (typeid(*function_base) == typeid(FunctionCapture))
            {
                const auto & function_capture = static_cast<const FunctionCapture &>(*function_base);
                const auto & capture = function_capture.getCapture();
                const auto & body = function_capture.getActionsDAGForSemanticAnalysis();
                pending_dags.push_back(&body);
                /// Captures are separate children of FunctionCapture, but scan their declared
                /// types as well so malformed/missing capture edges fail closed.
                if (capture.captured_types.size() != node->children.size() || !budget.consume(capture.captured_types.size()))
                    return false;
                for (size_t i = 0; i < capture.captured_types.size(); ++i)
                    if (!node->children[i] || !node->children[i]->result_type
                        || !sameNativeTypeTree(node->children[i]->result_type, capture.captured_types[i], budget))
                        return false;
            }
            else if (node_result_type && typeid(*node_result_type) == typeid(DataTypeFunction))
            {
                /// An unrecognized callable carrier can hide a lambda ActionsDAG.
                return false;
            }
        }
        else if (node->type == ActionsDAG::ActionType::COLUMN && node_result_type && typeid(*node_result_type) == typeid(DataTypeFunction))
        {
            if (!node->column || !node->children.empty() || node->is_runtime_filter_id || node->is_masked_secret
                || !node->is_deterministic_constant)
                return false;
            const auto * column_const = typeid_cast<const ColumnConst *>(node->column.get());
            const auto * column_function = column_const ? typeid_cast<const ColumnFunction *>(&column_const->getDataColumn()) : nullptr;
            const auto * function_expression
                = column_function ? typeid_cast<const FunctionExpression *>(column_function->getFunction().get()) : nullptr;
            if (!column_function || !function_expression || typeid(*column_function) != typeid(ColumnFunction)
                || typeid(*function_expression) != typeid(FunctionExpression))
                return false;
            const auto & capture = function_expression->getCapture();
            pending_dags.push_back(&function_expression->getActionsDAGForSemanticAnalysis());

            const auto & captures = column_function->getCapturedColumns();
            if (captures.size() != capture.captured_types.size() || !budget.consume(captures.size()))
                return false;
            for (size_t i = 0; i < captures.size(); ++i)
            {
                const auto & captured_column = captures[i];
                if (!captured_column.type || !captured_column.column
                    || !sameNativeTypeTree(captured_column.type, capture.captured_types[i], budget))
                    return false;
            }
        }
        else if (node->type == ActionsDAG::ActionType::COLUMN && (node->has_folded_function || node->function_base))
        {
            /// A scalar COLUMN with folded function ancestry has lost argument edges, so context stability cannot be proved.
            return false;
        }
        else if (
            node->type != ActionsDAG::ActionType::INPUT && node->type != ActionsDAG::ActionType::COLUMN
            && node->type != ActionsDAG::ActionType::ALIAS && node->type != ActionsDAG::ActionType::ARRAY_JOIN
            && node->type != ActionsDAG::ActionType::PLACEHOLDER)
        {
            return false;
        }
    }

    return true;
}

enum class ExpressionMatchMode : UInt8
{
    DirectIN,
    ArrayJoinWrapper,
};

const ActionsDAG::Node * getNodeWithoutAliases(const RPNBuilderTreeNode & tree_node, ExpressionMatchBudget & budget)
{
    const ActionsDAG::Node * node = tree_node.getDAGNode();
    while (node && node->type == ActionsDAG::ActionType::ALIAS)
    {
        if (!budget.consume() || node->children.size() != 1)
            return nullptr;
        node = node->children[0];
    }
    return node;
}

struct DataTypePair
{
    const IDataType * left;
    const IDataType * right;

    bool operator==(const DataTypePair &) const = default;
};

struct DataTypePairHash
{
    size_t operator()(const DataTypePair & pair) const
    {
        const auto left_hash = std::hash<const IDataType *>{}(pair.left);
        const auto right_hash = std::hash<const IDataType *>{}(pair.right);
        return left_hash ^ (right_hash + 0x9e3779b9 + (left_hash << 6) + (left_hash >> 2));
    }
};

bool isSupportedLeafType(const IDataType & type)
{
    if (type.getCustomName())
        return false;

    return typeid(type) == typeid(DataTypeUInt8) || typeid(type) == typeid(DataTypeUInt16)
        || typeid(type) == typeid(DataTypeUInt32) || typeid(type) == typeid(DataTypeUInt64)
        || typeid(type) == typeid(DataTypeUInt128) || typeid(type) == typeid(DataTypeUInt256)
        || typeid(type) == typeid(DataTypeInt8) || typeid(type) == typeid(DataTypeInt16)
        || typeid(type) == typeid(DataTypeInt32) || typeid(type) == typeid(DataTypeInt64)
        || typeid(type) == typeid(DataTypeInt128) || typeid(type) == typeid(DataTypeInt256)
        || typeid(type) == typeid(DataTypeBFloat16) || typeid(type) == typeid(DataTypeFloat32)
        || typeid(type) == typeid(DataTypeFloat64) || typeid(type) == typeid(DataTypeString)
        || typeid(type) == typeid(DataTypeDate) || typeid(type) == typeid(DataTypeDate32)
        || typeid(type) == typeid(DataTypeUUID) || typeid(type) == typeid(DataTypeIPv4) || typeid(type) == typeid(DataTypeIPv6);
}

bool isNullableNothingType(const DataTypePtr & type)
{
    const auto * nullable_type = type ? typeid_cast<const DataTypeNullable *>(type.get()) : nullptr;
    if (!nullable_type || type->getCustomName() || type->getCustomSerialization())
        return false;

    const auto & nested_type = nullable_type->getNestedType();
    const IDataType * nested_type_raw = nested_type.get();
    return nested_type_raw && typeid(*nested_type_raw) == typeid(DataTypeNothing)
        && !nested_type->getCustomName() && !nested_type->getCustomSerialization();
}

bool isNativeBoolType(const IDataType & type)
{
    if (typeid(type) != typeid(DataTypeUInt8))
        return false;

    const auto & uint8_type = static_cast<const DataTypeUInt8 &>(type);
    if (uint8_type.canUnsignedBeSigned())
        return false;

    const auto * custom_serialization = type.getCustomSerialization();
    if (!custom_serialization || typeid(*custom_serialization) != typeid(SerializationBool))
        return false;

    const auto * nested_serialization = static_cast<const SerializationBool *>(custom_serialization)->getNested().get();
    if (!nested_serialization || typeid(*nested_serialization) != typeid(SerializationNumber<UInt8>))
        return false;

    const auto * custom_name = type.getCustomName();
    if (!custom_name || typeid(*custom_name) != typeid(DataTypeCustomFixedName))
        return false;

    return custom_name->getName() == "Bool";
}

template <typename T>
bool sameEnumType(const DataTypeEnum<T> & left, const DataTypeEnum<T> & right, ExpressionMatchBudget & budget)
{
    if (left.isAdd() || right.isAdd()
        || left.getRelativeFlagsSize() != 0 || right.getRelativeFlagsSize() != 0)
        return false;

    /// A shared normal Enum type is identical to itself; avoid rescanning its full map.
    if (&left == &right)
        return true;

    const auto & left_values = left.getValues();
    const auto & right_values = right.getValues();
    if (left_values.size() != right_values.size() || !budget.consume(left_values.size()))
        return false;

    for (size_t i = 0; i < left_values.size(); ++i)
    {
        const auto & [left_name, left_value] = left_values[i];
        const auto & [right_name, right_value] = right_values[i];
        if (!budget.consume(left_name.size()) || !budget.consume(right_name.size())
            || left_name != right_name || left_value != right_value)
            return false;
    }
    return true;
}

bool sameType(const DataTypePtr & left, const DataTypePtr & right, ExpressionMatchBudget & budget)
{
    if (!left || !right)
        return false;

    std::vector<std::pair<DataTypePtr, DataTypePtr>> pending;
    std::unordered_set<DataTypePair, DataTypePairHash> visited;
    pending.emplace_back(left, right);

    while (!pending.empty())
    {
        if (!budget.consume())
            return false;

        auto [left_type, right_type] = std::move(pending.back());
        pending.pop_back();
        if (!left_type || !right_type)
            return false;

        const IDataType * left_type_raw = left_type.get();
        const IDataType * right_type_raw = right_type.get();

        const bool left_is_native_bool = isNativeBoolType(*left_type);
        const bool right_is_native_bool = isNativeBoolType(*right_type);
        if (left_is_native_bool || right_is_native_bool)
        {
            if (!left_is_native_bool || !right_is_native_bool)
                return false;
            continue;
        }

        if (left_type->getCustomName() || right_type->getCustomName())
            return false;

        /// Function-typed arguments carry lambda captures outside the ordinary child structure.
        if (typeid_cast<const DataTypeFunction *>(left_type.get()) || typeid_cast<const DataTypeFunction *>(right_type.get()))
            return false;

        const auto * left_nullable_type = typeid_cast<const DataTypeNullable *>(left_type.get());
        const auto * right_nullable_type = typeid_cast<const DataTypeNullable *>(right_type.get());
        const IDataType * left_nullable_nested_type_raw = left_nullable_type
            ? left_nullable_type->getNestedType().get() : nullptr;
        const IDataType * right_nullable_nested_type_raw = right_nullable_type
            ? right_nullable_type->getNestedType().get() : nullptr;
        const bool left_nullable_nothing = left_nullable_type
            && left_nullable_nested_type_raw
            && typeid(*left_nullable_nested_type_raw) == typeid(DataTypeNothing);
        const bool right_nullable_nothing = right_nullable_type
            && right_nullable_nested_type_raw
            && typeid(*right_nullable_nested_type_raw) == typeid(DataTypeNothing);
        if (left_nullable_nothing || right_nullable_nothing)
        {
            if (!left_nullable_nothing || !right_nullable_nothing
                || !isNullableNothingType(left_type) || !isNullableNothingType(right_type)
                || !budget.consume())
                return false;
            continue;
        }

        const bool left_is_nothing = typeid(*left_type_raw) == typeid(DataTypeNothing);
        const bool right_is_nothing = typeid(*right_type_raw) == typeid(DataTypeNothing);
        if (left_is_nothing || right_is_nothing)
        {
            if (!left_is_nothing || !right_is_nothing
                || left_type->getCustomSerialization() || right_type->getCustomSerialization())
                return false;
            continue;
        }

        if (const auto * left_datetime64 = typeid_cast<const DataTypeDateTime64 *>(left_type.get()))
        {
            const auto * right_datetime64 = typeid_cast<const DataTypeDateTime64 *>(right_type.get());
            if (!right_datetime64
                || left_datetime64->getScale() != right_datetime64->getScale()
                || left_datetime64->hasExplicitTimeZone() != right_datetime64->hasExplicitTimeZone())
                return false;

            const auto & left_time_zone = left_datetime64->getTimeZone().getTimeZone();
            const auto & right_time_zone = right_datetime64->getTimeZone().getTimeZone();
            if (!budget.consume(left_time_zone.size()) || !budget.consume(right_time_zone.size())
                || left_time_zone != right_time_zone)
                return false;
            continue;
        }

        if (const auto * left_decimal32 = typeid_cast<const DataTypeDecimal32 *>(left_type.get()))
        {
            const auto * right_decimal32 = typeid_cast<const DataTypeDecimal32 *>(right_type.get());
            if (!right_decimal32 || left_decimal32->getPrecision() != right_decimal32->getPrecision()
                || left_decimal32->getScale() != right_decimal32->getScale())
                return false;
            continue;
        }
        if (const auto * left_decimal64 = typeid_cast<const DataTypeDecimal64 *>(left_type.get()))
        {
            const auto * right_decimal64 = typeid_cast<const DataTypeDecimal64 *>(right_type.get());
            if (!right_decimal64 || left_decimal64->getPrecision() != right_decimal64->getPrecision()
                || left_decimal64->getScale() != right_decimal64->getScale())
                return false;
            continue;
        }
        if (const auto * left_decimal128 = typeid_cast<const DataTypeDecimal128 *>(left_type.get()))
        {
            const auto * right_decimal128 = typeid_cast<const DataTypeDecimal128 *>(right_type.get());
            if (!right_decimal128 || left_decimal128->getPrecision() != right_decimal128->getPrecision()
                || left_decimal128->getScale() != right_decimal128->getScale())
                return false;
            continue;
        }
        if (const auto * left_decimal256 = typeid_cast<const DataTypeDecimal256 *>(left_type.get()))
        {
            const auto * right_decimal256 = typeid_cast<const DataTypeDecimal256 *>(right_type.get());
            if (!right_decimal256 || left_decimal256->getPrecision() != right_decimal256->getPrecision()
                || left_decimal256->getScale() != right_decimal256->getScale())
                return false;
            continue;
        }

        if (const auto * left_enum8 = typeid_cast<const DataTypeEnum8 *>(left_type.get()))
        {
            const auto * right_enum8 = typeid_cast<const DataTypeEnum8 *>(right_type.get());
            if (!right_enum8 || !sameEnumType(*left_enum8, *right_enum8, budget))
                return false;
            continue;
        }
        if (const auto * left_enum16 = typeid_cast<const DataTypeEnum16 *>(left_type.get()))
        {
            const auto * right_enum16 = typeid_cast<const DataTypeEnum16 *>(right_type.get());
            if (!right_enum16 || !sameEnumType(*left_enum16, *right_enum16, budget))
                return false;
            continue;
        }

        if (left_type.get() == right_type.get())
        {
            /// Pointer identity includes parameters such as a timezone or enum map. Walk exact shared
            /// containers too, so unsupported/custom descendants still decline before a match.
            const size_t children = left_type->getNumberOfChildren();
            if (children > budget.remaining || !budget.consume(children))
                return false;
            for (size_t i = 0; i < children; ++i)
                pending.emplace_back(left_type->getChild(i), right_type->getChild(i));
            continue;
        }

        if (typeid(*left_type_raw) != typeid(*right_type_raw))
            return false;

        const DataTypePair pair{left_type.get(), right_type.get()};
        if (!visited.emplace(pair).second)
            continue;

        if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left_type.get()))
        {
            const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right_type.get());
            if (!right_tuple)
                return false;

            const auto & left_elements = left_tuple->getElements();
            const auto & right_elements = right_tuple->getElements();
            const auto & left_names = left_tuple->getElementNames();
            const auto & right_names = right_tuple->getElementNames();
            if (left_elements.size() != right_elements.size()
                || left_tuple->hasExplicitNames() != right_tuple->hasExplicitNames()
                || left_names.size() != left_elements.size() || right_names.size() != right_elements.size()
                || !budget.consume(left_elements.size()))
                return false;

            for (size_t i = 0; i < left_elements.size(); ++i)
            {
                if (!budget.consume(left_names[i].size()) || !budget.consume(right_names[i].size())
                    || left_names[i] != right_names[i])
                    return false;
                pending.emplace_back(left_elements[i], right_elements[i]);
            }
            continue;
        }

        if (const auto * left_map = typeid_cast<const DataTypeMap *>(left_type.get()))
        {
            const auto * right_map = typeid_cast<const DataTypeMap *>(right_type.get());
            if (!right_map || left_map->getNumberOfChildren() != 2 || right_map->getNumberOfChildren() != 2
                || !budget.consume(2))
                return false;
            pending.emplace_back(left_map->getChild(0), right_map->getChild(0));
            pending.emplace_back(left_map->getChild(1), right_map->getChild(1));
            continue;
        }

        if (typeid_cast<const DataTypeArray *>(left_type.get())
            || typeid_cast<const DataTypeNullable *>(left_type.get())
            || typeid_cast<const DataTypeLowCardinality *>(left_type.get()))
        {
            if (left_type->getNumberOfChildren() != 1 || right_type->getNumberOfChildren() != 1 || !budget.consume())
                return false;
            pending.emplace_back(left_type->getChild(0), right_type->getChild(0));
            continue;
        }

        if (const auto * left_fixed_string = typeid_cast<const DataTypeFixedString *>(left_type.get()))
        {
            const auto * right_fixed_string = typeid_cast<const DataTypeFixedString *>(right_type.get());
            if (!right_fixed_string || left_fixed_string->getN() != right_fixed_string->getN())
                return false;
            continue;
        }

        if (const auto * left_datetime = typeid_cast<const DataTypeDateTime *>(left_type.get()))
        {
            const auto * right_datetime = typeid_cast<const DataTypeDateTime *>(right_type.get());
            if (!right_datetime
                || left_datetime->hasExplicitTimeZone() != right_datetime->hasExplicitTimeZone())
                return false;

            const auto & left_time_zone = left_datetime->getTimeZone().getTimeZone();
            const auto & right_time_zone = right_datetime->getTimeZone().getTimeZone();
            if (!budget.consume(left_time_zone.size()) || !budget.consume(right_time_zone.size())
                || left_time_zone != right_time_zone)
                return false;
            continue;
        }

        if (const auto * left_uint8 = typeid_cast<const DataTypeUInt8 *>(left_type.get()))
        {
            const auto * right_uint8 = typeid_cast<const DataTypeUInt8 *>(right_type.get());
            if (!right_uint8 || left_uint8->canUnsignedBeSigned() != right_uint8->canUnsignedBeSigned())
                return false;
            continue;
        }
        if (const auto * left_uint16 = typeid_cast<const DataTypeUInt16 *>(left_type.get()))
        {
            const auto * right_uint16 = typeid_cast<const DataTypeUInt16 *>(right_type.get());
            if (!right_uint16 || left_uint16->canUnsignedBeSigned() != right_uint16->canUnsignedBeSigned())
                return false;
            continue;
        }
        if (const auto * left_uint32 = typeid_cast<const DataTypeUInt32 *>(left_type.get()))
        {
            const auto * right_uint32 = typeid_cast<const DataTypeUInt32 *>(right_type.get());
            if (!right_uint32 || left_uint32->canUnsignedBeSigned() != right_uint32->canUnsignedBeSigned())
                return false;
            continue;
        }
        if (const auto * left_uint64 = typeid_cast<const DataTypeUInt64 *>(left_type.get()))
        {
            const auto * right_uint64 = typeid_cast<const DataTypeUInt64 *>(right_type.get());
            if (!right_uint64 || left_uint64->canUnsignedBeSigned() != right_uint64->canUnsignedBeSigned())
                return false;
            continue;
        }
        if (const auto * left_uint128 = typeid_cast<const DataTypeUInt128 *>(left_type.get()))
        {
            const auto * right_uint128 = typeid_cast<const DataTypeUInt128 *>(right_type.get());
            if (!right_uint128 || left_uint128->canUnsignedBeSigned() != right_uint128->canUnsignedBeSigned())
                return false;
            continue;
        }
        if (const auto * left_uint256 = typeid_cast<const DataTypeUInt256 *>(left_type.get()))
        {
            const auto * right_uint256 = typeid_cast<const DataTypeUInt256 *>(right_type.get());
            if (!right_uint256 || left_uint256->canUnsignedBeSigned() != right_uint256->canUnsignedBeSigned())
                return false;
            continue;
        }

        if (!isSupportedLeafType(*left_type) || !isSupportedLeafType(*right_type))
            return false;
    }

    return true;
}

bool sameFunctionType(const DataTypePtr & left, const DataTypePtr & right, ExpressionMatchBudget & budget)
{
    const auto * left_function = left ? typeid_cast<const DataTypeFunction *>(left.get()) : nullptr;
    const auto * right_function = right ? typeid_cast<const DataTypeFunction *>(right.get()) : nullptr;
    if (!left_function || !right_function)
        return false;

    const auto & left_arguments = left_function->getArgumentTypes();
    const auto & right_arguments = right_function->getArgumentTypes();
    const auto & left_result = left_function->getReturnType();
    const auto & right_result = right_function->getReturnType();
    if (left_arguments.size() != right_arguments.size() || !left_result || !right_result
        || !budget.consume(left_arguments.size()) || !sameType(left_result, right_result, budget))
        return false;

    for (size_t i = 0; i < left_arguments.size(); ++i)
    {
        if (typeid_cast<const DataTypeFunction *>(left_arguments[i].get())
            || typeid_cast<const DataTypeFunction *>(right_arguments[i].get())
            || !sameType(left_arguments[i], right_arguments[i], budget))
            return false;
    }

    return true;
}

bool isStringType(const DataTypePtr & type)
{
    return type && typeid_cast<const DataTypeString *>(type.get());
}

bool isArrayOfStringType(const DataTypePtr & type)
{
    const auto * array_type = type ? typeid_cast<const DataTypeArray *>(type.get()) : nullptr;
    return array_type && isStringType(array_type->getNestedType());
}

bool isSupportedBloomFilterArrayElementType(const DataTypePtr & type);

bool isSupportedBloomFilterArrayType(const DataTypePtr & type)
{
    const auto * array_type = type ? typeid_cast<const DataTypeArray *>(type.get()) : nullptr;
    return array_type && isSupportedBloomFilterArrayElementType(array_type->getNestedType());
}

bool compareColumnStringStorage(
    const ColumnString & left,
    const ColumnString & right,
    size_t expected_rows,
    ExpressionMatchBudget & budget)
{
    const auto & left_offsets = left.getOffsets();
    const auto & right_offsets = right.getOffsets();
    const auto & left_chars = left.getChars();
    const auto & right_chars = right.getChars();

    if (left_offsets.size() != expected_rows || right_offsets.size() != expected_rows
        || left_chars.size() > ExpressionMatchBudget::max_constant_bytes
        || right_chars.size() > ExpressionMatchBudget::max_constant_bytes
        || (!left_offsets.empty() && left_offsets.back() != left_chars.size())
        || (!right_offsets.empty() && right_offsets.back() != right_chars.size()))
        return false;

    if (!budget.consume(left_chars.size() + right_chars.size() + left_offsets.size() + right_offsets.size()))
        return false;

    return std::equal(left_offsets.begin(), left_offsets.end(), right_offsets.begin(), right_offsets.end())
        && std::equal(left_chars.begin(), left_chars.end(), right_chars.begin(), right_chars.end());
}

bool compareFixedWidthArrayStorage(
    const DataTypePtr & left_type,
    const DataTypePtr & right_type,
    const IColumn & left_data,
    const IColumn & right_data,
    ExpressionMatchBudget & budget)
{
    if (!isSupportedBloomFilterArrayType(left_type) || !isSupportedBloomFilterArrayType(right_type))
        return false;

    const auto * left_array = typeid_cast<const ColumnArray *>(&left_data);
    const auto * right_array = typeid_cast<const ColumnArray *>(&right_data);
    if (!left_array || !right_array)
        return false;

    const auto & left_offsets = left_array->getOffsets();
    const auto & right_offsets = right_array->getOffsets();
    if (left_offsets.size() != 1 || right_offsets.size() != 1)
        return false;

    const size_t left_size = left_offsets[0];
    const size_t right_size = right_offsets[0];
    const IColumn & left_nested = left_array->getData();
    const IColumn & right_nested = right_array->getData();
    if (left_size > ExpressionMatchBudget::max_array_elements || right_size > ExpressionMatchBudget::max_array_elements
        || left_size != right_size || left_size != left_nested.size() || right_size != right_nested.size()
        || !left_nested.isFixedAndContiguous() || !right_nested.isFixedAndContiguous())
        return false;

    const size_t left_value_size = left_nested.sizeOfValueIfFixed();
    const size_t right_value_size = right_nested.sizeOfValueIfFixed();
    if (left_value_size == 0 || left_value_size != right_value_size
        || left_size > ExpressionMatchBudget::max_constant_bytes / left_value_size)
        return false;

    const size_t expected_bytes = left_size * left_value_size;
    const auto left_raw = left_nested.getRawData();
    const auto right_raw = right_nested.getRawData();
    if (left_raw.size() != expected_bytes || right_raw.size() != expected_bytes
        || left_raw.size() > ExpressionMatchBudget::max_constant_bytes
        || right_raw.size() > ExpressionMatchBudget::max_constant_bytes
        || !budget.consume(2 + left_size + left_raw.size() + right_raw.size()))
        return false;

    return std::equal(left_raw.begin(), left_raw.end(), right_raw.begin(), right_raw.end());
}

template <typename T>
bool compareSupportedNumericConstant(
    const DataTypePtr & left_type,
    const DataTypePtr & right_type,
    const IColumn & left_data,
    const IColumn & right_data,
    ExpressionMatchBudget & budget)
{
    const auto * left_number_type = typeid_cast<const DataTypeNumber<T> *>(left_type.get());
    const auto * right_number_type = typeid_cast<const DataTypeNumber<T> *>(right_type.get());
    const auto * left_vector = typeid_cast<const ColumnVector<T> *>(&left_data);
    const auto * right_vector = typeid_cast<const ColumnVector<T> *>(&right_data);
    if (!left_number_type || !right_number_type
        || left_number_type->canUnsignedBeSigned() != right_number_type->canUnsignedBeSigned()
        || !left_vector || !right_vector || left_vector->size() != 1 || right_vector->size() != 1
        || !budget.consume(2 * sizeof(T)))
        return false;

    if constexpr (std::is_same_v<T, Float32>)
        return std::bit_cast<UInt32>(left_vector->getElement(0)) == std::bit_cast<UInt32>(right_vector->getElement(0));
    else if constexpr (std::is_same_v<T, Float64>)
        return std::bit_cast<UInt64>(left_vector->getElement(0)) == std::bit_cast<UInt64>(right_vector->getElement(0));
    else
        return std::memcmp(&left_vector->getElement(0), &right_vector->getElement(0), sizeof(T)) == 0;
}

bool compareSupportedNumericConstant(
    const DataTypePtr & left_type,
    const DataTypePtr & right_type,
    const IColumn & left_data,
    const IColumn & right_data,
    ExpressionMatchBudget & budget)
{
    const IDataType * left_raw_type = left_type.get();
    const IDataType * right_raw_type = right_type.get();
    if (!left_raw_type || !right_raw_type || typeid(*left_raw_type) != typeid(*right_raw_type))
        return false;

    if (typeid_cast<const DataTypeUInt8 *>(left_type.get()))
        return compareSupportedNumericConstant<UInt8>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeUInt16 *>(left_type.get()))
        return compareSupportedNumericConstant<UInt16>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeUInt32 *>(left_type.get()))
        return compareSupportedNumericConstant<UInt32>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeUInt64 *>(left_type.get()))
        return compareSupportedNumericConstant<UInt64>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeUInt128 *>(left_type.get()))
        return compareSupportedNumericConstant<UInt128>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeUInt256 *>(left_type.get()))
        return compareSupportedNumericConstant<UInt256>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeInt8 *>(left_type.get()))
        return compareSupportedNumericConstant<Int8>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeInt16 *>(left_type.get()))
        return compareSupportedNumericConstant<Int16>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeInt32 *>(left_type.get()))
        return compareSupportedNumericConstant<Int32>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeInt64 *>(left_type.get()))
        return compareSupportedNumericConstant<Int64>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeInt128 *>(left_type.get()))
        return compareSupportedNumericConstant<Int128>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeInt256 *>(left_type.get()))
        return compareSupportedNumericConstant<Int256>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeFloat32 *>(left_type.get()))
        return compareSupportedNumericConstant<Float32>(left_type, right_type, left_data, right_data, budget);
    if (typeid_cast<const DataTypeFloat64 *>(left_type.get()))
        return compareSupportedNumericConstant<Float64>(left_type, right_type, left_data, right_data, budget);
    return false;
}

bool isNativeArrayNothingType(const DataTypePtr & type)
{
    const auto * array_type = type ? typeid_cast<const DataTypeArray *>(type.get()) : nullptr;
    if (!array_type || typeid(*array_type) != typeid(DataTypeArray) || type->getCustomName() || type->getCustomSerialization()
        || array_type->getNumberOfChildren() != 1)
        return false;

    const auto & nested_type = array_type->getNestedType();
    const IDataType * nested_type_raw = nested_type.get();
    return nested_type_raw && typeid(*nested_type_raw) == typeid(DataTypeNothing) && !nested_type->getCustomName()
        && !nested_type->getCustomSerialization();
}

bool compareNativeArrayNothingConstants(
    const DataTypePtr & left_type,
    const IColumn & left_data,
    const DataTypePtr & right_type,
    const IColumn & right_data,
    ExpressionMatchBudget & budget)
{
    if (!isNativeArrayNothingType(left_type) || !isNativeArrayNothingType(right_type) || !sameType(left_type, right_type, budget))
        return false;

    const auto * left_array = typeid_cast<const ColumnArray *>(&left_data);
    const auto * right_array = typeid_cast<const ColumnArray *>(&right_data);
    if (!left_array || !right_array || typeid(left_data) != typeid(ColumnArray) || typeid(right_data) != typeid(ColumnArray)
        || left_array->size() != 1 || right_array->size() != 1 || left_array->getOffsets().size() != 1
        || right_array->getOffsets().size() != 1 || left_array->getOffsets()[0] != 0 || right_array->getOffsets()[0] != 0)
        return false;

    const IColumn * left_nested = left_array->getDataPtr().get();
    const IColumn * right_nested = right_array->getDataPtr().get();
    if (!left_nested || !right_nested || typeid(*left_nested) != typeid(ColumnNothing) || typeid(*right_nested) != typeid(ColumnNothing)
        || left_nested->size() != 0 || right_nested->size() != 0 || !budget.consume(4))
        return false;

    return true;
}

bool isSupportedTupleConstantLeafType(const DataTypePtr & type)
{
    const IDataType * type_raw = type.get();
    if (!type_raw || type_raw->getCustomName() || type_raw->getCustomSerialization())
        return false;

    return isStringType(type) || typeid(*type_raw) == typeid(DataTypeUInt8) || typeid(*type_raw) == typeid(DataTypeUInt16)
        || typeid(*type_raw) == typeid(DataTypeUInt32) || typeid(*type_raw) == typeid(DataTypeUInt64)
        || typeid(*type_raw) == typeid(DataTypeUInt128) || typeid(*type_raw) == typeid(DataTypeUInt256)
        || typeid(*type_raw) == typeid(DataTypeInt8) || typeid(*type_raw) == typeid(DataTypeInt16)
        || typeid(*type_raw) == typeid(DataTypeInt32) || typeid(*type_raw) == typeid(DataTypeInt64)
        || typeid(*type_raw) == typeid(DataTypeInt128) || typeid(*type_raw) == typeid(DataTypeInt256)
        || typeid(*type_raw) == typeid(DataTypeFloat32) || typeid(*type_raw) == typeid(DataTypeFloat64);
}

bool compareSupportedTupleLeafValues(
    const DataTypePtr & left_type,
    const IColumn & left_data,
    const DataTypePtr & right_type,
    const IColumn & right_data,
    ExpressionMatchBudget & budget)
{
    if (!isSupportedTupleConstantLeafType(left_type) || !isSupportedTupleConstantLeafType(right_type)
        || !sameType(left_type, right_type, budget))
        return false;

    if (isStringType(left_type))
    {
        const auto * left_string = typeid_cast<const ColumnString *>(&left_data);
        const auto * right_string = typeid_cast<const ColumnString *>(&right_data);
        return left_string && right_string && compareColumnStringStorage(*left_string, *right_string, 1, budget);
    }

    return compareSupportedNumericConstant(left_type, right_type, left_data, right_data, budget);
}

bool compareOneLevelTupleConstants(
    const DataTypePtr & left_type,
    const IColumn & left_data,
    const DataTypePtr & right_type,
    const IColumn & right_data,
    ExpressionMatchBudget & budget)
{
    const auto * left_tuple_type = left_type ? typeid_cast<const DataTypeTuple *>(left_type.get()) : nullptr;
    const auto * right_tuple_type = right_type ? typeid_cast<const DataTypeTuple *>(right_type.get()) : nullptr;
    if (!left_tuple_type || !right_tuple_type || left_type->getCustomName() || right_type->getCustomName()
        || left_type->getCustomSerialization() || right_type->getCustomSerialization() || !sameType(left_type, right_type, budget))
        return false;

    const auto & left_element_types = left_tuple_type->getElements();
    const auto & right_element_types = right_tuple_type->getElements();
    if (left_element_types.empty() || left_element_types.size() != right_element_types.size())
        return false;

    const auto * left_tuple = typeid_cast<const ColumnTuple *>(&left_data);
    const auto * right_tuple = typeid_cast<const ColumnTuple *>(&right_data);
    if (!left_tuple || !right_tuple || typeid(left_data) != typeid(ColumnTuple) || typeid(right_data) != typeid(ColumnTuple)
        || left_tuple->size() != 1 || right_tuple->size() != 1 || left_tuple->getColumns().size() != left_element_types.size()
        || right_tuple->getColumns().size() != right_element_types.size() || !budget.consume(left_element_types.size()))
        return false;

    for (size_t i = 0; i < left_element_types.size(); ++i)
    {
        const IColumn & left_element = left_tuple->getColumn(i);
        const IColumn & right_element = right_tuple->getColumn(i);
        if (left_element.size() != 1 || right_element.size() != 1
            || !compareSupportedTupleLeafValues(left_element_types[i], left_element, right_element_types[i], right_element, budget))
            return false;
    }

    return true;
}

bool compareSupportedConstantValues(
    const DataTypePtr & left_type,
    const IColumn & left_data,
    const DataTypePtr & right_type,
    const IColumn & right_data,
    ExpressionMatchBudget & budget)
{
    const bool left_is_nullable_nothing = isNullableNothingType(left_type);
    const bool right_is_nullable_nothing = isNullableNothingType(right_type);
    if (left_is_nullable_nothing || right_is_nullable_nothing)
    {
        if (!left_is_nullable_nothing || !right_is_nullable_nothing || !budget.consume(8))
            return false;

        const auto * left_nullable_column = typeid_cast<const ColumnNullable *>(&left_data);
        const auto * right_nullable_column = typeid_cast<const ColumnNullable *>(&right_data);
        if (!left_nullable_column || !right_nullable_column
            || typeid(left_data) != typeid(ColumnNullable) || typeid(right_data) != typeid(ColumnNullable))
            return false;

        const IColumn * left_nested_column = left_nullable_column->getNestedColumnPtr().get();
        const IColumn * right_nested_column = right_nullable_column->getNestedColumnPtr().get();
        const IColumn * left_null_map_column = left_nullable_column->getNullMapColumnPtr().get();
        const IColumn * right_null_map_column = right_nullable_column->getNullMapColumnPtr().get();
        if (!left_nested_column || !right_nested_column || !left_null_map_column || !right_null_map_column
            || typeid(*left_nested_column) != typeid(ColumnNothing)
            || typeid(*right_nested_column) != typeid(ColumnNothing)
            || typeid(*left_null_map_column) != typeid(ColumnUInt8)
            || typeid(*right_null_map_column) != typeid(ColumnUInt8))
            return false;

        const auto & left_null_map = left_nullable_column->getNullMapData();
        const auto & right_null_map = right_nullable_column->getNullMapData();
        return left_nullable_column->size() == 1 && right_nullable_column->size() == 1
            && left_nested_column->size() == 1 && right_nested_column->size() == 1
            && left_null_map.size() == 1 && right_null_map.size() == 1
            && left_null_map[0] == ColumnNullable::IS_NULL_MASK
            && right_null_map[0] == ColumnNullable::IS_NULL_MASK;
    }

    const bool left_is_tuple = left_type && typeid_cast<const DataTypeTuple *>(left_type.get());
    const bool right_is_tuple = right_type && typeid_cast<const DataTypeTuple *>(right_type.get());
    if (left_is_tuple || right_is_tuple)
        return left_is_tuple && right_is_tuple && compareOneLevelTupleConstants(left_type, left_data, right_type, right_data, budget);

    const bool left_is_array_nothing = isNativeArrayNothingType(left_type);
    const bool right_is_array_nothing = isNativeArrayNothingType(right_type);
    if (left_is_array_nothing || right_is_array_nothing)
        return left_is_array_nothing && right_is_array_nothing
            && compareNativeArrayNothingConstants(left_type, left_data, right_type, right_data, budget);

    if (compareSupportedNumericConstant(left_type, right_type, left_data, right_data, budget))
        return true;

    if (isStringType(left_type) && isStringType(right_type))
    {
        const auto * left_string = typeid_cast<const ColumnString *>(&left_data);
        const auto * right_string = typeid_cast<const ColumnString *>(&right_data);
        return left_string && right_string
            && compareColumnStringStorage(*left_string, *right_string, 1, budget);
    }

    if (isArrayOfStringType(left_type) && isArrayOfStringType(right_type))
    {
        const auto * left_array = typeid_cast<const ColumnArray *>(&left_data);
        const auto * right_array = typeid_cast<const ColumnArray *>(&right_data);
        if (!left_array || !right_array || left_array->getOffsets().size() != 1 || right_array->getOffsets().size() != 1)
            return false;

        const size_t left_size = left_array->getOffsets()[0];
        const size_t right_size = right_array->getOffsets()[0];
        if (left_size > ExpressionMatchBudget::max_array_elements || right_size > ExpressionMatchBudget::max_array_elements
            || left_size != right_size || !budget.consume(2))
            return false;

        const auto * left_strings = typeid_cast<const ColumnString *>(left_array->getDataPtr().get());
        const auto * right_strings = typeid_cast<const ColumnString *>(right_array->getDataPtr().get());
        return left_strings && right_strings
            && compareColumnStringStorage(*left_strings, *right_strings, left_size, budget);
    }

    if (compareFixedWidthArrayStorage(left_type, right_type, left_data, right_data, budget))
        return true;

    return false;
}

bool compareSupportedConstant(
    const ActionsDAG::Node & left,
    const ActionsDAG::Node & right,
    ExpressionMatchBudget & budget)
{
    if (!left.column || !right.column || left.is_runtime_filter_id || right.is_runtime_filter_id
        || left.is_masked_secret || right.is_masked_secret
        || !left.is_deterministic_constant || !right.is_deterministic_constant)
        return false;

    const auto * left_constant = typeid_cast<const ColumnConst *>(left.column.get());
    const auto * right_constant = typeid_cast<const ColumnConst *>(right.column.get());
    if (!left_constant || !right_constant)
        return false;

    /// ActionsDAG stores ColumnConst values at size 0; compare the single underlying value instead.
    return compareSupportedConstantValues(
        left.result_type, left_constant->getDataColumn(), right.result_type, right_constant->getDataColumn(), budget);
}

bool sameBoundedName(const String & left, const String & right, ExpressionMatchBudget & budget)
{
    return left.size() == right.size() && budget.consume(left.size()) && budget.consume(right.size()) && left == right;
}

enum class LambdaCarrierKind : UInt8
{
    FunctionCapture,
    ColumnFunction,
};

struct LambdaCarrierView
{
    LambdaCarrierKind kind = LambdaCarrierKind::FunctionCapture;
    const LambdaCapture * capture = nullptr;
    const ActionsDAG * body = nullptr;
    const ActionsDAG::Node * node = nullptr;
    const ColumnFunction * column_function = nullptr;
};

bool getLambdaCarrierView(const ActionsDAG::Node & node, LambdaCarrierView & view)
{
    if (node.type == ActionsDAG::ActionType::FUNCTION && !node.is_function_compiled && node.function_base)
    {
        const auto * function_capture = typeid_cast<const FunctionCapture *>(node.function_base.get());
        if (!function_capture)
            return false;

        view
            = {LambdaCarrierKind::FunctionCapture,
               &function_capture->getCapture(),
               &function_capture->getActionsDAGForSemanticAnalysis(),
               &node};
        return true;
    }

    if (node.type == ActionsDAG::ActionType::COLUMN && node.column && node.children.empty())
    {
        if (node.is_runtime_filter_id || node.is_masked_secret || !node.is_deterministic_constant)
            return false;

        const auto * column_const = typeid_cast<const ColumnConst *>(node.column.get());
        const auto * column_function = column_const ? typeid_cast<const ColumnFunction *>(&column_const->getDataColumn()) : nullptr;
        const auto * function_expression = column_function
            ? typeid_cast<const FunctionExpression *>(column_function->getFunction().get())
            : nullptr;
        if (!column_function || !function_expression)
            return false;

        view
            = {LambdaCarrierKind::ColumnFunction,
               &function_expression->getCapture(),
               &function_expression->getActionsDAGForSemanticAnalysis(),
               &node,
               column_function};
        return true;
    }

    return false;
}

bool sameLambdaMetadata(const LambdaCapture & left, const LambdaCapture & right, ExpressionMatchBudget & budget)
{
    if (left.captured_names.size() != right.captured_names.size()
        || left.captured_types.size() != right.captured_types.size()
        || left.captured_names.size() != left.captured_types.size()
        || right.captured_names.size() != right.captured_types.size()
        || left.lambda_arguments.size() != right.lambda_arguments.size()
        || !budget.consume(left.captured_names.size()) || !budget.consume(right.captured_names.size())
        || !budget.consume(left.lambda_arguments.size()) || !budget.consume(right.lambda_arguments.size())
        || !sameType(left.return_type, right.return_type, budget))
        return false;

    for (size_t i = 0; i < left.captured_names.size(); ++i)
    {
        if (typeid_cast<const DataTypeFunction *>(left.captured_types[i].get())
            || typeid_cast<const DataTypeFunction *>(right.captured_types[i].get())
            || !sameType(left.captured_types[i], right.captured_types[i], budget))
            return false;
    }

    auto left_argument = left.lambda_arguments.begin();
    auto right_argument = right.lambda_arguments.begin();
    for (; left_argument != left.lambda_arguments.end() && right_argument != right.lambda_arguments.end();
         ++left_argument, ++right_argument)
    {
        if (!sameBoundedName(left_argument->name, right_argument->name, budget)
            || typeid_cast<const DataTypeFunction *>(left_argument->type.get())
            || typeid_cast<const DataTypeFunction *>(right_argument->type.get())
            || !sameType(left_argument->type, right_argument->type, budget))
            return false;
    }

    return true;
}

bool addLambdaNameSlot(
    std::unordered_map<String, LambdaInputSlot> & slots,
    const String & name,
    LambdaInputSlot slot,
    ExpressionMatchBudget & budget)
{
    return budget.consume() && budget.consume(name.size()) && slots.emplace(name, slot).second;
}

bool mapLambdaBodyInputs(
    const ActionsDAG::Nodes & nodes,
    const ActionsDAG::NodeRawConstPtrs & inputs,
    const LambdaCapture & capture,
    std::unordered_map<const ActionsDAG::Node *, LambdaInputSlot> & bindings,
    ExpressionMatchBudget & budget)
{
    std::unordered_map<String, LambdaInputSlot> slots;
    std::vector<UInt8> seen_arguments(capture.lambda_arguments.size(), 0);
    std::vector<UInt8> seen_captures(capture.captured_names.size(), 0);
    std::vector<DataTypePtr> argument_types;
    if (!budget.consume(capture.lambda_arguments.size()))
        return false;
    argument_types.reserve(capture.lambda_arguments.size());

    auto argument = capture.lambda_arguments.begin();
    for (size_t i = 0; i < capture.lambda_arguments.size(); ++i, ++argument)
    {
        argument_types.push_back(argument->type);
        if (!addLambdaNameSlot(slots, argument->name, {LambdaInputKind::Argument, i}, budget))
            return false;
    }

    for (size_t i = 0; i < capture.captured_names.size(); ++i)
        if (!addLambdaNameSlot(slots, capture.captured_names[i], {LambdaInputKind::Capture, i}, budget))
            return false;

    if (!budget.consume(nodes.size()))
        return false;

    for (const auto & node : nodes)
    {
        if (node.type != ActionsDAG::ActionType::INPUT)
            continue;

        if (!node.children.empty() || !budget.consume() || !budget.consume(node.result_name.size()))
            return false;

        const auto slot = slots.find(node.result_name);
        if (slot == slots.end())
            return false;

        const DataTypePtr & expected_type = slot->second.kind == LambdaInputKind::Argument
            ? argument_types[slot->second.index]
            : capture.captured_types[slot->second.index];
        if (!sameType(node.result_type, expected_type, budget))
            return false;

        if (slot->second.kind == LambdaInputKind::Argument)
            seen_arguments[slot->second.index] = 1;
        else
            seen_captures[slot->second.index] = 1;
        if (!bindings.emplace(&node, slot->second).second)
            return false;
    }

    if (inputs.size() != bindings.size() || !budget.consume(inputs.size()))
        return false;

    std::unordered_set<const ActionsDAG::Node *> registered_inputs;
    for (const auto * input : inputs)
    {
        if (!input || !bindings.contains(input) || !registered_inputs.emplace(input).second)
            return false;
    }

    return std::ranges::all_of(seen_arguments, [](UInt8 seen) { return seen != 0; })
        && std::ranges::all_of(seen_captures, [](UInt8 seen) { return seen != 0; });
}

bool containsLambdaBodyNode(const ActionsDAG::Nodes & nodes, const ActionsDAG::Node * expected, ExpressionMatchBudget & budget)
{
    if (!expected || !budget.consume(nodes.size()))
        return false;

    for (const auto & node : nodes)
        if (&node == expected)
            return true;
    return false;
}

bool compareLambdaCarriers(
    const ActionsDAG::Node & left,
    const ActionsDAG::Node & right,
    ExpressionMatchBudget & budget,
    std::vector<ExpressionNodePair> & pending)
{
    LambdaCarrierView left_view{};
    LambdaCarrierView right_view{};
    if (!getLambdaCarrierView(left, left_view) || !getLambdaCarrierView(right, right_view)
        || !sameFunctionType(left.result_type, right.result_type, budget)
        || !sameLambdaMetadata(*left_view.capture, *right_view.capture, budget))
        return false;

    const auto * left_signature = typeid_cast<const DataTypeFunction *>(left.result_type.get());
    const auto * right_signature = typeid_cast<const DataTypeFunction *>(right.result_type.get());
    if (!left_signature || !right_signature
        || left_signature->getArgumentTypes().size() != left_view.capture->lambda_arguments.size()
        || right_signature->getArgumentTypes().size() != right_view.capture->lambda_arguments.size())
        return false;

    auto left_lambda_argument = left_view.capture->lambda_arguments.begin();
    auto right_lambda_argument = right_view.capture->lambda_arguments.begin();
    for (size_t i = 0; i < left_view.capture->lambda_arguments.size();
         ++i, ++left_lambda_argument, ++right_lambda_argument)
    {
        if (!sameType(left_signature->getArgumentTypes()[i], left_lambda_argument->type, budget)
            || !sameType(right_signature->getArgumentTypes()[i], right_lambda_argument->type, budget))
            return false;
    }
    if (!sameType(left_signature->getReturnType(), left_view.capture->return_type, budget)
        || !sameType(right_signature->getReturnType(), right_view.capture->return_type, budget))
        return false;

    const auto & left_nodes = left_view.body->getNodes();
    const auto & right_nodes = right_view.body->getNodes();
    const auto & left_outputs = left_view.body->getOutputs();
    const auto & right_outputs = right_view.body->getOutputs();
    if (left_nodes.size() != right_nodes.size() || left_outputs.size() != 1 || right_outputs.size() != 1
        || !left_outputs.front() || !right_outputs.front()
        || !containsLambdaBodyNode(left_nodes, left_outputs.front(), budget)
        || !containsLambdaBodyNode(right_nodes, right_outputs.front(), budget)
        || !sameBoundedName(left_outputs.front()->result_name, left_view.capture->return_name, budget)
        || !sameBoundedName(right_outputs.front()->result_name, right_view.capture->return_name, budget)
        || !sameType(left_outputs.front()->result_type, left_signature->getReturnType(), budget)
        || !sameType(right_outputs.front()->result_type, right_signature->getReturnType(), budget))
        return false;

    auto body_binding = std::make_shared<LambdaBodyBinding>();
    if (!mapLambdaBodyInputs(left_nodes, left_view.body->getInputs(), *left_view.capture, body_binding->query_inputs, budget)
        || !mapLambdaBodyInputs(right_nodes, right_view.body->getInputs(), *right_view.capture, body_binding->index_inputs, budget))
        return false;

    const auto validate_function_capture = [&](const ActionsDAG::Node & node, const LambdaCapture & capture)
    {
        const auto * function_capture = typeid_cast<const FunctionCapture *>(node.function_base.get());
        if (!function_capture || node.children.size() != capture.captured_names.size()
            || function_capture->getArgumentTypes().size() != capture.captured_types.size()
            || !budget.consume(node.children.size()))
            return false;

        for (size_t i = 0; i < node.children.size(); ++i)
        {
            if (!node.children[i]
                || !sameType(capture.captured_types[i], node.children[i]->result_type, budget)
                || !sameType(function_capture->getArgumentTypes()[i], capture.captured_types[i], budget))
                return false;
        }
        return true;
    };

    if (left_view.kind == LambdaCarrierKind::FunctionCapture
        && !validate_function_capture(left, *left_view.capture))
        return false;
    if (right_view.kind == LambdaCarrierKind::FunctionCapture
        && !validate_function_capture(right, *right_view.capture))
        return false;

    if (left_view.kind == LambdaCarrierKind::FunctionCapture
        && right_view.kind == LambdaCarrierKind::FunctionCapture)
    {
        for (size_t i = 0; i < left.children.size(); ++i)
            pending.emplace_back(left.children[i], right.children[i], false, true);
    }
    else
    {
        const auto * left_column_function = left_view.kind == LambdaCarrierKind::ColumnFunction ? left_view.column_function : nullptr;
        const auto * right_column_function = right_view.kind == LambdaCarrierKind::ColumnFunction ? right_view.column_function : nullptr;
        const auto * left_captured = left_column_function ? &left_column_function->getCapturedColumns() : nullptr;
        const auto * right_captured = right_column_function ? &right_column_function->getCapturedColumns() : nullptr;
        if ((left_captured && left_captured->size() != left_view.capture->captured_names.size())
            || (right_captured && right_captured->size() != right_view.capture->captured_names.size())
            || (left_captured && !budget.consume(left_captured->size()))
            || (right_captured && !budget.consume(right_captured->size())))
            return false;

        for (size_t i = 0; i < left_view.capture->captured_names.size(); ++i)
        {
            const auto * left_child = left_column_function ? nullptr : left.children[i];
            const auto * right_child = right_column_function ? nullptr : right.children[i];
            const auto * left_column = left_captured ? &(*left_captured)[i] : nullptr;
            const auto * right_column = right_captured ? &(*right_captured)[i] : nullptr;
            const auto * left_constant = left_column && left_column->column
                ? typeid_cast<const ColumnConst *>(left_column->column.get()) : nullptr;
            const auto * right_constant = right_column && right_column->column
                ? typeid_cast<const ColumnConst *>(right_column->column.get()) : nullptr;

            if ((left_column && (!left_constant || !sameType(left_view.capture->captured_types[i], left_column->type, budget)))
                || (right_column && (!right_constant || !sameType(right_view.capture->captured_types[i], right_column->type, budget))))
                return false;

            if (left_child && right_child)
            {
                pending.emplace_back(left_child, right_child, false, true);
                continue;
            }

            if (left_child && (left_child->type != ActionsDAG::ActionType::COLUMN
                    || !compareSupportedConstant(*left_child, *left_child, budget)))
                return false;
            if (right_child && (right_child->type != ActionsDAG::ActionType::COLUMN
                    || !compareSupportedConstant(*right_child, *right_child, budget)))
                return false;

            const DataTypePtr & left_type = left_column ? left_column->type : left_child->result_type;
            const DataTypePtr & right_type = right_column ? right_column->type : right_child->result_type;
            const auto * left_child_constant = left_child && left_child->column
                ? typeid_cast<const ColumnConst *>(left_child->column.get()) : nullptr;
            const auto * right_child_constant = right_child && right_child->column
                ? typeid_cast<const ColumnConst *>(right_child->column.get()) : nullptr;
            if ((left_child && !left_child_constant) || (right_child && !right_child_constant))
                return false;

            const IColumn & left_value = left_column ? left_constant->getDataColumn()
                : left_child_constant->getDataColumn();
            const IColumn & right_value = right_column ? right_constant->getDataColumn()
                : right_child_constant->getDataColumn();
            if (!sameType(left_type, right_type, budget)
                || !compareSupportedConstantValues(left_type, left_value, right_type, right_value, budget))
                return false;
        }
    }

    if (!budget.consume(left_nodes.size()) || !budget.consume(right_nodes.size()) || !budget.consume(left_outputs.size()))
        return false;

    auto left_node = left_nodes.begin();
    auto right_node = right_nodes.begin();
    for (; left_node != left_nodes.end() && right_node != right_nodes.end(); ++left_node, ++right_node)
        pending.emplace_back(&*left_node, &*right_node, false, true, body_binding);
    pending.emplace_back(left_outputs.front(), right_outputs.front(), false, true, body_binding);

    return true;
}

const FunctionCreator * getCanonicalArrayFunctionHandle(const String & function_name)
{
    return FunctionFactory::instance().tryGetCanonicalCreator(function_name);
}

const char * getAuditedDirectFunctionName(const FunctionCreator * handle)
{
    /// This is deliberately a closed set. A registered function handle is not enough to
    /// certify an empty/default semantic hash: its implementation must be admitted here.
    static constexpr std::array<const char *, 32> names{
        "lower",        "toYear",       "plus",          "minus",       "modulo",         "length",   "arrayReverse", "arraySort",
        "tuple",        "cityHash64",   "arraySlice",    "arrayResize", "arrayIntersect", "mapKeys",  "if",           "isNull",
        "arraySum",     "arrayMap",     "arrayDistinct", "arrayConcat", "CAST",           "toString", "toDecimal32",  "toDecimal64",
        "toDecimal128", "toDecimal256", "abs",           "negate",      "sign",           "bitNot",   "toUInt8",      "toUInt64"};

    if (!handle)
        return nullptr;

    const auto & factory = FunctionFactory::instance();
    for (const char * name : names)
        if (factory.tryGetCanonicalCreator(name) == handle)
            return name;
    return nullptr;
}

bool isPrimitiveIntegerType(const DataTypePtr & type)
{
    const IDataType * type_raw = type.get();
    if (!type_raw || type_raw->getCustomName() || type_raw->getCustomSerialization())
        return false;

    return typeid(*type_raw) == typeid(DataTypeUInt8) || typeid(*type_raw) == typeid(DataTypeUInt16)
        || typeid(*type_raw) == typeid(DataTypeUInt32) || typeid(*type_raw) == typeid(DataTypeUInt64)
        || typeid(*type_raw) == typeid(DataTypeUInt128) || typeid(*type_raw) == typeid(DataTypeUInt256)
        || typeid(*type_raw) == typeid(DataTypeInt8) || typeid(*type_raw) == typeid(DataTypeInt16)
        || typeid(*type_raw) == typeid(DataTypeInt32) || typeid(*type_raw) == typeid(DataTypeInt64)
        || typeid(*type_raw) == typeid(DataTypeInt128) || typeid(*type_raw) == typeid(DataTypeInt256);
}

bool isPrimitiveNumericType(const DataTypePtr & type)
{
    const IDataType * type_raw = type.get();
    return isPrimitiveIntegerType(type)
        || (type_raw && !type_raw->getCustomName() && !type_raw->getCustomSerialization()
            && (typeid(*type_raw) == typeid(DataTypeFloat32) || typeid(*type_raw) == typeid(DataTypeFloat64)));
}

bool isDecimalType(const DataTypePtr & type)
{
    const IDataType * type_raw = type.get();
    return type_raw && !type_raw->getCustomName() && !type_raw->getCustomSerialization()
        && (typeid(*type_raw) == typeid(DataTypeDecimal32) || typeid(*type_raw) == typeid(DataTypeDecimal64)
            || typeid(*type_raw) == typeid(DataTypeDecimal128) || typeid(*type_raw) == typeid(DataTypeDecimal256));
}

bool isSupportedTupleValueType(const DataTypePtr & type, size_t depth, ExpressionMatchBudget & budget)
{
    if (!type || type->getCustomName() || type->getCustomSerialization() || !budget.consume())
        return false;

    if (isSupportedLeafType(*type))
        return true;

    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
    if (!tuple_type || depth == 0 || tuple_type->getElements().empty() || !budget.consume(tuple_type->getElements().size()))
        return false;

    for (const auto & element_type : tuple_type->getElements())
        if (!isSupportedTupleValueType(element_type, depth - 1, budget))
            return false;
    return true;
}

bool isSupportedIfValueType(const DataTypePtr & type)
{
    if (!type || type->getCustomName() || type->getCustomSerialization())
        return false;
    if (isSupportedLeafType(*type))
        return true;
    if (isSupportedBloomFilterArrayType(type))
        return true;
    if (isNullableNothingType(type))
        return true;
    const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get());
    return nullable_type && isSupportedLeafType(*nullable_type->getNestedType());
}

bool isNativeStringType(const DataTypePtr & type)
{
    const IDataType * type_raw = type.get();
    return type_raw && typeid(*type_raw) == typeid(DataTypeString) && !type_raw->getCustomName() && !type_raw->getCustomSerialization();
}

bool isSupportedIfNullableStringPromotion(
    const DataTypePtr & first, const DataTypePtr & second, const DataTypePtr & result, ExpressionMatchBudget & budget)
{
    const DataTypePtr * string_branch = nullptr;
    if (isNullableNothingType(first) && isNativeStringType(second))
        string_branch = &second;
    else if (isNativeStringType(first) && isNullableNothingType(second))
        string_branch = &first;
    else
        return false;

    const IDataType * result_raw = result.get();
    const auto * nullable_result = result_raw ? typeid_cast<const DataTypeNullable *>(result_raw) : nullptr;
    if (!nullable_result || typeid(*result_raw) != typeid(DataTypeNullable) || result_raw->getCustomName()
        || result_raw->getCustomSerialization())
        return false;

    const DataTypePtr & nested_result = nullable_result->getNestedType();
    return isNativeStringType(nested_result) && sameType(*string_branch, nested_result, budget);
}

bool isSupportedArrayMapInputType(const DataTypePtr & type)
{
    const auto * array_type
        = type && !type->getCustomName() && !type->getCustomSerialization() ? typeid_cast<const DataTypeArray *>(type.get()) : nullptr;
    if (!array_type)
        return false;

    if (isSupportedBloomFilterArrayType(type))
        return true;

    const auto nested_type = array_type->getNestedType();
    return nested_type && !nested_type->getCustomName() && !nested_type->getCustomSerialization()
        && isSupportedBloomFilterArrayType(nested_type);
}

bool isSupportedDirectFunctionSignature(const String & name, const IFunctionBase & function, ExpressionMatchBudget & budget)
{
    const auto & arguments = function.getArgumentTypes();
    const auto & result = function.getResultType();
    const auto is_array = [](const DataTypePtr & type) { return isSupportedBloomFilterArrayType(type); };
    const auto same_argument_and_result
        = [&](size_t argument) { return argument < arguments.size() && sameType(arguments[argument], result, budget); };

    /// This guard excludes negate's context-dispatched tupleNegate path and every non-integer overload.
    if (name == "abs" || name == "negate" || name == "sign" || name == "bitNot")
        return arguments.size() == 1 && isPrimitiveIntegerType(arguments[0]) && isPrimitiveIntegerType(result);
    if (name == "toUInt8")
        return arguments.size() == 1 && isPrimitiveIntegerType(arguments[0]) && typeid_cast<const DataTypeUInt8 *>(result.get())
            && !result->getCustomName() && !result->getCustomSerialization();
    if (name == "toUInt64")
        return arguments.size() == 1 && isPrimitiveIntegerType(arguments[0]) && typeid_cast<const DataTypeUInt64 *>(result.get())
            && !result->getCustomName() && !result->getCustomSerialization();
    if (name == "lower")
        return arguments.size() == 1 && isStringType(arguments[0]) && isStringType(result);
    if (name == "toYear")
    {
        if (arguments.size() != 1 || !typeid_cast<const DataTypeUInt16 *>(result.get()))
            return false;
        if (typeid_cast<const DataTypeDate *>(arguments[0].get()) || typeid_cast<const DataTypeDate32 *>(arguments[0].get()))
            return true;
        const auto * datetime = typeid_cast<const DataTypeDateTime *>(arguments[0].get());
        return datetime && datetime->hasExplicitTimeZone();
    }
    if (name == "plus")
        return arguments.size() == 2 && isPrimitiveNumericType(arguments[0]) && isPrimitiveNumericType(arguments[1])
            && isPrimitiveNumericType(result);
    if (name == "minus")
        return arguments.size() == 2 && isPrimitiveIntegerType(arguments[0]) && isPrimitiveIntegerType(arguments[1])
            && isPrimitiveIntegerType(result);
    if (name == "modulo")
        return arguments.size() == 2 && isPrimitiveIntegerType(arguments[0]) && isPrimitiveIntegerType(arguments[1])
            && isPrimitiveIntegerType(result);
    if (name == "length")
        return arguments.size() == 1 && isArrayOfStringType(arguments[0]) && typeid_cast<const DataTypeUInt64 *>(result.get());
    if (name == "arrayReverse" || name == "arraySort")
        return arguments.size() == 1 && isArrayOfStringType(arguments[0]) && same_argument_and_result(0);
    if (name == "tuple")
    {
        const auto * tuple_result = typeid_cast<const DataTypeTuple *>(result.get());
        if (!tuple_result || arguments.empty() || arguments.size() != tuple_result->getElements().size()
            || !budget.consume(arguments.size()))
            return false;
        for (size_t i = 0; i < arguments.size(); ++i)
            if (!isSupportedTupleValueType(arguments[i], 3, budget) || !sameType(arguments[i], tuple_result->getElements()[i], budget))
                return false;
        return true;
    }
    if (name == "cityHash64")
    {
        if (arguments.empty() || !typeid_cast<const DataTypeUInt64 *>(result.get()) || !budget.consume(arguments.size()))
            return false;
        for (const auto & argument : arguments)
            if (!isSupportedTupleValueType(argument, 3, budget) && !isNativeArrayNothingType(argument))
                return false;
        return true;
    }
    if (name == "arraySlice")
    {
        if (arguments.size() != 2 || !isPrimitiveIntegerType(arguments[1]) || !same_argument_and_result(0))
            return false;

        const auto * array_type = arguments[0] ? typeid_cast<const DataTypeArray *>(arguments[0].get()) : nullptr;
        if (!array_type)
            return false;

        const auto nested_type = array_type->getNestedType();
        return typeid_cast<const DataTypeUInt64 *>(nested_type.get()) || isNativeStringType(nested_type);
    }
    if (name == "arrayResize")
        return arguments.size() == 2 && typeid_cast<const DataTypeArray *>(arguments[0].get())
            && typeid_cast<const DataTypeUInt64 *>(typeid_cast<const DataTypeArray *>(arguments[0].get())->getNestedType().get())
            && isPrimitiveIntegerType(arguments[1]) && same_argument_and_result(0);
    if (name == "arrayIntersect")
    {
        if (arguments.size() < 2 || !is_array(result) || !same_argument_and_result(0) || !budget.consume(arguments.size()))
            return false;
        for (size_t i = 1; i < arguments.size(); ++i)
            if (!is_array(arguments[i]) || !sameType(arguments[i], result, budget))
                return false;
        return true;
    }
    if (name == "mapKeys")
    {
        const auto * map_type = arguments.size() == 1 ? typeid_cast<const DataTypeMap *>(arguments[0].get()) : nullptr;
        const auto * result_array = typeid_cast<const DataTypeArray *>(result.get());
        return map_type && result_array && isStringType(map_type->getKeyType())
            && typeid_cast<const DataTypeUInt64 *>(map_type->getValueType().get()) && isStringType(result_array->getNestedType());
    }
    if (name == "if")
    {
        if (arguments.size() != 3 || !isPrimitiveIntegerType(arguments[0]) || !isSupportedIfValueType(arguments[1])
            || !isSupportedIfValueType(arguments[2]))
            return false;

        if (sameType(arguments[1], arguments[2], budget) && sameType(arguments[1], result, budget))
            return true;

        return isSupportedIfNullableStringPromotion(arguments[1], arguments[2], result, budget);
    }
    if (name == "isNull")
    {
        const auto * nullable = arguments.size() == 1 ? typeid_cast<const DataTypeNullable *>(arguments[0].get()) : nullptr;
        const IDataType * nested_type_raw = nullable ? nullable->getNestedType().get() : nullptr;
        return nullable && nested_type_raw && typeid(*nested_type_raw) == typeid(DataTypeNothing)
            && typeid_cast<const DataTypeUInt8 *>(result.get());
    }
    if (name == "arraySum")
    {
        const auto * array = arguments.size() == 1 ? typeid_cast<const DataTypeArray *>(arguments[0].get()) : nullptr;
        return array && typeid_cast<const DataTypeUInt64 *>(array->getNestedType().get())
            && typeid_cast<const DataTypeUInt64 *>(result.get());
    }
    if (name == "arrayMap")
    {
        return arguments.size() == 2 && typeid_cast<const DataTypeFunction *>(arguments[0].get())
            && isSupportedArrayMapInputType(arguments[1]) && is_array(result);
    }
    if (name == "arrayDistinct")
        return arguments.size() == 1 && is_array(arguments[0]) && same_argument_and_result(0);
    if (name == "arrayConcat")
    {
        if (arguments.size() < 2 || !is_array(result) || !budget.consume(arguments.size()))
            return false;
        for (const auto & argument : arguments)
            if ((!is_array(argument) && !isNativeArrayNothingType(argument))
                || (!isNativeArrayNothingType(argument) && !sameType(argument, result, budget)))
                return false;
        return true;
    }
    if (name == "toString")
        return arguments.size() == 1 && isStringType(result) && (isPrimitiveNumericType(arguments[0]) || isDecimalType(arguments[0]));
    if (name == "toDecimal32" || name == "toDecimal64" || name == "toDecimal128" || name == "toDecimal256")
        return arguments.size() == 2 && (isPrimitiveNumericType(arguments[0]) || isDecimalType(arguments[0]))
            && isPrimitiveIntegerType(arguments[1]) && isDecimalType(result);
    if (name == "CAST")
        return arguments.size() == 2;
    return false;
}

bool isSupportedArrayFunctionPair(
    const ActionsDAG::Node & left, const ActionsDAG::Node & right, ExpressionMatchBudget & budget)
{
    if (!left.function_base || !right.function_base || left.is_function_compiled || right.is_function_compiled)
        return false;

    const auto left_adaptor = std::dynamic_pointer_cast<const FunctionToFunctionBaseAdaptor>(left.function_base);
    const auto right_adaptor = std::dynamic_pointer_cast<const FunctionToFunctionBaseAdaptor>(right.function_base);
    if (!left_adaptor || !right_adaptor)
        return false;

    const auto & left_function = left_adaptor->getFunction();
    const auto & right_function = right_adaptor->getFunction();
    if (!left_function || !right_function)
        return false;
    const auto * left_function_raw = left_function.get();
    const auto * right_function_raw = right_function.get();

    const FunctionCreator * left_handle = left.function_base->getFactoryHandle();
    const FunctionCreator * right_handle = right.function_base->getFactoryHandle();

    String function_name;
    if (!left_handle || !right_handle)
    {
        /// ArrayJoinStep constructs this function directly, without a FunctionFactory handle. Authenticate
        /// both native implementations by exact type, and still verify any handle that is present.
        const auto * left_empty_array_to_single = typeid_cast<const FunctionEmptyArrayToSingle *>(left_function.get());
        const auto * right_empty_array_to_single = typeid_cast<const FunctionEmptyArrayToSingle *>(right_function.get());
        const FunctionCreator * canonical_empty_array_to_single = getCanonicalArrayFunctionHandle("emptyArrayToSingle");
        if (!left_empty_array_to_single || !right_empty_array_to_single
            || (left_handle && left_handle != canonical_empty_array_to_single)
            || (right_handle && right_handle != canonical_empty_array_to_single))
            return false;

        function_name = "emptyArrayToSingle";
    }
    else
    {
        if (left_handle != right_handle)
            return false;

        /// Authenticate against the small registered allowlist before asking a function for its name.
        /// Arbitrary/custom function names must not induce unbounded string work in this matcher.
        if (left_handle == getCanonicalArrayFunctionHandle("arrayDistinct"))
            function_name = "arrayDistinct";
        else if (left_handle == getCanonicalArrayFunctionHandle("arrayConcat"))
            function_name = "arrayConcat";
        else if (left_handle == getCanonicalArrayFunctionHandle("emptyArrayToSingle"))
            function_name = "emptyArrayToSingle";
        else
            return false;
    }

    if (function_name == "emptyArrayToSingle"
        && (typeid(*left_function_raw) != typeid(FunctionEmptyArrayToSingle)
            || typeid(*right_function_raw) != typeid(FunctionEmptyArrayToSingle)))
        return false;

    /// These authenticated native IFunction creators have no separate function parameters.
    if (!left_function || !right_function
        || left_function->getName() != function_name || right_function->getName() != function_name
        || left_function->isStateful() || right_function->isStateful()
        || !left_function->isDeterministic() || !right_function->isDeterministic()
        || !left_function->isDeterministicInScopeOfQuery() || !right_function->isDeterministicInScopeOfQuery()
        || !isSupportedBloomFilterArrayType(left.function_base->getResultType())
        || !isSupportedBloomFilterArrayType(right.function_base->getResultType()))
        return false;

    const auto & left_argument_types = left.function_base->getArgumentTypes();
    const auto & right_argument_types = right.function_base->getArgumentTypes();
    if (left_argument_types.empty() || left_argument_types.size() != right_argument_types.size()
        || left_argument_types.size() != left.children.size() || right_argument_types.size() != right.children.size())
        return false;

    /// Debit all argument-type scans and child pairs before walking the type vectors. A crafted function
    /// signature with many arguments must decline before this loop, even when its node is later rejected.
    if (!budget.consume(left_argument_types.size()) || !budget.consume(right_argument_types.size())
        || !budget.consume(left.children.size()) || !budget.consume(right.children.size()))
        return false;

    for (const auto & argument_type : left_argument_types)
    {
        if (!isSupportedBloomFilterArrayType(argument_type)
            || !sameType(argument_type, left.function_base->getResultType(), budget))
            return false;
    }
    for (const auto & argument_type : right_argument_types)
    {
        if (!isSupportedBloomFilterArrayType(argument_type)
            || !sameType(argument_type, right.function_base->getResultType(), budget))
            return false;
    }

    return true;
}

bool isSupportedDirectFunctionPair(
    const ActionsDAG::Node & left,
    const ActionsDAG::Node & right,
    ExpressionMatchBudget & budget,
    bool inside_lambda_body,
    const std::shared_ptr<const LambdaBodyBinding> & body_binding,
    std::vector<ExpressionNodePair> & pending)
{
    if (!left.function_base || !right.function_base || left.is_function_compiled || right.is_function_compiled)
        return false;

    const FunctionCreator * left_handle = left.function_base->getFactoryHandle();
    if (!left_handle || right.function_base->getFactoryHandle() != left_handle)
        return false;

    /// Resolve the implementation through the native registry before invoking any function virtuals.
    /// Unknown creators may inspect captured values from determinism or hash hooks.
    const char * audited_name = getAuditedDirectFunctionName(left_handle);
    if (!audited_name)
        return false;
    const String function_name(audited_name);

    const auto & left_argument_types = left.function_base->getArgumentTypes();
    const auto & right_argument_types = right.function_base->getArgumentTypes();
    if (left_argument_types.size() != right_argument_types.size()
        || left_argument_types.size() != left.children.size() || right_argument_types.size() != right.children.size()
        || !budget.consume(left_argument_types.size()) || !budget.consume(right_argument_types.size()))
        return false;

    if (!isSupportedDirectFunctionSignature(function_name, *left.function_base, budget)
        || !isSupportedDirectFunctionSignature(function_name, *right.function_base, budget))
        return false;

    bool has_lambda_argument = false;
    for (size_t i = 0; i < left_argument_types.size(); ++i)
    {
        const bool left_is_lambda = typeid_cast<const DataTypeFunction *>(left_argument_types[i].get());
        const bool right_is_lambda = typeid_cast<const DataTypeFunction *>(right_argument_types[i].get());
        if (left_is_lambda != right_is_lambda)
            return false;
        has_lambda_argument |= left_is_lambda;
    }

    /// Only the registered arrayMap wrapper has bounded static safety hooks for a lambda argument.
    /// Other higher-order wrappers can inspect the closure body in these virtual methods.
    if (has_lambda_argument
        && (inside_lambda_body || function_name != "arrayMap" || left_handle != getCanonicalArrayFunctionHandle("arrayMap")))
        return false;

    if (left.function_base->isStateful() || right.function_base->isStateful()
        || !left.function_base->isDeterministic() || !right.function_base->isDeterministic()
        || !left.function_base->isDeterministicInScopeOfQuery() || !right.function_base->isDeterministicInScopeOfQuery()
        || left.function_base->hasObservableSideEffects() || right.function_base->hasObservableSideEffects())
        return false;

    if (!sameType(left.function_base->getResultType(), right.function_base->getResultType(), budget)
        || !sameType(left.function_base->getResultType(), left.result_type, budget)
        || !sameType(right.function_base->getResultType(), right.result_type, budget))
        return false;

    for (size_t i = 0; i < left_argument_types.size(); ++i)
    {
        const auto * left_child = left.children[i];
        const auto * right_child = right.children[i];
        if (!left_child || !right_child
            || (has_lambda_argument && typeid_cast<const DataTypeFunction *>(left_argument_types[i].get())
                ? (!sameFunctionType(left_argument_types[i], right_argument_types[i], budget)
                    || !sameFunctionType(left_argument_types[i], left_child->result_type, budget)
                    || !sameFunctionType(right_argument_types[i], right_child->result_type, budget))
                : (!sameType(left_argument_types[i], right_argument_types[i], budget)
                    || !sameType(left_argument_types[i], left_child->result_type, budget)
                    || !sameType(right_argument_types[i], right_child->result_type, budget))))
            return false;

        if (typeid_cast<const DataTypeFunction *>(left_argument_types[i].get()))
        {
            if (!budget.consume())
                return false;
            pending.emplace_back(left_child, right_child, true, inside_lambda_body, body_binding);
        }
    }

    /// arrayMap has no per-call parameters: its authenticated factory, matching signature and
    /// bounded lambda body/captures fully identify this wrapper. Avoid updateHash on an adaptor
    /// that may inspect a FunctionExpression argument recursively.
    if (has_lambda_argument)
        return true;

    /// IFunctionBase::updateHash is the semantic-context hook for captured value-affecting state.
    if (areCastsEquivalentForBloomFilterExpressionMatching(*left.function_base, *right.function_base)
        || areNativeConversionsEquivalentForBloomFilterExpressionMatching(*left.function_base, *right.function_base))
        return true;

    SipHash left_hash;
    SipHash right_hash;
    left.function_base->updateHash(left_hash);
    right.function_base->updateHash(right_hash);
    return left_hash.get128() == right_hash.get128();
}

bool isSupportedArrayJoinFunctionPair(
    const ActionsDAG::Node & left,
    const ActionsDAG::Node & right,
    ExpressionMatchBudget & budget,
    bool inside_lambda_body,
    std::vector<ExpressionNodePair> & pending)
{
    if (!left.function_base || !right.function_base)
        return false;

    const auto * canonical_empty_array_to_single = getCanonicalArrayFunctionHandle("emptyArrayToSingle");
    const FunctionCreator * left_handle = left.function_base->getFactoryHandle();
    const FunctionCreator * right_handle = right.function_base->getFactoryHandle();
    if (left_handle && right_handle && left_handle == canonical_empty_array_to_single && right_handle == canonical_empty_array_to_single)
        return isSupportedArrayFunctionPair(left, right, budget);

    if (left_handle && right_handle)
        return isSupportedDirectFunctionPair(left, right, budget, inside_lambda_body, {}, pending);

    /// ArrayJoinStep creates emptyArrayToSingle outside FunctionFactory. When either handle is absent,
    /// only the existing exact native-function bridge may authenticate the pair.
    return isSupportedArrayFunctionPair(left, right, budget);
}

const ActionsDAG::Node * normalizeExpressionNode(const ActionsDAG::Node * node, ExpressionMatchBudget & budget)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS)
    {
        if (!budget.consume() || node->children.size() != 1)
            return nullptr;
        node = node->children[0];
    }
    return node;
}

bool isCanonicalArrayMapWithNativeLambda(const ActionsDAG::Node * raw_node, ExpressionMatchBudget & budget)
{
    const auto * node = normalizeExpressionNode(raw_node, budget);
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base
        || node->is_function_compiled
        || node->function_base->getFactoryHandle() != getCanonicalArrayFunctionHandle("arrayMap")
        || !isSupportedBloomFilterArrayType(node->result_type))
        return false;

    const auto & argument_types = node->function_base->getArgumentTypes();
    if (argument_types.empty() || argument_types.size() != node->children.size()
        || !budget.consume(argument_types.size()) || !budget.consume(node->children.size()))
        return false;

    const auto * lambda_type = typeid_cast<const DataTypeFunction *>(argument_types.front().get());
    const auto * lambda_node = node->children.front();
    LambdaCarrierView lambda_view{};
    return lambda_type && lambda_node && sameFunctionType(argument_types.front(), lambda_node->result_type, budget)
        && getLambdaCarrierView(*lambda_node, lambda_view);
}

bool structurallyMatchesIndexedExpression(
    const ActionsDAG::Node * query_node,
    const ActionsDAG::Node * index_node,
    ExpressionMatchMode mode,
    ExpressionMatchBudget & budget)
{
    std::vector<ExpressionNodePair> pending;
    std::unordered_set<ExpressionNodePair, ExpressionNodePairHash> visited;
    if (!budget.consume())
        return false;
    pending.emplace_back(query_node, index_node);

    while (!pending.empty())
    {
        const auto pair = pending.back();
        pending.pop_back();
        if (!budget.consume())
            return false;

        const auto * left = normalizeExpressionNode(pair.query_node, budget);
        const auto * right = normalizeExpressionNode(pair.index_node, budget);
        if (!left || !right)
            return false;
        if (!visited.emplace(ExpressionNodePair{left, right, pair.is_lambda_carrier, pair.inside_lambda_body, pair.body_binding}).second)
            continue;

        const bool left_is_function_type = left->result_type
            && typeid_cast<const DataTypeFunction *>(left->result_type.get());
        const bool right_is_function_type = right->result_type
            && typeid_cast<const DataTypeFunction *>(right->result_type.get());
        if (left_is_function_type || right_is_function_type || pair.is_lambda_carrier)
        {
            if (!left_is_function_type || !right_is_function_type || !pair.is_lambda_carrier || pair.inside_lambda_body
                || !compareLambdaCarriers(*left, *right, budget, pending))
                return false;
            continue;
        }

        if (!sameType(left->result_type, right->result_type, budget) || left->type != right->type)
            return false;

        switch (left->type)
        {
            case ActionsDAG::ActionType::INPUT:
                if (!left->children.empty() || !right->children.empty())
                    return false;
                if (pair.body_binding)
                {
                    if (!pair.inside_lambda_body)
                        return false;
                    const auto left_slot = pair.body_binding->query_inputs.find(left);
                    const auto right_slot = pair.body_binding->index_inputs.find(right);
                    if (left_slot == pair.body_binding->query_inputs.end()
                        || right_slot == pair.body_binding->index_inputs.end()
                        || !(left_slot->second == right_slot->second))
                        return false;
                }
                else if (!sameBoundedName(left->result_name, right->result_name, budget))
                    return false;
                break;
            case ActionsDAG::ActionType::COLUMN:
                if (!left->children.empty() || !right->children.empty()
                    || !compareSupportedConstant(*left, *right, budget))
                    return false;
                break;
            case ActionsDAG::ActionType::FUNCTION:
                if (left->children.size() != right->children.size()
                    || (mode == ExpressionMatchMode::DirectIN
                        ? !isSupportedDirectFunctionPair(*left, *right, budget, pair.inside_lambda_body, pair.body_binding, pending)
                        : !isSupportedArrayJoinFunctionPair(*left, *right, budget, pair.inside_lambda_body, pending)))
                    return false;
                for (size_t i = 0; i < left->children.size(); ++i)
                {
                    if (!left->children[i] || !right->children[i])
                        return false;

                    /// The direct function helper already queued function-typed arguments as lambda carriers.
                    const bool left_is_function_argument = left->children[i]->result_type
                        && typeid_cast<const DataTypeFunction *>(left->children[i]->result_type.get());
                    const bool right_is_function_argument = right->children[i]->result_type
                        && typeid_cast<const DataTypeFunction *>(right->children[i]->result_type.get());
                    if (left_is_function_argument || right_is_function_argument)
                    {
                        if (!left_is_function_argument || !right_is_function_argument)
                            return false;
                        continue;
                    }

                    if (!budget.consume())
                        return false;
                    pending.emplace_back(left->children[i], right->children[i], false, pair.inside_lambda_body, pair.body_binding);
                }
                break;
            case ActionsDAG::ActionType::ALIAS:
                /// Aliases are removed before this switch.
                return false;
            case ActionsDAG::ActionType::ARRAY_JOIN:
            case ActionsDAG::ActionType::PLACEHOLDER:
                return false;
        }
    }

    return true;
}

bool matchesIndexedOutput(
    const ActionsDAG::Node * query_node,
    const ActionsDAG & indexed_expression_dag,
    const Block & header,
    const String & column_name,
    ExpressionMatchMode mode,
    ExpressionMatchBudget & budget)
{
    if (!query_node || !budget.consume(column_name.size()) || !header.has(column_name))
        return false;

    const size_t position = header.getPositionByName(column_name);
    const auto & outputs = indexed_expression_dag.getOutputs();
    if (position >= outputs.size() || !outputs[position] || !header.getByPosition(position).type
        || !sameType(outputs[position]->result_type, header.getByPosition(position).type, budget))
        return false;

    return structurallyMatchesIndexedExpression(query_node, outputs[position], mode, budget);
}

bool findMatchingDirectLambdaArrayMapOutputByPosition(
    const ActionsDAG::Node * query_node,
    const ActionsDAG & indexed_expression_dag,
    const Block & header,
    size_t & matched_position,
    ExpressionMatchBudget & budget)
{
    const auto & outputs = indexed_expression_dag.getOutputs();
    if (outputs.size() != header.columns() || !isCanonicalArrayMapWithNativeLambda(query_node, budget)
        || !budget.consume(outputs.size()))
        return false;

    std::unordered_set<String> unique_names;
    unique_names.reserve(header.columns());
    for (size_t position = 0; position < outputs.size(); ++position)
    {
        const auto & header_column = header.getByPosition(position);
        if (!budget.consume() || !budget.consume(header_column.name.size())
            || !unique_names.emplace(header_column.name).second
            || header.getPositionByName(header_column.name) != position)
            return false;
    }

    for (size_t position = 0; position < outputs.size(); ++position)
    {
        const auto & header_column = header.getByPosition(position);
        const auto * output = outputs[position];
        if (!output || !output->result_type || !header_column.type
            || !isSupportedBloomFilterArrayType(header_column.type)
            || !isSupportedBloomFilterArrayType(output->result_type)
            || !sameType(output->result_type, header_column.type, budget)
            || !isCanonicalArrayMapWithNativeLambda(output, budget))
        {
            if (budget.remaining == 0)
                return false;
            continue;
        }

        if (matchesIndexedOutput(
                query_node, indexed_expression_dag, header, header_column.name,
                ExpressionMatchMode::DirectIN, budget))
        {
            matched_position = position;
            return true;
        }

        if (budget.remaining == 0)
            return false;
    }

    return false;
}

bool findMatchingArrayJoinOutputByPosition(
    const ActionsDAG::Node * query_node,
    const ActionsDAG & indexed_expression_dag,
    const Block & header,
    String & column_name,
    ExpressionMatchBudget & budget)
{
    const auto & outputs = indexed_expression_dag.getOutputs();
    if (!query_node || !query_node->result_type
        || !typeid_cast<const DataTypeArray *>(query_node->result_type.get())
        || !isSupportedBloomFilterArrayType(query_node->result_type)
        || outputs.size() != header.columns())
        return false;

    for (size_t position = 0; position < outputs.size(); ++position)
    {
        if (!budget.consume())
            return false;

        const auto & header_column = header.getByPosition(position);
        if (!budget.consume(header_column.name.size()))
            return false;

        const auto * header_array_type = header_column.type
            ? typeid_cast<const DataTypeArray *>(header_column.type.get()) : nullptr;
        const auto * output = outputs[position];
        if (!header_array_type || !isSupportedBloomFilterArrayType(header_column.type)
            || !output || !output->result_type
            || !typeid_cast<const DataTypeArray *>(output->result_type.get())
            || !isSupportedBloomFilterArrayType(output->result_type))
            continue;

        /// Preserve the exact header/output positional association even if a malformed header repeats a name.
        if (header.getPositionByName(header_column.name) != position)
            continue;

        if (matchesIndexedOutput(
                query_node, indexed_expression_dag, header, header_column.name,
                ExpressionMatchMode::ArrayJoinWrapper, budget))
        {
            column_name = header_column.name;
            return true;
        }

        if (budget.remaining == 0)
            return false;
    }

    return false;
}

bool getDirectArrayJoinIndexColumn(
    const RPNBuilderTreeNode & tree_node,
    const Block & header,
    const ActionsDAG & indexed_expression_dag,
    String & column_name,
    ExpressionMatchBudget & budget)
{
    const ActionsDAG::Node * node = getNodeWithoutAliases(tree_node, budget);
    if (!node
        || (node->type != ActionsDAG::ActionType::INPUT && node->type != ActionsDAG::ActionType::FUNCTION)
        || !node->result_type)
        return false;

    column_name = tree_node.getColumnName();
    if (!header.has(column_name))
    {
        /// Query DAG and stored-index sample-block names can differ across analyzer paths.
        /// Only the array-join wrapper path may recover by matching aligned outputs structurally.
        return findMatchingArrayJoinOutputByPosition(
            tree_node.getDAGNode(), indexed_expression_dag, header, column_name, budget);
    }

    const DataTypePtr & index_type = header.getByPosition(header.getPositionByName(column_name)).type;
    return typeid_cast<const DataTypeArray *>(index_type.get())
        && matchesIndexedOutput(node, indexed_expression_dag, header, column_name, ExpressionMatchMode::ArrayJoinWrapper, budget);
}

bool isIdentityDynamicArrayCast(
    const RPNBuilderFunctionTreeNode & function,
    const Block & header,
    const ActionsDAG & indexed_expression_dag,
    String & column_name,
    ExpressionMatchBudget & budget)
{
    const String function_name = function.getFunctionName();
    if ((function_name != "CAST" && function_name != "_CAST") || function.getArgumentsSize() != 2)
        return false;

    const ActionsDAG::Node * outer_node = getNodeWithoutAliases(function, budget);
    if (!outer_node || outer_node->type != ActionsDAG::ActionType::FUNCTION || !outer_node->result_type
        || !typeid_cast<const DataTypeArray *>(outer_node->result_type.get()))
        return false;

    const RPNBuilderTreeNode dynamic_node = function.getArgumentAt(0);
    if (!dynamic_node.isFunction())
        return false;

    const auto dynamic_function = dynamic_node.toFunctionNode();
    const String dynamic_function_name = dynamic_function.getFunctionName();
    if ((dynamic_function_name != "CAST" && dynamic_function_name != "_CAST") || dynamic_function.getArgumentsSize() != 2)
        return false;

    const ActionsDAG::Node * dynamic_cast_node = getNodeWithoutAliases(dynamic_function, budget);
    if (!dynamic_cast_node || dynamic_cast_node->type != ActionsDAG::ActionType::FUNCTION || !dynamic_cast_node->result_type
        || !typeid_cast<const DataTypeDynamic *>(dynamic_cast_node->result_type.get()))
        return false;

    const RPNBuilderTreeNode source_node = dynamic_function.getArgumentAt(0);
    if (!getDirectArrayJoinIndexColumn(source_node, header, indexed_expression_dag, column_name, budget))
        return false;

    const ActionsDAG::Node * source_dag_node = getNodeWithoutAliases(source_node, budget);
    const DataTypePtr & index_type = header.getByPosition(header.getPositionByName(column_name)).type;
    return source_dag_node && sameType(source_dag_node->result_type, index_type, budget)
        && sameType(outer_node->result_type, index_type, budget);
}

struct ArrayJoinIndexInput
{
    String column_name;
    bool has_empty_array_to_single = false;
    DataTypePtr generated_default_type;
    /// Array-element target types, outermost to innermost, for casts above emptyArrayToSingle.
    std::vector<DataTypePtr> array_cast_targets;
};

bool resolveArrayJoinIndexInput(
    const RPNBuilderTreeNode & input,
    const Block & header,
    const ActionsDAG & indexed_expression_dag,
    ArrayJoinIndexInput & result,
    ExpressionMatchBudget & budget)
{
    const ActionsDAG::Node * input_node = getNodeWithoutAliases(input, budget);
    if (!input_node || !input_node->result_type || !typeid_cast<const DataTypeArray *>(input_node->result_type.get()))
        return false;

    std::vector<DataTypePtr> casts_above_default;
    RPNBuilderTreeNode current = input;

    while (true)
    {
        if (!budget.consume())
            return false;
        if (getDirectArrayJoinIndexColumn(current, header, indexed_expression_dag, result.column_name, budget))
            return true;

        if (!current.isFunction())
            return false;

        const auto function = current.toFunctionNode();
        const ActionsDAG::Node * function_node = getNodeWithoutAliases(function, budget);
        if (!function_node || function_node->type != ActionsDAG::ActionType::FUNCTION || !function_node->result_type)
            return false;

        const String function_name = function.getFunctionName();
        if (function_name == "emptyArrayToSingle")
        {
            if (result.has_empty_array_to_single || function.getArgumentsSize() != 1)
                return false;

            const RPNBuilderTreeNode argument = function.getArgumentAt(0);
            const ActionsDAG::Node * argument_node = getNodeWithoutAliases(argument, budget);
            const auto * input_array_type = argument_node && argument_node->result_type
                ? typeid_cast<const DataTypeArray *>(argument_node->result_type.get()) : nullptr;
            const auto * result_array_type = typeid_cast<const DataTypeArray *>(function_node->result_type.get());
            if (!input_array_type || !result_array_type
                || !sameType(argument_node->result_type, function_node->result_type, budget))
                return false;

            result.has_empty_array_to_single = true;
            result.generated_default_type = input_array_type->getNestedType();
            result.array_cast_targets.swap(casts_above_default);
            current = argument;
            continue;
        }

        if (isIdentityDynamicArrayCast(function, header, indexed_expression_dag, result.column_name, budget))
            return true;

        if (!isLosslessConversionFunction(*function_node))
            return false;

        const RPNBuilderTreeNode argument = function.getArgumentAt(0);
        const ActionsDAG::Node * argument_node = getNodeWithoutAliases(argument, budget);
        const auto * result_array_type = typeid_cast<const DataTypeArray *>(function_node->result_type.get());
        const auto * argument_array_type = argument_node && argument_node->result_type
            ? typeid_cast<const DataTypeArray *>(argument_node->result_type.get()) : nullptr;
        if (!result_array_type || !argument_array_type)
            return false;

        if (!result.has_empty_array_to_single)
            casts_above_default.emplace_back(result_array_type->getNestedType());
        current = argument;
    }
}

bool isSupportedBloomFilterArrayElementType(const DataTypePtr & type)
{
    if (!type || canContainNull(*type))
        return false;

    const DataTypePtr primitive_type = removeLowCardinalityAndNullable(type);
    const WhichDataType which(primitive_type);
    return which.isUInt() || which.isInt() || which.isString() || which.isFixedString() || which.isDate() || which.isDateTime()
        || which.isDateTime64() || which.isEnum() || which.isUUID() || which.isIPv4() || which.isIPv6();
}

bool hashMatchesFilter(const BloomFilterPtr& bloom_filter, UInt64 hash, size_t hash_functions)
{
    return std::all_of(BloomFilterHash::bf_hash_seed,
                       BloomFilterHash::bf_hash_seed + hash_functions,
                       [&](const auto &hash_seed)
                       {
                           return bloom_filter->findHashWithSeed(hash,
                                                                 hash_seed);
                       });
}

bool maybeTrueOnBloomFilter(const IColumn * hash_column, const BloomFilterPtr & bloom_filter, size_t hash_functions, bool match_all)
{
    const auto * const_column = typeid_cast<const ColumnConst *>(hash_column);
    const auto * non_const_column = typeid_cast<const ColumnUInt64 *>(hash_column);

    if (!const_column && !non_const_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash column must be Const or UInt64.");

    if (const_column)
    {
        return hashMatchesFilter(bloom_filter,
                                 const_column->getValue<UInt64>(),
                                 hash_functions);
    }

    const ColumnUInt64::Container & hashes = non_const_column->getData();

    if (match_all)
    {
        return std::all_of(hashes.begin(),
                           hashes.end(),
                           [&](const auto& hash_row)
                           {
                               return hashMatchesFilter(bloom_filter,
                                                        hash_row,
                                                        hash_functions);
                           });
    }

    return std::any_of(
        hashes.begin(), hashes.end(), [&](const auto & hash_row) { return hashMatchesFilter(bloom_filter, hash_row, hash_functions); });
}

/// Information about a Map column's presence in the bloom filter index.
/// Used to unify handling of both `arrayElement(map, key)` function calls
/// and `map.key_<serialized_key>` subcolumn references produced by `FunctionToSubcolumnsPass`.
struct MapIndexInfo
{
    size_t keys_index_position = 0;
    size_t values_index_position = 0;
    bool has_keys_index = false;
    bool has_values_index = false;
    Field key_field;
};

/// Try to resolve a Map column against the bloom filter index header by the map column name
/// and the key as a Field. `map_key_type` is the key type of the map column, when it is known
/// independently of the index header. Returns std::nullopt if neither `mapKeys(<col>)` nor
/// `mapValues(<col>)` is present in the index, or if the key is not representable in the key type.
std::optional<MapIndexInfo> tryResolveMapIndexInfo(
    const String & map_column_name, const Field & key_field, const DataTypePtr & map_key_type, const Block & header)
{
    auto map_keys_index_column_name = fmt::format("mapKeys({})", map_column_name);
    auto map_values_index_column_name = fmt::format("mapValues({})", map_column_name);

    auto keys_position = header.findPositionByName(map_keys_index_column_name);
    auto values_position = header.findPositionByName(map_values_index_column_name);

    if (!keys_position && !values_position)
        return std::nullopt;

    MapIndexInfo info;
    info.key_field = key_field;

    /// The key is hashed as a value of the key type of the map, while the constant in the query can have
    /// a different type: e.g. a map with `Enum` keys is indexed by the name of the enum value, which is a
    /// `String`. Convert the key to the key type; if it is not representable in that type, decline the
    /// whole predicate: the key cannot be present in the map, but the index cannot be used to prove it
    /// either, because `arrayElement` returns the default value for a missing key, and for an `Enum` key
    /// it throws `UNKNOWN_ELEMENT_OF_ENUM` instead. Probing the `mapValues` index alone would be unsound
    /// as well: it could prune the granules before `arrayElement` runs and silently replace that
    /// exception by an empty result. This is why the check is not done only when `mapKeys` is indexed.
    DataTypePtr key_type = map_key_type;
    if (keys_position)
    {
        const auto & index_type = header.getByPosition(*keys_position).type;
        key_type = assert_cast<const DataTypeArray &>(*index_type).getNestedType();
    }

    if (key_type)
    {
        Field converted_key_field = tryConvertFieldToType(key_field, *key_type, nullptr, {}, /* strict */ true);

        if (converted_key_field.isNull())
            return std::nullopt;

        info.key_field = converted_key_field;
    }

    if (keys_position)
    {
        info.has_keys_index = true;
        info.keys_index_position = *keys_position;
    }
    if (values_position)
    {
        info.has_values_index = true;
        info.values_index_position = *values_position;
    }

    return info;
}

/// Try to parse a Map subcolumn reference like `map.key_<serialized_key>` and resolve it
/// against the bloom filter index header. The subcolumn name format is produced by
/// `FunctionToSubcolumnsPass`.
std::optional<MapIndexInfo> tryParseMapSubcolumn(
    const String & column_name, const Block & header, const NameSet & shadowing_columns)
{
    auto parsed = tryParseMapSubcolumnName(column_name, shadowing_columns);
    if (!parsed)
        return std::nullopt;

    auto & [map_column_name, serialized_key] = *parsed;

    auto map_keys_index_column_name = fmt::format("mapKeys({})", map_column_name);
    if (!header.has(map_keys_index_column_name))
        return tryResolveMapIndexInfo(map_column_name, {}, nullptr, header);

    /// Deserialize the key from its text representation using the key type from the index header.
    size_t keys_position = header.getPositionByName(map_keys_index_column_name);
    const DataTypePtr & index_type = header.getByPosition(keys_position).type;
    const auto & array_type = assert_cast<const DataTypeArray &>(*index_type);
    const auto & key_type = array_type.getNestedType();

    auto key_column = key_type->createColumn();
    ReadBufferFromString buf(serialized_key);
    key_type->getDefaultSerialization()->deserializeWholeText(*key_column, buf, FormatSettings());

    Field key_field;
    key_column->get(0, key_field);

    return tryResolveMapIndexInfo(map_column_name, key_field, key_type, header);
}

/// Try to resolve a `MapIndexInfo` from a key node that is either an `arrayElement(map, key)`
/// function call or a `map.key_<serialized_key>` subcolumn reference.
std::optional<MapIndexInfo> tryResolveMapInfoFromNode(
    const RPNBuilderTreeNode & key_node, const Block & header, const NameSet & shadowing_columns)
{
    if (key_node.isFunction())
    {
        auto key_node_function = key_node.toFunctionNode();
        if (key_node_function.getFunctionName() == "arrayElement")
        {
            auto first_argument = key_node_function.getArgumentAt(0);
            auto second_argument = key_node_function.getArgumentAt(1);

            Field constant_value;
            DataTypePtr constant_type;
            if (!second_argument.tryGetConstant(constant_value, constant_type))
                return std::nullopt;

            /// The key type of the map is needed even when only `mapValues` is indexed, to tell a key that
            /// cannot be represented in it, see `tryResolveMapIndexInfo`.
            DataTypePtr map_key_type;
            if (const auto * dag_node = first_argument.getDAGNode())
                if (const auto * map_type = typeid_cast<const DataTypeMap *>(removeNullable(dag_node->result_type).get()))
                    map_key_type = map_type->getKeyType();

            return tryResolveMapIndexInfo(first_argument.getColumnName(), constant_value, map_key_type, header);
        }
    }

    return tryParseMapSubcolumn(key_node.getColumnName(), header, shadowing_columns);
}

}

MergeTreeIndexConditionBloomFilter::MergeTreeIndexConditionBloomFilter(
    const ActionsDAG::Node * predicate,
    ContextPtr context_,
    const Block & header_,
    const ActionsDAG & indexed_expression_dag_,
    size_t hash_functions_,
    NameSet columns_shadowing_map_subcolumns_,
    JSONIndexArgumentTypes json_argument_types_)
    : WithContext(context_)
    , header(header_)
    , hash_functions(hash_functions_)
    , columns_shadowing_map_subcolumns(std::move(columns_shadowing_map_subcolumns_))
    , json_argument_types(std::move(json_argument_types_))
    , validate_enum_literals_in_operators(context_->getSettingsRef()[Setting::validate_enum_literals_in_operators])
{
    if (!predicate || !isBloomIndexExpressionContextStable(indexed_expression_dag_))
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        predicate,
        context_,
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out, indexed_expression_dag_); });
    rpn = std::move(builder).extractRPN();
}

bool MergeTreeIndexConditionBloomFilter::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        rpn,
        {RPNElement::FUNCTION_EQUALS,
         RPNElement::FUNCTION_HAS,
         RPNElement::FUNCTION_HAS_ANY,
         RPNElement::FUNCTION_HAS_ALL,
         RPNElement::FUNCTION_IN});
}

bool MergeTreeIndexConditionBloomFilter::mayBeTrueOnGranule(const MergeTreeIndexGranuleBloomFilter * granule, const UpdatePartialDisjunctionResultFn & update_partial_result_disjuntion_fn) const
{
    std::vector<BoolMask> rpn_stack;
    const auto & filters = granule->getFilters();

    size_t element_idx = 0;
    for (const auto & element : rpn)
    {
        switch (element.function)
        {
            case RPNElement::FUNCTION_UNKNOWN:
                rpn_stack.emplace_back(true, true);
                break;
            case RPNElement::FUNCTION_IN:
            case RPNElement::FUNCTION_EQUALS:
            case RPNElement::FUNCTION_HAS:
            case RPNElement::FUNCTION_HAS_ANY:
            case RPNElement::FUNCTION_HAS_ALL:
            {
                bool match_rows = true;
                bool match_all = element.function == RPNElement::FUNCTION_HAS_ALL;
                const auto & predicate = element.predicate;
                for (size_t index = 0; match_rows && index < predicate.size(); ++index)
                {
                    const auto & query_index_hash = predicate[index];
                    const auto & filter = filters[query_index_hash.first];
                    const ColumnPtr & hash_column = query_index_hash.second;

                    match_rows = maybeTrueOnBloomFilter(&*hash_column,
                                                        filter,
                                                        hash_functions,
                                                        match_all);
                }

                rpn_stack.emplace_back(match_rows, true);
                break;
            }
            case RPNElement::FUNCTION_NOT:
                rpn_stack.back() = !rpn_stack.back();
                break;
            case RPNElement::FUNCTION_OR:
            {
                auto arg1 = rpn_stack.back();
                rpn_stack.pop_back();
                auto arg2 = rpn_stack.back();
                rpn_stack.back() = arg1 | arg2;
                break;
            }
            case RPNElement::FUNCTION_AND:
            {
                auto arg1 = rpn_stack.back();
                rpn_stack.pop_back();
                auto arg2 = rpn_stack.back();
                rpn_stack.back() = arg1 & arg2;
                break;
            }
            case RPNElement::ALWAYS_TRUE:
                rpn_stack.emplace_back(true, false);
                break;
            case RPNElement::ALWAYS_FALSE:
                rpn_stack.emplace_back(false, true);
                break;
            /// No `default:` to make the compiler warn if not all enum values are handled.
        }

        if (update_partial_result_disjuntion_fn)
        {
            update_partial_result_disjuntion_fn(element_idx, rpn_stack.back().can_be_true, element.function == RPNElement::FUNCTION_UNKNOWN);
            ++element_idx;
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in MergeTreeIndexConditionBloomFilter::mayBeTrueOnGranule");

    return rpn_stack[0].can_be_true;
}

bool MergeTreeIndexConditionBloomFilter::extractAtomFromTree(
    const RPNBuilderTreeNode & node,
    RPNElement & out,
    const ActionsDAG & indexed_expression_dag)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (node.tryGetConstant(const_value, const_type))
        {
            if (const_value.getType() == Field::Types::UInt64)
            {
                out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Int64)
            {
                out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Float64)
            {
                out.function = const_value.safeGet<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }
        }
    }

    return traverseFunction(node, out, nullptr /*parent*/, indexed_expression_dag);
}

namespace
{

/// Hash the JSON path string and append a predicate entry for bloom filter index.
void fillJSONPathBloomPredicate(
    const JSONSubcolumnIndexInfo & json_info,
    const Block & header,
    MergeTreeIndexConditionBloomFilter::RPNElement & out)
{
    const DataTypePtr & index_type = header.getByPosition(json_info.header_position).type;
    const auto actual_type = BloomFilter::getPrimitiveType(index_type);
    Field path_field(json_info.path);
    out.predicate.emplace_back(std::make_pair(
        json_info.header_position,
        BloomFilterHash::hashWithField(actual_type.get(), path_field)));
}

}

bool MergeTreeIndexConditionBloomFilter::traverseFunction(
    const RPNBuilderTreeNode & node,
    RPNElement & out,
    const RPNBuilderTreeNode * parent,
    const ActionsDAG & indexed_expression_dag)
{
    if (!node.isFunction())
        return false;

    const auto function = node.toFunctionNode();
    auto arguments_size = function.getArgumentsSize();
    auto function_name = function.getFunctionName();

    if (parent == nullptr)
    {
        /// Recurse a little bit for indexOf().
        for (size_t i = 0; i < arguments_size; ++i)
        {
            auto argument = function.getArgumentAt(i);
            if (traverseFunction(argument, out, &node, indexed_expression_dag))
                return true;
        }
    }

    /// Handle isNotNull for JSON subcolumns: isNotNull(json.some.path)
    /// When a JSON path is absent, the value is NULL (for Dynamic/Nullable types),
    /// so isNotNull(NULL) = false — always safe to skip granules where path is absent.
    if (function_name == "isNotNull" && arguments_size == 1)
    {
        auto arg = function.getArgumentAt(0);
        if (auto json_info = tryMatchNodeToJSONIndex(arg, header, "JSONAllPaths", json_argument_types))
        {
            auto arg_type = arg.getDAGNode()->result_type;
            /// It doesn't make sense to use bloom filter for isNotNull on non-Nullable type, as isNotNull will be always true.
            if (!canContainNull(*arg_type))
                return false;

            fillJSONPathBloomPredicate(*json_info, header, out);
            out.function = RPNElement::FUNCTION_HAS;
            return true;
        }
    }

    if (arguments_size != 2)
        return false;

    /// indexOf() should be inside comparison function, e.g. greater(indexOf(key, 42), 0).
    /// Other conditions should be at top level, e.g. equals(key, 42), not equals(equals(key, 42), 1).
    if ((function_name == "indexOf") != (parent != nullptr))
        return false;

    auto lhs_argument = function.getArgumentAt(0);
    auto rhs_argument = function.getArgumentAt(1);

    if (functionIsInOrGlobalInOperator(function_name))
    {
        /// A bloom filter only answers "may be present", so a negated set atom holds for any granule
        /// that has one other value and can never skip one.
        if (function_name == "notIn" || function_name == "globalNotIn")
            return false;

        if (auto future_set = rhs_argument.tryGetPreparedSet(); future_set)
        {
            if (auto prepared_set = future_set->buildOrderedSetInplace(rhs_argument.getContext()); prepared_set)
            {
                if (prepared_set->hasExplicitSetElements())
                {
                    const auto prepared_info = getPreparedSetInfo(prepared_set);
                    size_t expression_match_work_remaining = ExpressionMatchBudget::max_work;
                    if (traverseTreeIn(
                            function_name, lhs_argument, prepared_set, prepared_info.type, prepared_info.column,
                            out, indexed_expression_dag, expression_match_work_remaining))
                        return true;
                }
            }
        }
        return false;
    }

    if (function_name == "equals" ||
        function_name == "has" ||
        function_name == "mapContains" ||
        function_name == "mapContainsKey" ||
        function_name == "mapContainsValue" ||
        function_name == "indexOf" ||
        function_name == "hasAny" ||
        function_name == "hasAll")
    {
        Field const_value;
        DataTypePtr const_type;

        if (rhs_argument.tryGetConstant(const_value, const_type))
        {
            if (traverseTreeEquals(function_name, lhs_argument, const_type, const_value, out, parent))
                return true;
        }
        else if (lhs_argument.tryGetConstant(const_value, const_type) &&
            (function_name == "equals" || function_name == "has" || function_name == "hasAny"))
        {
            if (traverseTreeEquals(function_name, rhs_argument, const_type, const_value, out, parent))
                return true;
        }

        return false;
    }

    return false;
}

/// True when converting the constant to the element type yields the exact bytes the index holds, so
/// hashing it is equivalent to the comparison. Floats are excluded: `-0.0` equals but hashes apart.
static bool bloomFilterHashDomainMatches(const DataTypePtr & value_type, const DataTypePtr & nested_type)
{
    if (!value_type)
        return false;

    auto value = removeLowCardinalityAndNullable(value_type);
    auto element = removeLowCardinalityAndNullable(nested_type);

    if (isFloat(value) || isFloat(element))
        return false;

    return (isInteger(value) && isInteger(element)) || value->equals(*element);
}

/// LEFT ARRAY JOIN is arrayJoin(emptyArrayToSingle(col)), the default element it adds is not in an index on col
static String getArrayJoinColumnName(const RPNBuilderTreeNode & node, const Block & header, bool & padded)
{
    if (header.has(node.getColumnName()) || !node.isFunction() || node.toFunctionNode().getFunctionName() != "emptyArrayToSingle")
        return node.getColumnName();
    padded = true;
    return node.toFunctionNode().getArgumentAt(0).getColumnName();
}

bool MergeTreeIndexConditionBloomFilter::traverseTreeIn(
    const String & function_name,
    const RPNBuilderTreeNode & wrapped_key_node,
    const ConstSetPtr & prepared_set,
    const DataTypePtr & type,
    const ColumnPtr & column,
    RPNElement & out,
    const ActionsDAG & indexed_expression_dag,
    size_t & work_remaining)
{
    ExpressionMatchBudget expression_match_budget(work_remaining);
    if (!expression_match_budget.consume())
        return false;

    const auto key_node = unwrapLosslessConversion(wrapped_key_node);
    auto key_node_column_name = key_node.getColumnName();
    size_t direct_index_position = 0;
    bool matched_index_output = false;

    if (!expression_match_budget.consume(key_node_column_name.size()))
        return false;

    if (header.has(key_node_column_name))
    {
        matched_index_output = matchesIndexedOutput(
            key_node.getDAGNode(), indexed_expression_dag, header, key_node_column_name,
            ExpressionMatchMode::DirectIN, expression_match_budget);
        if (matched_index_output)
            direct_index_position = header.getPositionByName(key_node_column_name);
    }
    else
        matched_index_output = findMatchingDirectLambdaArrayMapOutputByPosition(
            key_node.getDAGNode(), indexed_expression_dag, header, direct_index_position, expression_match_budget);

    if (matched_index_output)
    {
        size_t row_size = column->size();
        const DataTypePtr & index_type = header.getByPosition(direct_index_position).type;

        /// A NULL of a set built for `toNullable(key)` does not cast to the key type and matches no stored value.
        const auto * nullable_column = typeid_cast<const ColumnNullable *>(column.get());
        if (nullable_column && !index_type->isNullable() && std::ranges::any_of(nullable_column->getNullMapData(), [](UInt8 is_null) { return is_null != 0; }))
            return false;

        const auto & converted_column = castColumn(ColumnWithTypeAndName{column, type, ""}, index_type);

        /// An `Array` index holds one hash per element, so a set array is looked up by its elements
        /// and an empty one has no hash that can stand for it. Contribute no predicate at all: a
        /// tuple `IN` shares this element, and a sibling component would re-enable the lookup.
        if ((function_name == "in" || function_name == "globalIn")
            && setHasEmptyArray(index_type, converted_column, row_size))
            return false;

        out.predicate.emplace_back(std::make_pair(direct_index_position, BloomFilterHash::hashWithColumn(index_type, converted_column, 0, row_size)));

        if (function_name == "in"  || function_name == "globalIn")
            out.function = RPNElement::FUNCTION_IN;

        /// `nullIn` (transform_null_in=1) selects the same rows as `in` only for a NULL-free,
        /// single-column, non-Array set whose type matches the index; otherwise no pruning.
        if ((function_name == "nullIn" || function_name == "globalNullIn") && prepared_set
            && prepared_set->getDataTypes().size() == 1 && !prepared_set->hasNull()
            && prepared_set->areTypesEqual(0, index_type)
            && !typeid_cast<const DataTypeArray *>(index_type.get()))
            out.function = RPNElement::FUNCTION_IN;

        return true;
    }

    /// Try to match the column name to a JSONAllPaths index for JSON subcolumn IN filtering.
    /// tryMatchNodeToJSONIndex handles both plain subcolumns and CAST-wrapped expressions.
    /// NOT IN is not supported because after BoolMask inversion it never skips any granules.
    /// nullIn/globalNullIn are deliberately not wired here: JSON paths need per-path NULL checks.
    if (auto json_info = tryMatchNodeToJSONIndex(key_node, header, "JSONAllPaths", json_argument_types))
    {
        if (function_name != "in" && function_name != "globalIn")
            return false;

        if (!prepared_set)
            return false;

        auto key_type = key_node.getDAGNode()->result_type;

        /// Check safety: if key type is non-Nullable and the set contains the default value,
        /// we cannot skip granules where the path is absent.
        if (!canContainNull(*key_type))
        {
            auto default_column_to_check = key_type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();
            ColumnWithTypeAndName default_column_with_type_to_check{default_column_to_check, key_type, ""};
            ColumnsWithTypeAndName default_columns_with_type_to_check = {default_column_with_type_to_check};
            auto result = prepared_set->execute(default_columns_with_type_to_check, false);
            const auto & result_data = assert_cast<const ColumnUInt8 &>(*result).getData();
            if (result_data[0])
                return false;
        }

        fillJSONPathBloomPredicate(*json_info, header, out);
        out.function = RPNElement::FUNCTION_IN;

        return true;
    }

    if (key_node.isFunction())
    {
        auto key_node_function = key_node.toFunctionNode();
        auto key_node_function_name = key_node_function.getFunctionName();
        size_t key_node_function_arguments_size = key_node_function.getArgumentsSize();

        WhichDataType which(type);

        if (which.isTuple() && key_node_function_name == "tuple")
        {
            const auto & tuple_column = typeid_cast<const ColumnTuple *>(column.get());
            const auto & tuple_data_type = typeid_cast<const DataTypeTuple *>(type.get());

            if (tuple_data_type->getElements().size() != key_node_function_arguments_size || tuple_column->getColumns().size() != key_node_function_arguments_size)
                return false;

            bool match_with_subtype = false;
            const auto & sub_columns = tuple_column->getColumns();
            const auto & sub_data_types = tuple_data_type->getElements();

            for (size_t index = 0; index < key_node_function_arguments_size; ++index)
            {
                if (work_remaining == 0)
                    break;

                match_with_subtype |= traverseTreeIn(
                    function_name, key_node_function.getArgumentAt(index), nullptr, sub_data_types[index], sub_columns[index],
                    out, indexed_expression_dag, work_remaining);
            }

            return match_with_subtype;
        }
    }

    /// Handle both `arrayElement(map, 'key') IN (set)` and `map.key_<serialized_key> IN (set)`.
    if (auto map_info = tryResolveMapInfoFromNode(key_node, header, columns_shadowing_map_subcolumns))
    {
        /** It is important to ignore keys like column_map['Key'] IN ('') because if the key does not exist in the map
          * we return the default value for arrayElement.
          *
          * We cannot skip keys that does not exist in map if comparison is with default type value because
          * that way we skip necessary granules where the map key does not exist.
          */
        if (!prepared_set)
            return false;

        auto default_column_to_check = type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();
        ColumnWithTypeAndName default_column_with_type_to_check{default_column_to_check, type, ""};
        ColumnsWithTypeAndName default_columns_with_type_to_check = {default_column_with_type_to_check};
        auto set_contains_default_value_predicate_column = prepared_set->execute(default_columns_with_type_to_check, false /*negative*/);
        const auto & set_contains_default_value_predicate_column_typed = assert_cast<const ColumnUInt8 &>(*set_contains_default_value_predicate_column);
        bool set_contain_default_value = set_contains_default_value_predicate_column_typed.getData()[0];
        if (set_contain_default_value)
            return false;

        /// Over `Enum` map values `arrayElement` returns the zero of the underlying integer for a missing key,
        /// which is not necessarily the default value of the `Enum` (its first value), so check it as well.
        /// Over `Nullable(Enum)` map values a missing key gives `NULL` rather than the zero, so this is not needed.
        if (const auto * dag_node = key_node.getDAGNode(); dag_node && isEnum(removeLowCardinality(dag_node->result_type)))
        {
            if (!isEnum(removeLowCardinalityAndNullable(type)))
                return false;

            auto zero_column = type->createColumn();
            zero_column->insert(Field(Int64(0)));
            auto set_contains_zero_column = prepared_set->execute({ColumnWithTypeAndName{std::move(zero_column), type, ""}}, false /*negative*/);
            if (assert_cast<const ColumnUInt8 &>(*set_contains_zero_column).getData()[0])
                return false;
        }

        if (map_info->has_keys_index)
        {
            /// For mapKeys we serialize key argument with bloom filter
            size_t position = map_info->keys_index_position;
            const DataTypePtr & index_type = header.getByPosition(position).type;
            const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), map_info->key_field)));
        }
        else if (map_info->has_values_index)
        {
            /// For mapValues we serialize set with bloom filter
            size_t row_size = column->size();
            size_t position = map_info->values_index_position;
            const DataTypePtr & index_type = header.getByPosition(position).type;
            const auto & array_type = assert_cast<const DataTypeArray &>(*index_type);
            const auto & array_nested_type = array_type.getNestedType();
            const auto & converted_column = castColumn(ColumnWithTypeAndName{column, type, ""}, array_nested_type);
            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithColumn(array_nested_type, converted_column, 0, row_size)));
        }
        else
        {
            return false;
        }

        /// nullIn/globalNullIn are deliberately not wired here, as in the JSON branch above.
        if (function_name == "in" || function_name == "globalIn")
            out.function = RPNElement::FUNCTION_IN;

        return true;
    }

    /// `arrayJoin(col) IN (set)` needs a set element in the granule, same as `hasAny(col, set)`.
    /// `nullIn` has the same positive match behavior only when the prepared set contains no NULL.
    /// `notIn` is not derivable: a granule holding a set element still yields rows outside the set.
    const bool is_in = function_name == "in" || function_name == "globalIn";
    const bool is_null_in = function_name == "nullIn" || function_name == "globalNullIn";
    if (!is_in && !is_null_in)
        return false;
    if (!column)
        return false;

    auto array_join_argument = key_node.getArrayJoinArgument();
    if (!array_join_argument)
        return false;

    /// Retain the scalar conversions around ARRAY_JOIN so an emptyArrayToSingle default
    /// can be checked using the exact type of the left operand.
    std::vector<DataTypePtr> scalar_cast_targets;
    RPNBuilderTreeNode array_join_node = wrapped_key_node;
    DataTypePtr array_join_value_type;
    while (true)
    {
        if (!expression_match_budget.consume())
            return false;

        if (array_join_node.getArrayJoinArgument())
        {
            const ActionsDAG::Node * dag_node = getNodeWithoutAliases(array_join_node, expression_match_budget);
            if (!dag_node || dag_node->type != ActionsDAG::ActionType::ARRAY_JOIN || !dag_node->result_type)
                return false;
            array_join_value_type = dag_node->result_type;
            break;
        }

        if (!array_join_node.isFunction())
            return false;

        const auto function = array_join_node.toFunctionNode();
        const ActionsDAG::Node * dag_node = getNodeWithoutAliases(function, expression_match_budget);
        if (!dag_node || !isLosslessConversionFunction(*dag_node) || !dag_node->result_type)
            return false;

        scalar_cast_targets.emplace_back(dag_node->result_type);
        array_join_node = function.getArgumentAt(0);
    }

    auto array_join_input = array_join_node.getArrayJoinArgument();
    if (!array_join_input)
        return false;

    ArrayJoinIndexInput array_input;
    if (!resolveArrayJoinIndexInput(
            *array_join_input, header, indexed_expression_dag, array_input, expression_match_budget)
        || !header.has(array_input.column_name))
        return false;

    const ActionsDAG::Node * input_dag_node = getNodeWithoutAliases(*array_join_input, expression_match_budget);
    const auto * input_array_type = input_dag_node && input_dag_node->result_type
        ? typeid_cast<const DataTypeArray *>(input_dag_node->result_type.get()) : nullptr;
    if (!input_array_type || !sameType(input_array_type->getNestedType(), array_join_value_type, expression_match_budget))
        return false;

    const DataTypePtr & effective_left_type = scalar_cast_targets.empty() ? array_join_value_type : scalar_cast_targets.front();

    size_t position = header.getPositionByName(array_input.column_name);
    const auto * array_type = typeid_cast<const DataTypeArray *>(header.getByPosition(position).type.get());
    if (!array_type)
        return false;

    const auto & array_nested_type = array_type->getNestedType();
    if (!isSupportedBloomFilterArrayElementType(array_nested_type) || !bloomFilterHashDomainMatches(type, array_nested_type))
        return false;

    /// A NULL set member does not cast to a non-Nullable indexed element and cannot match a stored value.
    const auto * nullable_column = typeid_cast<const ColumnNullable *>(column.get());
    if (nullable_column && !array_nested_type->isNullable()
        && std::ranges::any_of(nullable_column->getNullMapData(), [](UInt8 is_null) { return is_null != 0; }))
        return false;

    /// Both the generated-default oracle and the NULL-membership oracle require one completed
    /// scalar set. Tuple recursion deliberately passes nullptr, so decline before either oracle.
    if ((array_input.has_empty_array_to_single || is_null_in)
        && (!prepared_set || prepared_set->getDataTypes().size() != 1))
        return false;

    if ((array_input.has_empty_array_to_single || is_null_in) && !prepared_set->areTypesEqual(0, effective_left_type))
        return false;

    if (is_null_in && prepared_set->hasNull())
        return false;

    if (array_input.has_empty_array_to_single)
    {
        if (!array_input.generated_default_type)
            return false;

        ColumnPtr default_column = array_input.generated_default_type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();
        DataTypePtr default_type = array_input.generated_default_type;

        for (auto it = array_input.array_cast_targets.rbegin(); it != array_input.array_cast_targets.rend(); ++it)
        {
            default_column = castColumn(ColumnWithTypeAndName{default_column, default_type, ""}, *it);
            default_type = *it;
        }

        for (auto it = scalar_cast_targets.rbegin(); it != scalar_cast_targets.rend(); ++it)
        {
            default_column = castColumn(ColumnWithTypeAndName{default_column, default_type, ""}, *it);
            default_type = *it;
        }

        /// If the static wrapper map did not end at the ARRAY_JOIN left type, decline before
        /// constructing a Set probe. Accepted conversions above are already proven lossless.
        if (!default_type->equals(*effective_left_type))
            return false;

        ColumnsWithTypeAndName default_probe;
        default_probe.emplace_back(default_column, default_type, "");
        const auto default_match = prepared_set->execute(default_probe, false);
        if (assert_cast<const ColumnUInt8 &>(*default_match).getData()[0])
            return false;
    }

    const auto & converted_column = castColumn(ColumnWithTypeAndName{column, type, ""}, array_nested_type);
    if (array_input.has_empty_array_to_single && converted_column->getNumberOfDefaultRows() != 0)
        return false;

    out.predicate.emplace_back(
        std::make_pair(position, BloomFilterHash::hashWithColumn(array_nested_type, converted_column, 0, column->size())));
    out.function = is_in ? RPNElement::FUNCTION_HAS_ANY : RPNElement::FUNCTION_IN;
    return true;
}


/// The array-search functions coerce the constant with CAST before comparing it to the elements:
/// `hasAny`/`hasAll`, and `has`/`indexOf` over a `FixedString` element, cast both sides to the least
/// supertype (hasAllAny.h, arrayIndex.h `executeGeneric`), and `has`/`indexOf` over a `LowCardinality`
/// element cast the constant straight to the dictionary type (LowCardinalityExecutionHelpers.h
/// `dictionaryIndexForConstant`). A CAST of `FixedString` to `String` strips the trailing zero
/// padding, while `convertFieldToType` keeps it, so the index hashed a value the function never
/// compares and wrongly pruned granules.
///
/// Replicate that coercion at the `Field` level: strip the padding of a `FixedString` constant, then
/// re-pad it to the width of the element type, which is the stored form of every element the function
/// can match. Returns a null `Field` (the `convertFieldToType` convention) when no stored element can
/// match, or when the runtime CAST would throw `TOO_LARGE_STRING_SIZE`; the caller must then decline
/// the index, so that the error stays reachable instead of turning into silently pruned granules.
static Field coerceStringFieldLikeSearchFunction(
    const Field & field, const DataTypePtr & value_type, const DataTypePtr & actual_type, bool cast_to_supertype)
{
    if (field.isNull())
        return {};

    String value = field.safeGet<String>();
    const auto * fixed_string_type = typeid_cast<const DataTypeFixedString *>(actual_type.get());

    if (isFixedString(removeLowCardinalityAndNullable(value_type)))
    {
        /// The direct cast to a dictionary type rejects a `FixedString` constant wider than the
        /// element up front, by width alone, while the supertype cast strips the padding first.
        if (!cast_to_supertype && fixed_string_type && value.size() > fixed_string_type->getN())
            return {};

        value.resize(value.find_last_not_of('\0') + 1);
    }

    if (fixed_string_type)
    {
        if (value.size() > fixed_string_type->getN())
            return {};

        value.resize(fixed_string_type->getN(), '\0');
    }

    return Field(std::move(value));
}

/// True for the element/constant types whose comparison `coerceStringFieldLikeSearchFunction`
/// replicates. Numeric elements take `executeIntegral`, which compares without coercing.
static bool searchFunctionCoercesConstant(const DataTypePtr & value_type, const DataTypePtr & actual_type)
{
    return value_type
        && isStringOrFixedString(removeLowCardinalityAndNullable(value_type))
        && isStringOrFixedString(actual_type);
}

/// Convert a constant for hashing against the elements of an indexed array column, the way the
/// functions built on arrayIndex.h (`has`, `indexOf`, `mapContainsKey`, `mapContainsValue`,
/// `mapContains`, and `has` over a `Map`) compare it. Over a plain `String` element they compare
/// the constant's raw padded bytes (arrayIndex.h `executeString`), so the padded form is the value
/// to hash. The test must read the type before `getPrimitiveType` strips `LowCardinality`, whose
/// elements do coerce.
/// Over `Enum` values a `String` or `FixedString` constant is compared by the name of the enum value,
/// after the cast of both arguments to their common type `String`, which strips the padding of a
/// `FixedString` (arrayIndex.h `executeGeneric`, hasAllAny.h). Do the same, and return the value of the
/// enum to hash. A name that is not in the `Enum` does not throw there, it does not match, so return a
/// null `Field` for it: the caller declines the index instead of throwing `UNKNOWN_ELEMENT_OF_ENUM`
/// while it is prepared. Returns `std::nullopt` when the constant is not a name of an enum value.
static std::optional<Field> tryConvertEnumNameConstant(const Field & value_field, const DataTypePtr & value_type, const DataTypePtr & enum_type)
{
    if (!isEnum(enum_type) || value_field.getType() != Field::Types::String || !value_type
        || !isStringOrFixedString(removeLowCardinalityAndNullable(value_type)))
        return std::nullopt;

    String name = value_field.safeGet<String>();
    if (isFixedString(removeLowCardinalityAndNullable(value_type)))
        name.resize(name.find_last_not_of('\0') + 1);

    return tryConvertFieldToType(Field(std::move(name)), *enum_type, nullptr, {}, /* strict */ true);
}

static Field convertConstantForArrayIndexFunction(
    const Field & value_field, const DataTypePtr & value_type, const DataTypePtr & nested_type, const DataTypePtr & actual_type)
{
    if (auto enum_value = tryConvertEnumNameConstant(value_field, value_type, actual_type))
        return std::move(*enum_value);

    if (WhichDataType(removeNullable(nested_type)).isString() || !searchFunctionCoercesConstant(value_type, actual_type))
        return convertFieldToType(value_field, *actual_type, value_type.get());

    return coerceStringFieldLikeSearchFunction(value_field, value_type, actual_type, /*cast_to_supertype=*/ !nested_type->lowCardinality());
}

static ColumnPtr createColumnFromConstantArray(
    const Field & value_field, const DataTypePtr & value_type, const DataTypePtr & actual_type, bool coerce_like_search_function)
{
    if (value_field.getType() != Field::Types::Array)
        return nullptr;

    /// The elements' own type. `convertFieldToType` needs it to convert a value that carries a unit -
    /// a `Date` day number probed against an `Array(DateTime)` column - instead of re-basing its raw
    /// number as seconds, which hashes something the column can never hold. Every other call in this
    /// file passes the same hint.
    DataTypePtr element_type;
    if (value_type)
        if (const auto * value_array_type = typeid_cast<const DataTypeArray *>(removeLowCardinalityAndNullable(value_type).get()))
            element_type = value_array_type->getNestedType();

    const bool coerce = coerce_like_search_function && element_type && searchFunctionCoercesConstant(element_type, actual_type);
    const bool is_nullable = actual_type->isNullable();
    const auto * fixed_string_type = typeid_cast<const DataTypeFixedString *>(actual_type.get());
    auto mutable_column = actual_type->createColumn();

    for (const auto & f : value_field.safeGet<Array>())
    {
        if ((f.isNull() && !is_nullable) || f.isDecimal(f.getType())) /// NOLINT(readability-static-accessed-through-instance)
            return nullptr;

        /// `hasAny`/`hasAll` compare a `String` or `FixedString` constant with `Enum` elements by the name
        /// of the enum value, see `tryConvertEnumNameConstant`.
        std::optional<Field> enum_value;
        if (coerce_like_search_function)
            enum_value = tryConvertEnumNameConstant(f, element_type, actual_type);

        Field converted;
        if (enum_value)
            converted = std::move(*enum_value);
        else if (coerce)
            converted = coerceStringFieldLikeSearchFunction(f, element_type, actual_type, /*cast_to_supertype=*/ true);
        else
            converted = convertFieldToType(f, *actual_type, element_type.get());
        if (converted.isNull())
            return nullptr;

        /// `has(<constant array>, <indexed scalar>)` compares the `Field`s without a cast, and an
        /// `Enum` element is converted to its name, which can be wider than the indexed
        /// `FixedString(N)` - the name is not truncated to it. Such a value cannot match a narrower
        /// `FixedString` scalar, but `ColumnFixedString::insert` would throw `TOO_LARGE_STRING_SIZE`
        /// while preparing the index. Decline the index and let the function evaluate normally
        /// instead. The `coerce` branch rejects an over-wide value on its own.
        if (fixed_string_type && converted.getType() == Field::Types::String
            && converted.safeGet<String>().size() > fixed_string_type->getN())
        {
            return nullptr;
        }

        mutable_column->insert(converted);
    }

    return std::move(mutable_column);
}


static bool indexOfCanUseBloomFilter(const RPNBuilderTreeNode * parent)
{
    if (!parent)
        return true;

    if (!parent->isFunction())
        return false;

    auto function = parent->toFunctionNode();
    auto function_name = function.getFunctionName();

    /// `parent` is a function where `indexOf` is located.
    /// Example: `indexOf(arr, x) = 1`, parent is a function named `equals`.
    if (function_name == "and")
    {
        return true;
    }
    if (function_name == "equals" /// notEquals is not applicable
        || function_name == "greater" || function_name == "greaterOrEquals" || function_name == "less" || function_name == "lessOrEquals")
    {
        size_t function_arguments_size = function.getArgumentsSize();
        if (function_arguments_size != 2)
            return false;

        /// We don't allow constant expressions like `indexOf(arr, x) = 1 + 0` but it's negligible.

        /// We should return true when the corresponding expression implies that the array contains the element.
        /// Example: when `indexOf(arr, x)` > 10 is written, it means that arr definitely should contain the element
        /// (at least at 11th position but it does not matter).

        bool reversed = false;
        Field constant_value;
        DataTypePtr constant_type;

        if (function.getArgumentAt(0).tryGetConstant(constant_value, constant_type))
        {
            reversed = true;
        }
        else if (function.getArgumentAt(1).tryGetConstant(constant_value, constant_type))
        {
        }
        else
        {
            return false;
        }

        Field zero(0);
        bool constant_equal_zero = accurateEquals(constant_value, zero);

        if (function_name == "equals" && !constant_equal_zero)
        {
            /// indexOf(...) = c, c != 0
            return true;
        }
        if (function_name == "notEquals" && constant_equal_zero)
        {
            /// indexOf(...) != c, c = 0
            return true;
        }
        if (function_name == (reversed ? "less" : "greater") && !accurateLess(constant_value, zero))
        {
            /// indexOf(...) > c, c >= 0
            return true;
        }
        if (function_name == (reversed ? "lessOrEquals" : "greaterOrEquals")
            && accurateLess(zero, constant_value))
        {
            /// indexOf(...) >= c, c > 0
            return true;
        }

        return false;
    }

    return false;
}


bool MergeTreeIndexConditionBloomFilter::traverseTreeEquals(
    const String & function_name,
    const RPNBuilderTreeNode & wrapped_key_node,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out,
    const RPNBuilderTreeNode * parent)
{
    const auto key_node = unwrapLosslessConversion(wrapped_key_node);
    auto key_column_name = key_node.getColumnName();

    /// `arrayJoin(col) = const` needs an element equal to the constant, same as `has(col, const)`.
    /// `notEquals` is not derivable: a granule holding the constant still yields differing rows.
    if (function_name == "equals")
    {
        if (auto array_join_argument = key_node.getArrayJoinArgument())
        {
            bool padded = false;
            auto array_column_name = getArrayJoinColumnName(*array_join_argument, header, padded);
            if (header.has(array_column_name))
            {
                size_t position = header.getPositionByName(array_column_name);
                const auto * array_type = typeid_cast<const DataTypeArray *>(header.getByPosition(position).type.get());
                if (array_type && bloomFilterHashDomainMatches(value_type, array_type->getNestedType()))
                {
                    const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());
                    if (!validate_enum_literals_in_operators && isUnknownEnumElement(*actual_type, value_field))
                    {
                        out.function = RPNElement::ALWAYS_FALSE;
                        return true;
                    }

                    auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
                    if (converted_field.isNull())
                        return false;

                    if (padded && array_type->getNestedType()->createColumnConst(1, converted_field)->isDefaultAt(0))
                        return false;

                    out.function = RPNElement::FUNCTION_HAS;
                    out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));
                    return true;
                }
            }
        }
    }

    if (header.has(key_column_name))
    {
        size_t position = header.getPositionByName(key_column_name);
        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(index_type.get());

        if (function_name == "has" || function_name == "indexOf")
        {
            if (array_type)
            {
                /// We can treat `indexOf` function similar to `has`.
                /// But it is little more cumbersome, compare: `has(arr, elem)` and `indexOf(arr, elem) != 0`.
                /// The `parent` in this context is expected to be function `!=` (`notEquals`).
                if (function_name == "has" || indexOfCanUseBloomFilter(parent))
                {
                    out.function = RPNElement::FUNCTION_HAS;
                    /// The function coerces the constant by the element type it sees, which may differ in `LowCardinality`.
                    DataTypePtr nested_type = array_type->getNestedType();
                    if (const auto * wrapped_array_type = typeid_cast<const DataTypeArray *>(wrapped_key_node.getDAGNode()->result_type.get()))
                        nested_type = wrapped_array_type->getNestedType();

                    const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());
                    Field converted_field = convertConstantForArrayIndexFunction(value_field, value_type, nested_type, actual_type);
                    if (converted_field.isNull())
                        return false;

                    out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));
                }
            }
            else if (function_name == "has")
            {
                /// `has(<constant array>, <indexed scalar>)` compares `Field`s directly
                /// (arrayIndex.h `executeConst`), so it needs the padded form and no coercion.
                const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
                ColumnPtr column = createColumnFromConstantArray(value_field, value_type, actual_type, /*coerce_like_search_function=*/ false);

                if (!column)
                    return false;

                out.function = RPNElement::FUNCTION_HAS_ANY;
                out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithColumn(actual_type, column, 0, column->size())));
            }
        }
        else if (function_name == "hasAny" || function_name == "hasAll")
        {
            if (!array_type)
                return false;

            const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());
            ColumnPtr column = createColumnFromConstantArray(value_field, value_type, actual_type, /*coerce_like_search_function=*/ true);

            if (!column)
                return false;

            out.function = function_name == "hasAny" ?
                RPNElement::FUNCTION_HAS_ANY :
                RPNElement::FUNCTION_HAS_ALL;
            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithColumn(actual_type, column, 0, column->size())));
        }
        else
        {
            if (array_type)
                return false;

            out.function = RPNElement::FUNCTION_EQUALS;
            const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);

            /// Where equality compares zero-padded, the constant can equal a stored value of a different byte
            /// length, while the index holds only the hash of each value's exact bytes. It is then usable only
            /// for a `FixedString(N)` index at least as wide, where padding gives the one value that can match.
            if (isStringOrFixedString(actual_type) && value_field.getType() == Field::Types::String)
            {
                /// A `Variant` or `Dynamic` constant carries the nested padded value under its
                /// declared type, so an active `FixedString` alternative cannot be told from a `String` one.
                const WhichDataType which_constant(removeLowCardinalityAndNullable(value_type));
                const bool constant_may_be_fixed_string
                    = which_constant.isFixedString() || which_constant.isVariant() || which_constant.isDynamic();
                const size_t constant_bytes = value_field.safeGet<String>().size();
                const auto * fixed_index_type = typeid_cast<const DataTypeFixedString *>(actual_type.get());

                if (constant_may_be_fixed_string && !fixed_index_type)
                    return false;

                if (fixed_index_type && fixed_index_type->getN() < constant_bytes)
                    return false;
            }

            /// With validation off, `equals` against a name the enum lacks is constant false for every row,
            /// so the conversion below must not throw; `KeyCondition` folds the same way.
            if (function_name == "equals" && !validate_enum_literals_in_operators && isUnknownEnumElement(*actual_type, value_field))
            {
                out.function = RPNElement::ALWAYS_FALSE;
                return true;
            }

            auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
            if (converted_field.isNull())
                return false;

            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));
        }

        return true;
    }

    /// Try to match the column name to a JSONAllPaths index for JSON subcolumn filtering.
    /// tryMatchNodeToJSONIndex handles both plain subcolumns and CAST-wrapped expressions
    /// like `json.some.path = value`, `json.some.path.:Type = value`, or `json.path::Type = value`.
    if (auto json_info = tryMatchNodeToJSONIndex(key_node, header, "JSONAllPaths", json_argument_types))
    {
        if (function_name != "equals")
            return false;

        auto key_type = key_node.getDAGNode()->result_type;
        if (!isJSONPathFilterSafe(key_type, value_field, value_type))
            return false;

        out.function = RPNElement::FUNCTION_EQUALS;
        fillJSONPathBloomPredicate(*json_info, header, out);

        return true;
    }

    if (function_name == "mapContainsValue" || function_name == "mapContainsKey" || function_name == "mapContains" || function_name == "has")
    {
        auto map_keys_index_column_name = fmt::format("mapKeys({})", key_column_name);
        if (function_name == "mapContainsValue")
            map_keys_index_column_name = fmt::format("mapValues({})", key_column_name);

        if (!header.has(map_keys_index_column_name))
            return false;

        size_t position = header.getPositionByName(map_keys_index_column_name);

        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(index_type.get());

        if (!array_type)
            return false;

        /// `mapContainsKey`/`mapContainsValue`/`mapContains`, and `has` over a `Map`, are adapters
        /// of the same arrayIndex.h machinery, so the constant must be coerced the same way as for
        /// `has` over an array. The `mapKeys`/`mapValues` index expression strips the
        /// `LowCardinality` wrapper of the key/value type, so the coercion mode must be read from
        /// the `Map` type itself, not from the index header.
        DataTypePtr element_type;
        if (const auto * map_type = typeid_cast<const DataTypeMap *>(key_node.getDAGNode()->result_type.get()))
        {
            element_type = function_name == "mapContainsValue" ? map_type->getValueType() : map_type->getKeyType();

            /// The `mapContains*` adapters run over the keys/values subcolumn, which keeps the
            /// wrapper, so they compare against the dictionary. `has` over a `Map` instead goes
            /// through `executeMap`, which rewrites the map to an array of its keys and strips
            /// `LowCardinality` from both arguments before comparing (arrayIndex.h), so it compares
            /// the raw padded bytes exactly like `has` over an `Array(String)`.
            if (function_name == "has")
                element_type = recursiveRemoveLowCardinality(element_type);
        }

        out.function = RPNElement::FUNCTION_HAS;
        const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());

        /// Without the `Map` type the padded and the coerced form cannot be told apart.
        if (!element_type && searchFunctionCoercesConstant(value_type, actual_type))
            return false;

        Field converted_field = element_type
            ? convertConstantForArrayIndexFunction(value_field, value_type, element_type, actual_type)
            : convertFieldToType(value_field, *actual_type, value_type.get());
        if (converted_field.isNull())
            return false;

        out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));
        return true;
    }

    if (key_node.isFunction())
    {
        WhichDataType which(value_type);

        auto key_node_function = key_node.toFunctionNode();
        auto key_node_function_name = key_node_function.getFunctionName();
        size_t key_node_function_arguments_size = key_node_function.getArgumentsSize();

        if (which.isTuple() && key_node_function_name == "tuple")
        {
            const Tuple & tuple = value_field.safeGet<Tuple>();
            const auto * value_tuple_data_type = typeid_cast<const DataTypeTuple *>(value_type.get());

            if (tuple.size() != key_node_function_arguments_size)
                return false;

            bool match_with_subtype = false;
            const DataTypes & subtypes = value_tuple_data_type->getElements();

            for (size_t index = 0; index < tuple.size(); ++index)
                match_with_subtype |= traverseTreeEquals(function_name, key_node_function.getArgumentAt(index), subtypes[index], tuple[index], out, &key_node);

            return match_with_subtype;
        }
    }

    /// Handle both `arrayElement(map, 'key') = value` and `map.key_<serialized_key> = value`.
    if (function_name == "equals")
    {
        if (auto map_info = tryResolveMapInfoFromNode(key_node, header, columns_shadowing_map_subcolumns))
        {
            /** It is important to ignore keys like column_map['Key'] = '' because if the key does not exist in the map
              * we return the default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where the map key does not exist.
              */
            if (value_field == value_type->getDefault())
                return false;

            /// Over `Enum` map values the constant can be the name of the enum value, a `String`, which has
            /// to be converted to the value of the enum before it is hashed and compared with the default.
            /// The default value of an `Enum` is not the default value of the type of the constant. For a
            /// missing key `arrayElement` returns the zero of the underlying integer, which is not necessarily
            /// the default value of the `Enum` (its first value), so decline the index for both.
            Field map_value_field = value_field;
            if (const auto * dag_node = key_node.getDAGNode())
            {
                const DataTypePtr map_value_type = removeLowCardinalityAndNullable(dag_node->result_type);
                if (isEnum(map_value_type))
                {
                    if (auto enum_value = tryConvertEnumNameConstant(value_field, value_type, map_value_type))
                        map_value_field = std::move(*enum_value);
                    else
                        map_value_field = tryConvertFieldToType(value_field, *map_value_type, value_type.get(), {}, /* strict */ true);

                    if (map_value_field.isNull() || map_value_field == Field(Int64(0)) || map_value_field == map_value_type->getDefault())
                        return false;
                }
            }

            size_t position = 0;
            Field const_value;

            if (map_info->has_keys_index)
            {
                position = map_info->keys_index_position;
                const_value = map_info->key_field;
            }
            else if (map_info->has_values_index)
            {
                position = map_info->values_index_position;
                const_value = map_value_field;
            }
            else
            {
                return false;
            }

            out.function = RPNElement::FUNCTION_EQUALS;

            const auto & index_type = header.getByPosition(position).type;
            const auto actual_type = BloomFilter::getPrimitiveType(index_type);
            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), const_value)));

            return true;
        }
    }

    return false;
}

MergeTreeIndexAggregatorBloomFilter::MergeTreeIndexAggregatorBloomFilter(
    size_t bits_per_row_, size_t hash_functions_, const Names & columns_name_)
    : bits_per_row(bits_per_row_), hash_functions(hash_functions_), index_columns_name(columns_name_), column_hashes(columns_name_.size())
{
    chassert(bits_per_row != 0);
    chassert(hash_functions != 0);
}

bool MergeTreeIndexAggregatorBloomFilter::empty() const
{
    return !total_rows;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorBloomFilter::getGranuleAndReset()
{
    const auto granule = std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, column_hashes);
    total_rows = 0;
    column_hashes.clear();
    return granule;
}

void MergeTreeIndexAggregatorBloomFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                        "Position: {}, Block rows: {}.", *pos, block.rows());

    Block granule_index_block;
    size_t max_read_rows = std::min(block.rows() - *pos, limit);

    for (size_t column = 0; column < index_columns_name.size(); ++column)
    {
        const auto & column_and_type = block.getByName(index_columns_name[column]);
        /// A bloom filter only needs the set of distinct hashes, so for LowCardinality
        /// columns this returns one hash per distinct dictionary value present in the
        /// granule instead of one per row -- turning O(rows) hash-set inserts into
        /// O(distinct). For other columns it is one hash per row, as before.
        auto index_column = BloomFilterHash::hashWithColumnDistinct(column_and_type.type, column_and_type.column, *pos, max_read_rows);

        const auto & index_col = checkAndGetColumn<ColumnUInt64>(*index_column);
        const auto & index_data = index_col.getData();
        for (const auto & hash: index_data)
            column_hashes[column].insert(hash);
    }

    *pos += max_read_rows;
    total_rows += max_read_rows;
}

MergeTreeIndexBloomFilter::MergeTreeIndexBloomFilter(
    StorageMetadataPtr metadata_snapshot_,
    const IndexDescription & index_,
    size_t bits_per_row_,
    size_t hash_functions_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
{
    chassert(bits_per_row != 0);
    chassert(hash_functions != 0);
}

MergeTreeIndexGranulePtr MergeTreeIndexBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, index.column_names.size());
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBloomFilter::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorBloomFilter>(bits_per_row, hash_functions, index.column_names);
}

MergeTreeIndexConditionPtr MergeTreeIndexBloomFilter::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionBloomFilter>(
        predicate, context, index.sample_block, index.expression->getActionsDAG(), hash_functions,
        getColumnsShadowingMapSubcolumns(), collectJSONIndexArgumentTypes(*index.expression));
}

static void assertIndexColumnsType(const Block & header)
{
    if (header.empty() || !header.columns())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Index must have columns.");

    const DataTypes & columns_data_types = header.getDataTypes();

    for (const auto & type : columns_data_types)
    {
        const IDataType * actual_type = BloomFilter::getPrimitiveType(type).get();
        WhichDataType which(actual_type);

        if (!which.isUInt() && !which.isInt() && !which.isString() && !which.isFixedString() && !which.isFloat() &&
            !which.isDate() && !which.isDateTime() && !which.isDateTime64() && !which.isEnum() && !which.isUUID() &&
            !which.isIPv4() && !which.isIPv6())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type {} of bloom filter index.", type->getName());
    }
}

MergeTreeIndexPtr bloomFilterIndexCreator(
    StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & /*settings*/)
{
    double false_positive_rate = 0.025;

    if (index.arguments && !index.arguments->children.empty())
    {
        auto argument = getFieldFromIndexArgumentAST(index.arguments->children[0]);
        false_positive_rate = std::min<Float64>(1.0, std::max<Float64>(argument.safeGet<Float64>(), 0.0));
    }

    const auto & bits_per_row_and_size_of_hash_functions = BloomFilterHash::calculationBestPractices(false_positive_rate);

    return std::make_shared<MergeTreeIndexBloomFilter>(
        std::move(metadata_snapshot), index, bits_per_row_and_size_of_hash_functions.first, bits_per_row_and_size_of_hash_functions.second);
}

void bloomFilterIndexValidator(const IndexDescription & index, bool attach, const MergeTreeSettings & /*settings*/)
{
    assertIndexColumnsType(index.sample_block);

    /// The index hashing rejects an array of nullable elements (see `unwrapArraySlice` in
    /// `BloomFilterHash.h`), which `assertIndexColumnsType` does not notice because it looks at the
    /// primitive type only. Such an index is accepted at DDL and then fails every insert, merge and
    /// mutation of the table, and the natural recovery - `DROP INDEX` - is refused while one of those
    /// failed mutations is pending. Reject it here, like a nested array already is. Not on `ATTACH`:
    /// a table created before this check must still load, so that its index can be dropped.
    if (!attach)
    {
        for (const auto & type : index.sample_block.getDataTypes())
        {
            const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
            if (array_type && array_type->getNestedType()->isNullable())
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type {} of bloom filter index.", type->getName());
        }
    }

    if (index.arguments && index.arguments->children.size() > 1)
    {
        if (!attach) /// This is for backward compatibility.
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "BloomFilter index cannot have more than one parameter.");
    }

    if (index.arguments && !index.arguments->children.empty())
    {
        auto argument = getFieldFromIndexArgumentAST(index.arguments->children[0]);

        if (!attach && (argument.getType() != Field::Types::Float64 || argument.safeGet<Float64>() < 0 || argument.safeGet<Float64>() > 1))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The BloomFilter false positive must be a double number between 0 and 1.");
    }
}

}
