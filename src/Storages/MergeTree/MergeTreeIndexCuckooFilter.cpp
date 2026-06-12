#include <Storages/MergeTree/MergeTreeIndexCuckooFilter.h>

#include <cmath>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldAccurateComparison.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/BloomFilterHash.h>
#include <Interpreters/CuckooFilter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Storages/IndicesDescription.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/RPNBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_QUERY;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleCuckooFilter::MergeTreeIndexGranuleCuckooFilter(double /* false_positive_rate */, size_t f_bits_, size_t index_columns_)
    : f_bits(f_bits_), cuckoo_filters(index_columns_)
{
    total_rows = 0;
    for (size_t column = 0; column < index_columns_; ++column)
        cuckoo_filters[column] = std::make_shared<CuckooFilter>(CuckooFilter::empty(f_bits));
}

MergeTreeIndexGranuleCuckooFilter::MergeTreeIndexGranuleCuckooFilter(
    double false_positive_rate_, size_t f_bits_, const std::vector<HashSet<UInt64>> & column_hashes_)
    : f_bits(f_bits_), cuckoo_filters(column_hashes_.size())
{
    if (column_hashes_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule index hash sets are empty.");

    size_t max_distinct = 0;
    for (const auto & column_hash : column_hashes_)
        max_distinct = std::max(max_distinct, column_hash.size());

    total_rows = max_distinct;

    for (size_t column = 0, columns = column_hashes_.size(); column < columns; ++column)
        cuckoo_filters[column] = std::make_shared<CuckooFilter>(CuckooFilter::buildFromHashes(column_hashes_[column], false_positive_rate_));
}

bool MergeTreeIndexGranuleCuckooFilter::empty() const
{
    return !total_rows;
}

size_t MergeTreeIndexGranuleCuckooFilter::memoryUsageBytes() const
{
    size_t sum = 0;
    for (const auto & filter : cuckoo_filters)
        sum += filter->memoryUsageBytes();
    return sum;
}

void MergeTreeIndexGranuleCuckooFilter::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    UInt64 total_rows_u64 = 0;
    readVarUInt(total_rows_u64, istr);
    total_rows = static_cast<size_t>(total_rows_u64);

    for (auto & filter : cuckoo_filters)
    {
        filter = std::make_shared<CuckooFilter>(CuckooFilter::empty(f_bits));
        filter->deserializeBinary(istr, version);
    }
}

void MergeTreeIndexGranuleCuckooFilter::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty cuckoo filter index.");

    writeVarUInt(static_cast<UInt64>(total_rows), ostr);

    for (const auto & filter : cuckoo_filters)
        filter->serializeBinary(ostr);
}

namespace
{

ColumnWithTypeAndName getPreparedSetInfo(const ConstSetPtr & prepared_set)
{
    if (prepared_set->getDataTypes().size() == 1)
        return {prepared_set->getSetElements()[0], prepared_set->getElementsTypes()[0], "dummy"};

    Columns set_elements;
    for (auto & set_element : prepared_set->getSetElements())

        set_elements.emplace_back(set_element->convertToFullColumnIfConst());

    return {ColumnTuple::create(set_elements), std::make_shared<DataTypeTuple>(prepared_set->getElementsTypes()), "dummy"};
}

bool maybeTrueOnCuckooFilter(const IColumn * hash_column, const CuckooFilterPtr & cuckoo_filter, bool match_all)
{
    const auto * const_column = typeid_cast<const ColumnConst *>(hash_column);
    const auto * non_const_column = typeid_cast<const ColumnUInt64 *>(hash_column);

    if (!const_column && !non_const_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash column must be Const or UInt64.");

    if (const_column)
        return cuckoo_filter->contains(const_column->getValue<UInt64>());

    const ColumnUInt64::Container & hashes = non_const_column->getData();

    if (match_all)
    {
        return std::all_of(hashes.begin(), hashes.end(), [&](UInt64 hash_row) { return cuckoo_filter->contains(hash_row); });
    }

    return std::any_of(hashes.begin(), hashes.end(), [&](UInt64 hash_row) { return cuckoo_filter->contains(hash_row); });
}

/// Information about a Map column's presence in the cuckoo filter index.
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

/// Try to resolve a Map column against the cuckoo filter index header by the map column name
/// and the key as a Field. Returns std::nullopt if neither `mapKeys(<col>)` nor `mapValues(<col>)`
/// is present in the index.
std::optional<MapIndexInfo> tryResolveMapIndexInfo(const String & map_column_name, const Field & key_field, const Block & header)
{
    auto map_keys_index_column_name = fmt::format("mapKeys({})", map_column_name);
    auto map_values_index_column_name = fmt::format("mapValues({})", map_column_name);

    auto keys_position = header.findPositionByName(map_keys_index_column_name);
    auto values_position = header.findPositionByName(map_values_index_column_name);

    if (!keys_position && !values_position)
        return std::nullopt;

    MapIndexInfo info;
    info.key_field = key_field;
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
/// against the cuckoo filter index header. The subcolumn name format is produced by
/// `FunctionToSubcolumnsPass`.
std::optional<MapIndexInfo> tryParseMapSubcolumn(const String & column_name, const Block & header)
{
    auto parsed = tryParseMapSubcolumnName(column_name);
    if (!parsed)
        return std::nullopt;

    auto & [map_column_name, serialized_key] = *parsed;

    auto map_keys_index_column_name = fmt::format("mapKeys({})", map_column_name);
    if (!header.has(map_keys_index_column_name))
        return tryResolveMapIndexInfo(map_column_name, {}, header);

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

    return tryResolveMapIndexInfo(map_column_name, key_field, header);
}

/// Try to resolve a `MapIndexInfo` from a key node that is either an `arrayElement(map, key)`
/// function call or a `map.key_<serialized_key>` subcolumn reference.
std::optional<MapIndexInfo> tryResolveMapInfoFromNode(const RPNBuilderTreeNode & key_node, const Block & header)
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

            return tryResolveMapIndexInfo(first_argument.getColumnName(), constant_value, header);
        }
    }

    return tryParseMapSubcolumn(key_node.getColumnName(), header);
}

}

MergeTreeIndexConditionCuckooFilter::MergeTreeIndexConditionCuckooFilter(
    const ActionsDAG::Node * predicate, ContextPtr context_, const Block & header_)
    : WithContext(context_), header(header_)
{
    if (!predicate)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        predicate,
        context_,
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
}

bool MergeTreeIndexConditionCuckooFilter::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(
        rpn,
        {RPNElement::FUNCTION_EQUALS,
         RPNElement::FUNCTION_NOT_EQUALS,
         RPNElement::FUNCTION_HAS,
         RPNElement::FUNCTION_HAS_ANY,
         RPNElement::FUNCTION_HAS_ALL,
         RPNElement::FUNCTION_IN,
         RPNElement::FUNCTION_NOT_IN});
}

bool MergeTreeIndexConditionCuckooFilter::mayBeTrueOnGranule(const MergeTreeIndexGranuleCuckooFilter * granule, const UpdatePartialDisjunctionResultFn & update_partial_result_disjunction_fn) const
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
            case RPNElement::FUNCTION_NOT_IN:
            case RPNElement::FUNCTION_EQUALS:
            case RPNElement::FUNCTION_NOT_EQUALS:
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

                    match_rows = maybeTrueOnCuckooFilter(&*hash_column, filter, match_all);
                }

                rpn_stack.emplace_back(match_rows, true);
                if (element.function == RPNElement::FUNCTION_NOT_EQUALS || element.function == RPNElement::FUNCTION_NOT_IN)
                    rpn_stack.back() = !rpn_stack.back();
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

        if (update_partial_result_disjunction_fn)
        {
            update_partial_result_disjunction_fn(element_idx, rpn_stack.back().can_be_true, element.function == RPNElement::FUNCTION_UNKNOWN);
            ++element_idx;
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in MergeTreeIndexConditionCuckooFilter::mayBeTrueOnGranule");

    return rpn_stack[0].can_be_true;
}

bool MergeTreeIndexConditionCuckooFilter::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
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

    return traverseFunction(node, out, nullptr /*parent*/);
}

namespace
{

/// Hash the JSON path string and append a predicate entry for cuckoo filter index.
void fillJSONPathCuckooPredicate(
    const JSONSubcolumnIndexInfo & json_info,
    const Block & header,
    MergeTreeIndexConditionCuckooFilter::RPNElement & out)
{
    const DataTypePtr & index_type = header.getByPosition(json_info.header_position).type;
    const auto actual_type = BloomFilter::getPrimitiveType(index_type);
    Field path_field(json_info.path);
    out.predicate.emplace_back(std::make_pair(
        json_info.header_position,
        BloomFilterHash::hashWithField(actual_type.get(), path_field)));
}

}

bool MergeTreeIndexConditionCuckooFilter::traverseFunction(const RPNBuilderTreeNode & node, RPNElement & out, const RPNBuilderTreeNode * parent)
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
            if (traverseFunction(argument, out, &node))
                return true;
        }
    }

    /// Handle isNotNull for JSON subcolumns: isNotNull(json.some.path)
    /// When a JSON path is absent, the value is NULL (for Dynamic/Nullable types),
    /// so isNotNull(NULL) = false — always safe to skip granules where path is absent.
    /// Only for a bare atom: nested forms like isNotNull(json.a) = 0 must not use this fast path.
    if (parent == nullptr && function_name == "isNotNull" && arguments_size == 1)
    {
        auto arg = function.getArgumentAt(0);
        if (auto json_info = tryMatchNodeToJSONIndex(arg, header, "JSONAllPaths"))
        {
            auto arg_type = arg.getDAGNode()->result_type;
            /// It doesn't make sense to use cuckoo filter for isNotNull on non-Nullable type, as isNotNull will be always true.
            if (!canContainNull(*arg_type))
                return false;

            fillJSONPathCuckooPredicate(*json_info, header, out);
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
        if (auto future_set = rhs_argument.tryGetPreparedSet(); future_set)
        {
            if (auto prepared_set = future_set->buildOrderedSetInplace(rhs_argument.getTreeContext().getQueryContext()); prepared_set)
            {
                if (prepared_set->hasExplicitSetElements())
                {
                    const auto prepared_info = getPreparedSetInfo(prepared_set);
                    if (traverseTreeIn(function_name, lhs_argument, prepared_set, prepared_info.type, prepared_info.column, out))
                        return true;
                }
            }
        }
        return false;
    }

    if (function_name == "equals" ||
        function_name == "notEquals" ||
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
            (function_name == "equals" || function_name == "has" || function_name == "hasAny" || function_name == "notEquals"))
        {
            if (traverseTreeEquals(function_name, rhs_argument, const_type, const_value, out, parent))
                return true;
        }

        return false;
    }

    return false;
}

bool MergeTreeIndexConditionCuckooFilter::traverseTreeIn(
    const String & function_name,
    const RPNBuilderTreeNode & key_node,
    const ConstSetPtr & prepared_set,
    const DataTypePtr & type,
    const ColumnPtr & column,
    RPNElement & out)
{
    auto key_node_column_name = key_node.getColumnName();

    if (header.has(key_node_column_name))
    {
        size_t row_size = column->size();
        size_t position = header.getPositionByName(key_node_column_name);
        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto & converted_column = castColumn(ColumnWithTypeAndName{column, type, ""}, index_type);
        out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithColumn(index_type, converted_column, 0, row_size)));

        if (function_name == "in"  || function_name == "globalIn")
            out.function = RPNElement::FUNCTION_IN;

        if (function_name == "notIn"  || function_name == "globalNotIn")
            out.function = RPNElement::FUNCTION_NOT_IN;

        return true;
    }

    /// Try to match the column name to a JSONAllPaths index for JSON subcolumn IN filtering.
    /// tryMatchNodeToJSONIndex handles both plain subcolumns and CAST-wrapped expressions.
    /// NOT IN is not supported because after BoolMask inversion it never skips any granules.
    if (auto json_info = tryMatchNodeToJSONIndex(key_node, header, "JSONAllPaths"))
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

        fillJSONPathCuckooPredicate(*json_info, header, out);
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
                match_with_subtype |= traverseTreeIn(function_name, key_node_function.getArgumentAt(index), nullptr, sub_data_types[index], sub_columns[index], out);

            return match_with_subtype;
        }
    }

    /// Handle both `arrayElement(map, 'key') IN (set)` and `map.key_<serialized_key> IN (set)`.
    if (auto map_info = tryResolveMapInfoFromNode(key_node, header))
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

        if (map_info->has_keys_index)
        {
            /// For mapKeys we serialize key argument with cuckoo filter
            size_t position = map_info->keys_index_position;
            const DataTypePtr & index_type = header.getByPosition(position).type;
            const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), map_info->key_field)));
        }
        else if (map_info->has_values_index)
        {
            /// For mapValues we serialize set with cuckoo filter
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

        if (function_name == "in" || function_name == "globalIn")
            out.function = RPNElement::FUNCTION_IN;

        if (function_name == "notIn" || function_name == "globalNotIn")
            out.function = RPNElement::FUNCTION_NOT_IN;

        return true;
    }

    return false;
}


static ColumnPtr createColumnFromConstantArray(const Field & value_field, const DataTypePtr & actual_type)
{
    if (value_field.getType() != Field::Types::Array)
        return nullptr;

    const bool is_nullable = actual_type->isNullable();
    auto mutable_column = actual_type->createColumn();

    for (const auto & f : value_field.safeGet<Array>())
    {
        if ((f.isNull() && !is_nullable) || f.isDecimal(f.getType())) /// NOLINT(readability-static-accessed-through-instance)
            return nullptr;

        auto converted = convertFieldToType(f, *actual_type);
        if (converted.isNull())
            return nullptr;

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


bool MergeTreeIndexConditionCuckooFilter::traverseTreeEquals(
    const String & function_name,
    const RPNBuilderTreeNode & key_node,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out,
    const RPNBuilderTreeNode * parent)
{
    auto key_column_name = key_node.getColumnName();

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
                    const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());
                    auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
                    if (converted_field.isNull())
                        return false;

                    out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));
                }
            }
            else if (function_name == "has")
            {
                const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
                ColumnPtr column = createColumnFromConstantArray(value_field, actual_type);

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
            ColumnPtr column = createColumnFromConstantArray(value_field, actual_type);

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

            out.function = function_name == "equals" ? RPNElement::FUNCTION_EQUALS : RPNElement::FUNCTION_NOT_EQUALS;
            const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
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
    if (auto json_info = tryMatchNodeToJSONIndex(key_node, header, "JSONAllPaths"))
    {
        if (function_name != "equals")
            return false;

        auto key_type = key_node.getDAGNode()->result_type;
        if (!isJSONPathFilterSafe(key_type, value_field))
            return false;

        out.function = RPNElement::FUNCTION_EQUALS;
        fillJSONPathCuckooPredicate(*json_info, header, out);

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

        out.function = RPNElement::FUNCTION_HAS;
        const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());
        auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
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
    if (function_name == "equals" || function_name == "notEquals")
    {
        if (auto map_info = tryResolveMapInfoFromNode(key_node, header))
        {
            /** It is important to ignore keys like column_map['Key'] = '' because if the key does not exist in the map
              * we return the default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where the map key does not exist.
              */
            if (value_field == value_type->getDefault())
                return false;

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
                const_value = value_field;
            }
            else
            {
                return false;
            }

            out.function = function_name == "equals" ? RPNElement::FUNCTION_EQUALS : RPNElement::FUNCTION_NOT_EQUALS;

            const auto & index_type = header.getByPosition(position).type;
            const auto actual_type = BloomFilter::getPrimitiveType(index_type);
            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), const_value)));

            return true;
        }
    }

    return false;
}

MergeTreeIndexAggregatorCuckooFilter::MergeTreeIndexAggregatorCuckooFilter(
    double false_positive_rate_, size_t f_bits_, const Names & columns_name_)
    : false_positive_rate(false_positive_rate_), f_bits(f_bits_), index_columns_name(columns_name_), column_hashes(columns_name_.size())
{
}

bool MergeTreeIndexAggregatorCuckooFilter::empty() const
{
    return !total_rows;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorCuckooFilter::getGranuleAndReset()
{
    const auto granule = std::make_shared<MergeTreeIndexGranuleCuckooFilter>(false_positive_rate, f_bits, column_hashes);
    total_rows = 0;
    for (auto & hashes : column_hashes)
        hashes.clear();
    return granule;
}

void MergeTreeIndexAggregatorCuckooFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                        "Position: {}, Block rows: {}.", *pos, block.rows());

    Block granule_index_block;
    size_t max_read_rows = std::min(block.rows() - *pos, limit);

    for (size_t column = 0; column < index_columns_name.size(); ++column)
    {
        const auto & column_and_type = block.getByName(index_columns_name[column]);
        auto index_column = BloomFilterHash::hashWithColumn(column_and_type.type, column_and_type.column, *pos, max_read_rows);

        const auto & index_col = checkAndGetColumn<ColumnUInt64>(*index_column);
        const auto & index_data = index_col.getData();
        for (const auto & hash: index_data)
            column_hashes[column].insert(hash);
    }

    *pos += max_read_rows;
    total_rows += max_read_rows;
}

MergeTreeIndexCuckooFilter::MergeTreeIndexCuckooFilter(
    const IndexDescription & index_,
    double false_positive_rate_,
    size_t f_bits_)
    : IMergeTreeIndex(index_)
    , false_positive_rate(false_positive_rate_)
    , f_bits(f_bits_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexCuckooFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleCuckooFilter>(false_positive_rate, f_bits, index.column_names.size());
}

MergeTreeIndexAggregatorPtr MergeTreeIndexCuckooFilter::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorCuckooFilter>(false_positive_rate, f_bits, index.column_names);
}

MergeTreeIndexConditionPtr MergeTreeIndexCuckooFilter::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionCuckooFilter>(predicate, context, index.sample_block);
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
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type {} of cuckoo filter index.", type->getName());
    }
}

MergeTreeIndexPtr cuckooFilterIndexCreator(
    const IndexDescription & index)
{
    double false_positive_rate = 0.025;

    if (index.arguments && !index.arguments->children.empty())
    {
        auto argument = getFieldFromIndexArgumentAST(index.arguments->children[0]);
        false_positive_rate = argument.safeGet<Float64>();
    }

    /// Same open interval as `CuckooFilter::fingerprintBitsFromFalsePositiveRate`; clamp for attach / edge metadata.
    false_positive_rate = std::max(false_positive_rate, 1e-15);
    false_positive_rate = std::min(false_positive_rate, 1.0 - 1e-15);
    const size_t f_bits = CuckooFilter::fingerprintBitsFromFalsePositiveRate(false_positive_rate);

    return std::make_shared<MergeTreeIndexCuckooFilter>(index, false_positive_rate, f_bits);
}

void cuckooFilterIndexValidator(const IndexDescription & index, bool attach)
{
    assertIndexColumnsType(index.sample_block);

    if (index.arguments && index.arguments->children.size() > 1)
    {
        if (!attach) /// This is for backward compatibility.
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Cuckoo filter index cannot have more than one parameter.");
    }

    if (index.arguments && !index.arguments->children.empty())
    {
        auto argument = getFieldFromIndexArgumentAST(index.arguments->children[0]);

        if (!attach)
        {
            if (argument.getType() != Field::Types::Float64)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The cuckoo filter false positive rate must be a double strictly between 0 and 1 (exclusive).");

            const Float64 fpr = argument.safeGet<Float64>();
            if (!std::isfinite(fpr) || fpr <= 0 || fpr >= 1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The cuckoo filter false positive rate must be a double strictly between 0 and 1 (exclusive).");
        }
    }
}

}
