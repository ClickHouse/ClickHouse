#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/BloomFilterHash.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexUtils.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <base/types.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        read_size = filter->getFilter().size() * sizeof(BloomFilter::UnderType);
#endif
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
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        write_size = bloom_filter->getFilter().size() * sizeof(BloomFilter::UnderType);
#endif
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

ColumnWithTypeAndName getPreparedSetInfo(const ConstSetPtr & prepared_set)
{
    if (prepared_set->getDataTypes().size() == 1)
        return {prepared_set->getSetElements()[0], prepared_set->getElementsTypes()[0], "dummy"};

    Columns set_elements;
    for (auto & set_element : prepared_set->getSetElements())

        set_elements.emplace_back(set_element->convertToFullColumnIfConst());

    return {ColumnTuple::create(set_elements), std::make_shared<DataTypeTuple>(prepared_set->getElementsTypes()), "dummy"};
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
    else
    {
        return std::any_of(hashes.begin(),
                           hashes.end(),
                           [&](const auto& hash_row)
                           {
                               return hashMatchesFilter(bloom_filter,
                                                        hash_row,
                                                        hash_functions);
                           });
    }
}

}

MergeTreeIndexConditionBloomFilter::MergeTreeIndexConditionBloomFilter(
    const ActionsDAG * filter_actions_dag, ContextPtr context_, const Block & header_, size_t hash_functions_)
    : WithContext(context_), header(header_), hash_functions(hash_functions_)
{
    if (!filter_actions_dag)
    {
        rpn.push_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    RPNBuilder<RPNElement> builder(
        filter_actions_dag->getOutputs().at(0),
        context_,
        [&](const RPNBuilderTreeNode & node, RPNElement & out) { return extractAtomFromTree(node, out); });
    rpn = std::move(builder).extractRPN();
}

bool MergeTreeIndexConditionBloomFilter::alwaysUnknownOrTrue() const
{
    std::vector<bool> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN
            || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
            || element.function == RPNElement::FUNCTION_NOT_EQUALS
            || element.function == RPNElement::FUNCTION_HAS
            || element.function == RPNElement::FUNCTION_HAS_ANY
            || element.function == RPNElement::FUNCTION_HAS_ALL
            || element.function == RPNElement::FUNCTION_IN
            || element.function == RPNElement::FUNCTION_NOT_IN
            || element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            // do nothing
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 && arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 || arg2;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    return rpn_stack[0];
}

bool MergeTreeIndexConditionBloomFilter::mayBeTrueOnGranule(const MergeTreeIndexGranuleBloomFilter * granule) const
{
    std::vector<BoolMask> rpn_stack;
    const auto & filters = granule->getFilters();

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_IN
            || element.function == RPNElement::FUNCTION_NOT_IN
            || element.function == RPNElement::FUNCTION_EQUALS
            || element.function == RPNElement::FUNCTION_NOT_EQUALS
            || element.function == RPNElement::FUNCTION_HAS
            || element.function == RPNElement::FUNCTION_HAS_ANY
            || element.function == RPNElement::FUNCTION_HAS_ALL)
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
            if (element.function == RPNElement::FUNCTION_NOT_EQUALS || element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in KeyCondition::RPNElement");
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in KeyCondition::mayBeTrueInRange");

    return rpn_stack[0].can_be_true;
}

bool MergeTreeIndexConditionBloomFilter::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;

        if (node.tryGetConstant(const_value, const_type))
        {
            if (const_value.getType() == Field::Types::UInt64)
            {
                out.function = const_value.get<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Int64)
            {
                out.function = const_value.get<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }

            if (const_value.getType() == Field::Types::Float64)
            {
                out.function = const_value.get<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }
        }
    }

    return traverseFunction(node, out, nullptr /*parent*/);
}

bool MergeTreeIndexConditionBloomFilter::traverseFunction(const RPNBuilderTreeNode & node, RPNElement & out, const RPNBuilderTreeNode * parent)
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
        else if (lhs_argument.tryGetConstant(const_value, const_type) && (function_name == "equals" || function_name == "notEquals"))
        {
            if (traverseTreeEquals(function_name, rhs_argument, const_type, const_value, out, parent))
                return true;
        }

        return false;
    }

    return false;
}

bool MergeTreeIndexConditionBloomFilter::traverseTreeIn(
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
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types of arguments of function {}", function_name);

            bool match_with_subtype = false;
            const auto & sub_columns = tuple_column->getColumns();
            const auto & sub_data_types = tuple_data_type->getElements();

            for (size_t index = 0; index < key_node_function_arguments_size; ++index)
                match_with_subtype |= traverseTreeIn(function_name, key_node_function.getArgumentAt(index), nullptr, sub_data_types[index], sub_columns[index], out);

            return match_with_subtype;
        }

        if (key_node_function_name == "arrayElement")
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] IN ('') because if the key does not exist in the map
              * we return the default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where the map key does not exist.
              */
            if (!prepared_set)
                return false;

            auto default_column_to_check = type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();
            ColumnWithTypeAndName default_column_with_type_to_check { default_column_to_check, type, "" };
            ColumnsWithTypeAndName default_columns_with_type_to_check = {default_column_with_type_to_check};
            auto set_contains_default_value_predicate_column = prepared_set->execute(default_columns_with_type_to_check, false /*negative*/);
            const auto & set_contains_default_value_predicate_column_typed = assert_cast<const ColumnUInt8 &>(*set_contains_default_value_predicate_column);
            bool set_contain_default_value = set_contains_default_value_predicate_column_typed.getData()[0];
            if (set_contain_default_value)
                return false;

            auto first_argument = key_node_function.getArgumentAt(0);
            const auto column_name = first_argument.getColumnName();
            auto map_keys_index_column_name = fmt::format("mapKeys({})", column_name);
            auto map_values_index_column_name = fmt::format("mapValues({})", column_name);

            if (header.has(map_keys_index_column_name))
            {
                /// For mapKeys we serialize key argument with bloom filter

                auto second_argument = key_node_function.getArgumentAt(1);

                Field constant_value;
                DataTypePtr constant_type;

                if (second_argument.tryGetConstant(constant_value, constant_type))
                {
                    size_t position = header.getPositionByName(map_keys_index_column_name);
                    const DataTypePtr & index_type = header.getByPosition(position).type;
                    const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
                    out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), constant_value)));
                }
                else
                {
                    return false;
                }
            }
            else if (header.has(map_values_index_column_name))
            {
                /// For mapValues we serialize set with bloom filter

                size_t row_size = column->size();
                size_t position = header.getPositionByName(map_values_index_column_name);
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

            if (function_name == "in"  || function_name == "globalIn")
                out.function = RPNElement::FUNCTION_IN;

            if (function_name == "notIn"  || function_name == "globalNotIn")
                out.function = RPNElement::FUNCTION_NOT_IN;

            return true;
        }
    }

    return false;
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
    else if (function_name == "equals" /// notEquals is not applicable
        || function_name == "greater" || function_name == "greaterOrEquals"
        || function_name == "less" || function_name == "lessOrEquals")
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
        bool constant_equal_zero = applyVisitor(FieldVisitorAccurateEquals(), constant_value, zero);

        if (function_name == "equals" && !constant_equal_zero)
        {
            /// indexOf(...) = c, c != 0
            return true;
        }
        else if (function_name == "notEquals" && constant_equal_zero)
        {
            /// indexOf(...) != c, c = 0
            return true;
        }
        else if (function_name == (reversed ? "less" : "greater") && !applyVisitor(FieldVisitorAccurateLess(), constant_value, zero))
        {
            /// indexOf(...) > c, c >= 0
            return true;
        }
        else if (function_name == (reversed ? "lessOrEquals" : "greaterOrEquals") && applyVisitor(FieldVisitorAccurateLess(), zero, constant_value))
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
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be an array.", function_name);

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
        else if (function_name == "hasAny" || function_name == "hasAll")
        {
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be an array.", function_name);

            if (value_field.getType() != Field::Types::Array)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be an array.", function_name);

            const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());
            ColumnPtr column;
            {
                const bool is_nullable = actual_type->isNullable();
                auto mutable_column = actual_type->createColumn();

                for (const auto & f : value_field.get<Array>())
                {
                    if ((f.isNull() && !is_nullable) || f.isDecimal(f.getType())) /// NOLINT(readability-static-accessed-through-instance)
                        return false;

                    auto converted = convertFieldToType(f, *actual_type);
                    if (converted.isNull())
                        return false;

                    mutable_column->insert(converted);
                }

                column = std::move(mutable_column);
            }

            out.function = function_name == "hasAny" ?
                RPNElement::FUNCTION_HAS_ANY :
                RPNElement::FUNCTION_HAS_ALL;
            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithColumn(actual_type, column, 0, column->size())));
        }
        else
        {
            if (array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "An array type of bloom_filter supports only has(), indexOf(), and hasAny() functions.");

            out.function = function_name == "equals" ? RPNElement::FUNCTION_EQUALS : RPNElement::FUNCTION_NOT_EQUALS;
            const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
            auto converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
            if (converted_field.isNull())
                return false;

            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));
        }

        return true;
    }

    if (function_name == "mapContains" || function_name == "has")
    {
        auto map_keys_index_column_name = fmt::format("mapKeys({})", key_column_name);
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
            const Tuple & tuple = value_field.get<const Tuple &>();
            const auto * value_tuple_data_type = typeid_cast<const DataTypeTuple *>(value_type.get());

            if (tuple.size() != key_node_function_arguments_size)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types of arguments of function {}", function_name);

            bool match_with_subtype = false;
            const DataTypes & subtypes = value_tuple_data_type->getElements();

            for (size_t index = 0; index < tuple.size(); ++index)
                match_with_subtype |= traverseTreeEquals(function_name, key_node_function.getArgumentAt(index), subtypes[index], tuple[index], out, &key_node);

            return match_with_subtype;
        }

        if (key_node_function_name == "arrayElement" && (function_name == "equals" || function_name == "notEquals"))
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] = '' because if key does not exist in the map
              * we return default the value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where map key does not exist.
              */
            if (value_field == value_type->getDefault())
                return false;

            auto first_argument = key_node_function.getArgumentAt(0);
            const auto column_name = first_argument.getColumnName();

            auto map_keys_index_column_name = fmt::format("mapKeys({})", column_name);
            auto map_values_index_column_name = fmt::format("mapValues({})", column_name);

            size_t position = 0;
            Field const_value = value_field;
            DataTypePtr const_type;

            if (header.has(map_keys_index_column_name))
            {
                position = header.getPositionByName(map_keys_index_column_name);
                auto second_argument = key_node_function.getArgumentAt(1);

                if (!second_argument.tryGetConstant(const_value, const_type))
                    return false;
            }
            else if (header.has(map_values_index_column_name))
            {
                position = header.getPositionByName(map_values_index_column_name);
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

MergeTreeIndexAggregatorBloomFilter::MergeTreeIndexAggregatorBloomFilter(
    size_t bits_per_row_, size_t hash_functions_, const Names & columns_name_)
    : bits_per_row(bits_per_row_), hash_functions(hash_functions_), index_columns_name(columns_name_), column_hashes(columns_name_.size())
{
    assert(bits_per_row != 0);
    assert(hash_functions != 0);
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
        auto index_column = BloomFilterHash::hashWithColumn(column_and_type.type, column_and_type.column, *pos, max_read_rows);

        const auto & index_col = checkAndGetColumn<ColumnUInt64>(*index_column);
        const auto & index_data = index_col.getData();
        for (const auto & hash: index_data)
            column_hashes[column].insert(hash);
    }

    *pos += max_read_rows;
    total_rows += max_read_rows;
}

MergeTreeIndexBloomFilter::MergeTreeIndexBloomFilter(
    const IndexDescription & index_,
    size_t bits_per_row_,
    size_t hash_functions_)
    : IMergeTreeIndex(index_)
    , bits_per_row(bits_per_row_)
    , hash_functions(hash_functions_)
{
    assert(bits_per_row != 0);
    assert(hash_functions != 0);
}

MergeTreeIndexGranulePtr MergeTreeIndexBloomFilter::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, index.column_names.size());
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBloomFilter::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorBloomFilter>(bits_per_row, hash_functions, index.column_names);
}

MergeTreeIndexConditionPtr MergeTreeIndexBloomFilter::createIndexCondition(const ActionsDAG * filter_actions_dag, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionBloomFilter>(filter_actions_dag, context, index.sample_block, hash_functions);
}

static void assertIndexColumnsType(const Block & header)
{
    if (!header || !header.columns())
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
    const IndexDescription & index)
{
    double max_conflict_probability = 0.025;

    if (!index.arguments.empty())
    {
        const auto & argument = index.arguments[0];
        max_conflict_probability = std::min<Float64>(1.0, std::max<Float64>(argument.safeGet<Float64>(), 0.0));
    }

    const auto & bits_per_row_and_size_of_hash_functions = BloomFilterHash::calculationBestPractices(max_conflict_probability);

    return std::make_shared<MergeTreeIndexBloomFilter>(
        index, bits_per_row_and_size_of_hash_functions.first, bits_per_row_and_size_of_hash_functions.second);
}

void bloomFilterIndexValidator(const IndexDescription & index, bool attach)
{
    assertIndexColumnsType(index.sample_block);

    if (index.arguments.size() > 1)
    {
        if (!attach) /// This is for backward compatibility.
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "BloomFilter index cannot have more than one parameter.");
    }

    if (!index.arguments.empty())
    {
        const auto & argument = index.arguments[0];

        if (!attach && (argument.getType() != Field::Types::Float64 || argument.get<Float64>() < 0 || argument.get<Float64>() > 1))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The BloomFilter false positive must be a double number between 0 and 1.");
    }
}

}
