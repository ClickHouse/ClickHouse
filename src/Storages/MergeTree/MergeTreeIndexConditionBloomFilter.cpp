#include <Common/HashTable/ClearableHashMap.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/MergeTreeIndexGranuleBloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndexConditionBloomFilter.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/misc.h>
#include <Interpreters/BloomFilterHash.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

namespace
{

PreparedSetKey getPreparedSetKey(const ASTPtr & node, const DataTypePtr & data_type)
{
    /// If the data type is tuple, let's try unbox once
    if (node->as<ASTSubquery>() || node->as<ASTIdentifier>())
        return PreparedSetKey::forSubquery(*node);

    if (const auto * date_type_tuple = typeid_cast<const DataTypeTuple *>(&*data_type))
        return PreparedSetKey::forLiteral(*node, date_type_tuple->getElements());

    return PreparedSetKey::forLiteral(*node, DataTypes(1, data_type));
}

ColumnWithTypeAndName getPreparedSetInfo(const SetPtr & prepared_set)
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
        throw Exception("LOGICAL ERROR: hash column must be Const Column or UInt64 Column.", ErrorCodes::LOGICAL_ERROR);

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
    const SelectQueryInfo & info_, ContextPtr context_, const Block & header_, size_t hash_functions_)
    : WithContext(context_), header(header_), query_info(info_), hash_functions(hash_functions_)
{
    auto atom_from_ast = [this](auto & node, auto, auto & constants, auto & out) { return traverseAtomAST(node, constants, out); };
    rpn = std::move(RPNBuilder<RPNElement>(info_, getContext(), atom_from_ast).extractRPN());
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
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
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
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in KeyCondition::mayBeTrueInRange", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0].can_be_true;
}

bool MergeTreeIndexConditionBloomFilter::traverseAtomAST(const ASTPtr & node, Block & block_with_constants, RPNElement & out)
{
    {
        Field const_value;
        DataTypePtr const_type;
        if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
        {
            if (const_value.getType() == Field::Types::UInt64 || const_value.getType() == Field::Types::Int64 ||
                const_value.getType() == Field::Types::Float64)
            {
                /// Zero in all types is represented in memory the same way as in UInt64.
                out.function = const_value.reinterpret<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
                return true;
            }
        }
    }

    return traverseFunction(node, block_with_constants, out, nullptr);
}

bool MergeTreeIndexConditionBloomFilter::traverseFunction(const ASTPtr & node, Block & block_with_constants, RPNElement & out, const ASTPtr & parent)
{
    bool maybe_useful = false;

    if (const auto * function = node->as<ASTFunction>())
    {
        if (!function->arguments)
            return false;

        const ASTs & arguments = function->arguments->children;
        for (const auto & arg : arguments)
        {
            if (traverseFunction(arg, block_with_constants, out, node))
                maybe_useful = true;
        }

        if (arguments.size() != 2)
            return false;

        if (functionIsInOrGlobalInOperator(function->name))
        {
            auto prepared_set = getPreparedSet(arguments[1]);

            if (prepared_set)
            {
                if (traverseASTIn(function->name, arguments[0], prepared_set, out))
                    maybe_useful = true;
            }
        }
        else if (function->name == "equals" ||
                 function->name == "notEquals" ||
                 function->name == "has" ||
                 function->name == "mapContains" ||
                 function->name == "indexOf" ||
                 function->name == "hasAny" ||
                 function->name == "hasAll")
        {
            Field const_value;
            DataTypePtr const_type;
            if (KeyCondition::getConstant(arguments[1], block_with_constants, const_value, const_type))
            {
                if (traverseASTEquals(function->name, arguments[0], const_type, const_value, out, parent))
                    maybe_useful = true;
            }
            else if (KeyCondition::getConstant(arguments[0], block_with_constants, const_value, const_type))
            {
                if (traverseASTEquals(function->name, arguments[1], const_type, const_value, out, parent))
                    maybe_useful = true;
            }
        }
    }

    return maybe_useful;
}

bool MergeTreeIndexConditionBloomFilter::traverseASTIn(
    const String & function_name,
    const ASTPtr & key_ast,
    const SetPtr & prepared_set,
    RPNElement & out)
{
    const auto prepared_info = getPreparedSetInfo(prepared_set);
    return traverseASTIn(function_name, key_ast, prepared_set, prepared_info.type, prepared_info.column, out);
}

bool MergeTreeIndexConditionBloomFilter::traverseASTIn(
    const String & function_name,
    const ASTPtr & key_ast,
    const SetPtr & prepared_set,
    const DataTypePtr & type,
    const ColumnPtr & column,
    RPNElement & out)
{
    if (header.has(key_ast->getColumnName()))
    {
        size_t row_size = column->size();
        size_t position = header.getPositionByName(key_ast->getColumnName());
        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto & converted_column = castColumn(ColumnWithTypeAndName{column, type, ""}, index_type);
        out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithColumn(index_type, converted_column, 0, row_size)));

        if (function_name == "in"  || function_name == "globalIn")
            out.function = RPNElement::FUNCTION_IN;

        if (function_name == "notIn"  || function_name == "globalNotIn")
            out.function = RPNElement::FUNCTION_NOT_IN;

        return true;
    }

    if (const auto * function = key_ast->as<ASTFunction>())
    {
        WhichDataType which(type);

        if (which.isTuple() && function->name == "tuple")
        {
            const auto & tuple_column = typeid_cast<const ColumnTuple *>(column.get());
            const auto & tuple_data_type = typeid_cast<const DataTypeTuple *>(type.get());
            const ASTs & arguments = typeid_cast<const ASTExpressionList &>(*function->arguments).children;

            if (tuple_data_type->getElements().size() != arguments.size() || tuple_column->getColumns().size() != arguments.size())
                throw Exception("Illegal types of arguments of function " + function_name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            bool match_with_subtype = false;
            const auto & sub_columns = tuple_column->getColumns();
            const auto & sub_data_types = tuple_data_type->getElements();

            for (size_t index = 0; index < arguments.size(); ++index)
                match_with_subtype |= traverseASTIn(function_name, arguments[index], nullptr, sub_data_types[index], sub_columns[index], out);

            return match_with_subtype;
        }

        if (function->name == "arrayElement")
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] IN ('') because if key does not exists in map
              * we return default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where map key does not exists.
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

            const auto * column_ast_identifier = function->arguments.get()->children[0].get()->as<ASTIdentifier>();
            if (!column_ast_identifier)
                return false;

            const auto & col_name = column_ast_identifier->name();
            auto map_keys_index_column_name = fmt::format("mapKeys({})", col_name);
            auto map_values_index_column_name = fmt::format("mapValues({})", col_name);

            if (header.has(map_keys_index_column_name))
            {
                /// For mapKeys we serialize key argument with bloom filter

                auto & argument = function->arguments.get()->children[1];

                if (const auto * literal = argument->as<ASTLiteral>())
                {
                    size_t position = header.getPositionByName(map_keys_index_column_name);
                    const DataTypePtr & index_type = header.getByPosition(position).type;

                    auto element_key = literal->value;
                    const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
                    out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), element_key)));
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


static bool indexOfCanUseBloomFilter(const ASTPtr & parent)
{
    if (!parent)
        return true;

    /// `parent` is a function where `indexOf` is located.
    /// Example: `indexOf(arr, x) = 1`, parent is a function named `equals`.
    if (const auto * function = parent->as<ASTFunction>())
    {
        if (function->name == "and")
        {
            return true;
        }
        else if (function->name == "equals" /// notEquals is not applicable
            || function->name == "greater" || function->name == "greaterOrEquals"
            || function->name == "less" || function->name == "lessOrEquals")
        {
            if (function->arguments->children.size() != 2)
                return false;

            /// We don't allow constant expressions like `indexOf(arr, x) = 1 + 0` but it's negligible.

            /// We should return true when the corresponding expression implies that the array contains the element.
            /// Example: when `indexOf(arr, x)` > 10 is written, it means that arr definitely should contain the element
            /// (at least at 11th position but it does not matter).

            bool reversed = false;
            const ASTLiteral * constant = nullptr;

            if (const ASTLiteral * left = function->arguments->children[0]->as<ASTLiteral>())
            {
                constant = left;
                reversed = true;
            }
            else if (const ASTLiteral * right = function->arguments->children[1]->as<ASTLiteral>())
            {
                constant = right;
            }
            else
                return false;

            Field zero(0);
            return (function->name == "equals"  /// indexOf(...) = c, c != 0
                    && !applyVisitor(FieldVisitorAccurateEquals(), constant->value, zero))
                || (function->name == "notEquals"  /// indexOf(...) != c, c = 0
                    && applyVisitor(FieldVisitorAccurateEquals(), constant->value, zero))
                || (function->name == (reversed ? "less" : "greater")   /// indexOf(...) > c, c >= 0
                    && !applyVisitor(FieldVisitorAccurateLess(), constant->value, zero))
                || (function->name == (reversed ? "lessOrEquals" : "greaterOrEquals")   /// indexOf(...) >= c, c > 0
                    && applyVisitor(FieldVisitorAccurateLess(), zero, constant->value));
        }
    }

    return false;
}


bool MergeTreeIndexConditionBloomFilter::traverseASTEquals(
    const String & function_name,
    const ASTPtr & key_ast,
    const DataTypePtr & value_type,
    const Field & value_field,
    RPNElement & out,
    const ASTPtr & parent)
{
    if (header.has(key_ast->getColumnName()))
    {
        size_t position = header.getPositionByName(key_ast->getColumnName());
        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(index_type.get());

        if (function_name == "has" || function_name == "indexOf")
        {
            if (!array_type)
                throw Exception("First argument for function " + function_name + " must be an array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            /// We can treat `indexOf` function similar to `has`.
            /// But it is little more cumbersome, compare: `has(arr, elem)` and `indexOf(arr, elem) != 0`.
            /// The `parent` in this context is expected to be function `!=` (`notEquals`).
            if (function_name == "has" || indexOfCanUseBloomFilter(parent))
            {
                out.function = RPNElement::FUNCTION_HAS;
                const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());
                Field converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
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
                    if ((f.isNull() && !is_nullable) || f.isDecimal(f.getType()))
                        return false;

                    mutable_column->insert(convertFieldToType(f, *actual_type, value_type.get()));
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
                throw Exception("An array type of bloom_filter supports only has(), indexOf(), and hasAny() functions.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            out.function = function_name == "equals" ? RPNElement::FUNCTION_EQUALS : RPNElement::FUNCTION_NOT_EQUALS;
            const DataTypePtr actual_type = BloomFilter::getPrimitiveType(index_type);
            Field converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
            out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));
        }

        return true;
    }

    if (function_name == "mapContains" || function_name == "has")
    {
        const auto * key_ast_identifier = key_ast.get()->as<const ASTIdentifier>();
        if (!key_ast_identifier)
            return false;

        const auto & col_name = key_ast_identifier->name();
        auto map_keys_index_column_name = fmt::format("mapKeys({})", col_name);

        if (!header.has(map_keys_index_column_name))
            return false;

        size_t position = header.getPositionByName(map_keys_index_column_name);
        const DataTypePtr & index_type = header.getByPosition(position).type;
        const auto * array_type = typeid_cast<const DataTypeArray *>(index_type.get());

        if (!array_type)
            return false;

        out.function = RPNElement::FUNCTION_HAS;
        const DataTypePtr actual_type = BloomFilter::getPrimitiveType(array_type->getNestedType());
        Field converted_field = convertFieldToType(value_field, *actual_type, value_type.get());
        out.predicate.emplace_back(std::make_pair(position, BloomFilterHash::hashWithField(actual_type.get(), converted_field)));

        return true;
    }

    if (const auto * function = key_ast->as<ASTFunction>())
    {
        WhichDataType which(value_type);

        if (which.isTuple() && function->name == "tuple")
        {
            const Tuple & tuple = get<const Tuple &>(value_field);
            const auto * value_tuple_data_type = typeid_cast<const DataTypeTuple *>(value_type.get());
            const ASTs & arguments = typeid_cast<const ASTExpressionList &>(*function->arguments).children;

            if (tuple.size() != arguments.size())
                throw Exception("Illegal types of arguments of function " + function_name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            bool match_with_subtype = false;
            const DataTypes & subtypes = value_tuple_data_type->getElements();

            for (size_t index = 0; index < tuple.size(); ++index)
                match_with_subtype |= traverseASTEquals(function_name, arguments[index], subtypes[index], tuple[index], out, key_ast);

            return match_with_subtype;
        }

        if (function->name == "arrayElement" && (function_name == "equals" || function_name == "notEquals"))
        {
            /** Try to parse arrayElement for mapKeys index.
              * It is important to ignore keys like column_map['Key'] = '' because if key does not exists in map
              * we return default value for arrayElement.
              *
              * We cannot skip keys that does not exist in map if comparison is with default type value because
              * that way we skip necessary granules where map key does not exists.
              */
            if (value_field == value_type->getDefault())
                return false;

            const auto * column_ast_identifier = function->arguments.get()->children[0].get()->as<ASTIdentifier>();
            if (!column_ast_identifier)
                return false;

            const auto & col_name = column_ast_identifier->name();

            auto map_keys_index_column_name = fmt::format("mapKeys({})", col_name);
            auto map_values_index_column_name = fmt::format("mapValues({})", col_name);

            size_t position = 0;
            Field const_value = value_field;

            if (header.has(map_keys_index_column_name))
            {
                position = header.getPositionByName(map_keys_index_column_name);

                auto & argument = function->arguments.get()->children[1];

                if (const auto * literal = argument->as<ASTLiteral>())
                    const_value = literal->value;
                else
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

SetPtr MergeTreeIndexConditionBloomFilter::getPreparedSet(const ASTPtr & node)
{
    if (header.has(node->getColumnName()))
    {
        const auto & column_and_type = header.getByName(node->getColumnName());
        auto set_key = getPreparedSetKey(node, column_and_type.type);
        if (auto prepared_set = query_info.prepared_sets->getSet(set_key))
            return prepared_set;
    }
    else
    {
        for (const auto & set : query_info.prepared_sets->getByTreeHash(node->getTreeHash()))
            if (set->hasExplicitSetElements())
                return set;
    }

    return DB::SetPtr();
}

}
