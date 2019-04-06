#include <optional>

#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Core/Row.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>

#include <Common/typeid_cast.h>

#include <DataStreams/IBlockInputStream.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/sortBlock.h>

#include <Storages/MergeTree/KeyCondition.h>

#include <ext/range.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int INCORRECT_ELEMENT_OF_SET;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


template <typename Method>
void NO_INLINE Set::insertFromBlockImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    ConstNullMapPtr null_map,
    ColumnUInt8::Container * out_filter)
{
    if (null_map)
    {
        if (out_filter)
            insertFromBlockImplCase<Method, true, true>(method, key_columns, rows, variants, null_map, out_filter);
        else
            insertFromBlockImplCase<Method, true, false>(method, key_columns, rows, variants, null_map, out_filter);
    }
    else
    {
        if (out_filter)
            insertFromBlockImplCase<Method, false, true>(method, key_columns, rows, variants, null_map, out_filter);
        else
            insertFromBlockImplCase<Method, false, false>(method, key_columns, rows, variants, null_map, out_filter);
    }
}


template <typename Method, bool has_null_map, bool build_filter>
void NO_INLINE Set::insertFromBlockImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    [[maybe_unused]] ConstNullMapPtr null_map,
    [[maybe_unused]] ColumnUInt8::Container * out_filter)
{
    typename Method::State state(key_columns, key_sizes, nullptr);

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_null_map)
            if ((*null_map)[i])
                continue;

        [[maybe_unused]] auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        if constexpr (build_filter)
            (*out_filter)[i] = emplace_result.isInserted();
    }
}


void Set::setHeader(const Block & block)
{
    std::unique_lock lock(rwlock);

    if (!empty())
        return;

    keys_size = block.columns();
    ColumnRawPtrs key_columns;
    key_columns.reserve(keys_size);
    data_types.reserve(keys_size);

    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        materialized_columns.emplace_back(block.safeGetByPosition(i).column->convertToFullColumnIfConst());
        key_columns.emplace_back(materialized_columns.back().get());
        data_types.emplace_back(block.safeGetByPosition(i).type);

        /// Convert low cardinality column to full.
        if (auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_types.back().get()))
        {
            data_types.back() = low_cardinality_type->getDictionaryType();
            materialized_columns.emplace_back(key_columns.back()->convertToFullColumnIfLowCardinality());
            key_columns.back() = materialized_columns.back().get();
        }
    }

    /// We will insert to the Set only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    if (fill_set_elements)
    {
        /// Create empty columns with set values in advance.
        /// It is needed because set may be empty, so method 'insertFromBlock' will be never called.
        set_elements.reserve(keys_size);
        for (const auto & type : data_types)
            set_elements.emplace_back(removeNullable(type)->createColumn());
    }

    /// Choose data structure to use for the set.
    data.init(data.chooseMethod(key_columns, key_sizes));
}


bool Set::insertFromBlock(const Block & block)
{
    std::unique_lock lock(rwlock);

    if (empty())
        throw Exception("Method Set::setHeader must be called before Set::insertFromBlock", ErrorCodes::LOGICAL_ERROR);

    ColumnRawPtrs key_columns;
    key_columns.reserve(keys_size);

    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        materialized_columns.emplace_back(block.safeGetByPosition(i).column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality());
        key_columns.emplace_back(materialized_columns.back().get());
    }

    size_t rows = block.rows();

    /// We will insert to the Set only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    /// Filter to extract distinct values from the block.
    ColumnUInt8::MutablePtr filter;
    if (fill_set_elements)
        filter = ColumnUInt8::create(block.rows());

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            insertFromBlockImpl(*data.NAME, key_columns, rows, data, null_map, filter ? &filter->getData() : nullptr); \
            break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    if (fill_set_elements)
    {
        for (size_t i = 0; i < keys_size; ++i)
        {
            auto filtered_column = block.getByPosition(i).column->filter(filter->getData(), rows);
            if (set_elements[i]->empty())
                set_elements[i] = filtered_column;
            else
                set_elements[i]->insertRangeFrom(*filtered_column, 0, filtered_column->size());
        }
    }

    return limits.check(getTotalRowCount(), getTotalByteCount(), "IN-set", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}


static Field extractValueFromNode(const ASTPtr & node, const IDataType & type, const Context & context)
{
    if (const auto * lit = node->as<ASTLiteral>())
    {
        return convertFieldToType(lit->value, type);
    }
    else if (node->as<ASTFunction>())
    {
        std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(node, context);
        return convertFieldToType(value_raw.first, type, value_raw.second.get());
    }
    else
        throw Exception("Incorrect element of set. Must be literal or constant expression.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
}


void Set::createFromAST(const DataTypes & types, ASTPtr node, const Context & context)
{
    /// Will form a block with values from the set.

    Block header;
    size_t num_columns = types.size();
    for (size_t i = 0; i < num_columns; ++i)
        header.insert(ColumnWithTypeAndName(types[i]->createColumn(), types[i], "_" + toString(i)));
    setHeader(header);

    MutableColumns columns = header.cloneEmptyColumns();

    DataTypePtr tuple_type;
    Row tuple_values;
    const auto & list = node->as<ASTExpressionList &>();
    for (auto & elem : list.children)
    {
        if (num_columns == 1)
        {
            Field value = extractValueFromNode(elem, *types[0], context);

            if (!value.isNull())
                columns[0]->insert(value);
        }
        else if (const auto * func = elem->as<ASTFunction>())
        {
            Field function_result;
            const TupleBackend * tuple = nullptr;
            if (func->name != "tuple")
            {
                if (!tuple_type)
                    tuple_type = std::make_shared<DataTypeTuple>(types);

                function_result = extractValueFromNode(elem, *tuple_type, context);
                if (function_result.getType() != Field::Types::Tuple)
                    throw Exception("Invalid type of set. Expected tuple, got " + String(function_result.getTypeName()),
                                    ErrorCodes::INCORRECT_ELEMENT_OF_SET);

                tuple = &function_result.get<Tuple>().toUnderType();
            }

            size_t tuple_size = tuple ? tuple->size() : func->arguments->children.size();
            if (tuple_size != num_columns)
                throw Exception("Incorrect size of tuple in set: " + toString(tuple_size) + " instead of " + toString(num_columns),
                    ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            size_t i = 0;
            for (; i < tuple_size; ++i)
            {
                Field value = tuple ? (*tuple)[i]
                                    : extractValueFromNode(func->arguments->children[i], *types[i], context);

                /// If at least one of the elements of the tuple has an impossible (outside the range of the type) value, then the entire tuple too.
                if (value.isNull())
                    break;

                tuple_values[i] = value;
            }

            if (i == tuple_size)
                for (i = 0; i < tuple_size; ++i)
                    columns[i]->insert(tuple_values[i]);
        }
        else
            throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
    }

    Block block = header.cloneWithColumns(std::move(columns));
    insertFromBlock(block);
}


ColumnPtr Set::execute(const Block & block, bool negative) const
{
    size_t num_key_columns = block.columns();

    if (0 == num_key_columns)
        throw Exception("Logical error: no columns passed to Set::execute method.", ErrorCodes::LOGICAL_ERROR);

    auto res = ColumnUInt8::create();
    ColumnUInt8::Container & vec_res = res->getData();
    vec_res.resize(block.safeGetByPosition(0).column->size());

    if (vec_res.empty())
        return res;

    std::shared_lock lock(rwlock);

    /// If the set is empty.
    if (data_types.empty())
    {
        if (negative)
            memset(vec_res.data(), 1, vec_res.size());
        else
            memset(vec_res.data(), 0, vec_res.size());
        return res;
    }

    if (data_types.size() != num_key_columns)
    {
        std::stringstream message;
        message << "Number of columns in section IN doesn't match. "
            << num_key_columns << " at left, " << data_types.size() << " at right.";
        throw Exception(message.str(), ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
    }

    /// Remember the columns we will work with. Also check that the data types are correct.
    ColumnRawPtrs key_columns;
    key_columns.reserve(num_key_columns);

    /// The constant columns to the left of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    for (size_t i = 0; i < num_key_columns; ++i)
    {
        if (!removeNullable(data_types[i])->equals(*removeNullable(block.safeGetByPosition(i).type)))
            throw Exception("Types of column " + toString(i + 1) + " in section IN don't match: "
                + data_types[i]->getName() + " on the right, " + block.safeGetByPosition(i).type->getName() +
                " on the left.", ErrorCodes::TYPE_MISMATCH);

        materialized_columns.emplace_back(block.safeGetByPosition(i).column->convertToFullColumnIfConst());
        key_columns.emplace_back() = materialized_columns.back().get();
    }

    /// We will check existence in Set only for keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    executeOrdinary(key_columns, vec_res, negative, null_map);

    return res;
}


template <typename Method>
void NO_INLINE Set::executeImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    if (null_map)
        executeImplCase<Method, true>(method, key_columns, vec_res, negative, rows, null_map);
    else
        executeImplCase<Method, false>(method, key_columns, vec_res, negative, rows, null_map);
}


template <typename Method, bool has_null_map>
void NO_INLINE Set::executeImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    Arena pool;
    typename Method::State state(key_columns, key_sizes, nullptr);

    /// NOTE Optimization is not used for consecutive identical strings.

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
            vec_res[i] = negative;
        else
        {
            auto find_result = state.findKey(method.data, i, pool);
            vec_res[i] = negative ^ find_result.isFound();
        }
    }
}


void Set::executeOrdinary(
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    ConstNullMapPtr null_map) const
{
    size_t rows = key_columns[0]->size();

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            executeImpl(*data.NAME, key_columns, vec_res, negative, rows, null_map); \
            break;
    APPLY_FOR_SET_VARIANTS(M)
#undef M
    }
}


MergeTreeSetIndex::MergeTreeSetIndex(const Columns & set_elements, std::vector<KeyTuplePositionMapping> && index_mapping_)
    : indexes_mapping(std::move(index_mapping_))
{
    std::sort(indexes_mapping.begin(), indexes_mapping.end(),
        [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
        {
            return std::forward_as_tuple(l.key_index, l.tuple_index) < std::forward_as_tuple(r.key_index, r.tuple_index);
        });

    indexes_mapping.erase(std::unique(
        indexes_mapping.begin(), indexes_mapping.end(),
        [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
        {
            return l.key_index == r.key_index;
        }), indexes_mapping.end());

    size_t tuple_size = indexes_mapping.size();
    ordered_set.resize(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        ordered_set[i] = set_elements[indexes_mapping[i].tuple_index];

    Block block_to_sort;
    SortDescription sort_description;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        block_to_sort.insert({ ordered_set[i], nullptr, "" });
        sort_description.emplace_back(i, 1, 1);
    }

    sortBlock(block_to_sort, sort_description);

    for (size_t i = 0; i < tuple_size; ++i)
        ordered_set[i] = block_to_sort.getByPosition(i).column;
}


/** Return the BoolMask where:
  * 1: the intersection of the set and the range is non-empty
  * 2: the range contains elements not in the set
  */
BoolMask MergeTreeSetIndex::mayBeTrueInRange(const std::vector<Range> & key_ranges, const DataTypes & data_types)
{
    size_t tuple_size = indexes_mapping.size();

    using FieldWithInfinityTuple = std::vector<FieldWithInfinity>;

    FieldWithInfinityTuple left_point;
    FieldWithInfinityTuple right_point;
    left_point.reserve(tuple_size);
    right_point.reserve(tuple_size);

    bool invert_left_infinities = false;
    bool invert_right_infinities = false;

    for (size_t i = 0; i < tuple_size; ++i)
    {
        std::optional<Range> new_range = KeyCondition::applyMonotonicFunctionsChainToRange(
            key_ranges[indexes_mapping[i].key_index],
            indexes_mapping[i].functions,
            data_types[indexes_mapping[i].key_index]);

        if (!new_range)
            return {true, true};

        /** A range that ends in (x, y, ..., +inf) exclusive is the same as a range
          * that ends in (x, y, ..., -inf) inclusive and vice versa for the left bound.
          */
        if (new_range->left_bounded)
        {
            if (!new_range->left_included)
                invert_left_infinities = true;

            left_point.push_back(FieldWithInfinity(new_range->left));
        }
        else
        {
            if (invert_left_infinities)
                left_point.push_back(FieldWithInfinity::getPlusinfinity());
            else
                left_point.push_back(FieldWithInfinity::getMinusInfinity());
        }

        if (new_range->right_bounded)
        {
            if (!new_range->right_included)
                invert_right_infinities = true;

            right_point.push_back(FieldWithInfinity(new_range->right));
        }
        else
        {
            if (invert_right_infinities)
                right_point.push_back(FieldWithInfinity::getMinusInfinity());
            else
                right_point.push_back(FieldWithInfinity::getPlusinfinity());
        }
    }

    /// This allows to construct tuple in 'ordered_set' at specified index for comparison with range.

    auto indices = ext::range(0, ordered_set.at(0)->size());

    auto extract_tuple = [tuple_size, this](size_t i)
    {
        /// Inefficient.
        FieldWithInfinityTuple res;
        res.reserve(tuple_size);
        for (size_t j = 0; j < tuple_size; ++j)
            res.emplace_back((*ordered_set[j])[i]);
        return res;
    };

    auto compare = [&extract_tuple](size_t i, const FieldWithInfinityTuple & rhs)
    {
        return extract_tuple(i) < rhs;
    };

    /** Because each parallelogram maps to a contiguous sequence of elements
      * layed out in the lexicographically increasing order, the set intersects the range
      * if and only if either bound coincides with an element or at least one element
      * is between the lower bounds
      */
    auto left_lower = std::lower_bound(indices.begin(), indices.end(), left_point, compare);
    auto right_lower = std::lower_bound(indices.begin(), indices.end(), right_point, compare);

    return
    {
        left_lower != right_lower
            || (left_lower != indices.end() && extract_tuple(*left_lower) == left_point)
            || (right_lower != indices.end() && extract_tuple(*right_lower) == right_point),
        true
    };
}

}
