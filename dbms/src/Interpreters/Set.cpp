#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Core/Row.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>

#include <Common/typeid_cast.h>

#include <DataStreams/IProfilingBlockInputStream.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/NullableUtils.h>

#include <Storages/MergeTree/KeyCondition.h>


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
    ConstNullMapPtr null_map)
{
    if (null_map)
        insertFromBlockImplCase<Method, true>(method, key_columns, rows, variants, null_map);
    else
        insertFromBlockImplCase<Method, false>(method, key_columns, rows, variants, null_map);
}


template <typename Method, bool has_null_map>
void NO_INLINE Set::insertFromBlockImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    ConstNullMapPtr null_map)
{
    typename Method::State state;
    state.init(key_columns);

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
            continue;

        /// Obtain a key to insert to the set
        typename Method::Key key = state.getKey(key_columns, keys_size, i, key_sizes);

        typename Method::Data::iterator it;
        bool inserted;
        method.data.emplace(key, it, inserted);

        if (inserted)
            method.onNewKey(*it, keys_size, variants.string_pool);
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
        key_columns.emplace_back(block.safeGetByPosition(i).column.get());
        data_types.emplace_back(block.safeGetByPosition(i).type);

        if (ColumnPtr converted = key_columns.back()->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns.back() = materialized_columns.back().get();
        }
    }

    /// We will insert to the Set only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    /// Choose data structure to use for the set.
    data.init(data.chooseMethod(key_columns, key_sizes));
}


bool Set::insertFromBlock(const Block & block, bool fill_set_elements)
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
        key_columns.emplace_back(block.safeGetByPosition(i).column.get());

        if (ColumnPtr converted = key_columns.back()->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns.back() = materialized_columns.back().get();
        }
    }

    size_t rows = block.rows();

    /// We will insert to the Set only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            insertFromBlockImpl(*data.NAME, key_columns, rows, data, null_map); \
            break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    if (fill_set_elements)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            std::vector<Field> new_set_elements;
            for (size_t j = 0; j < keys_size; ++j)
                new_set_elements.push_back((*key_columns[j])[i]);

            set_elements->emplace_back(std::move(new_set_elements));
        }
    }

    return limits.check(getTotalRowCount(), getTotalByteCount(), "IN-set", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}


static Field extractValueFromNode(ASTPtr & node, const IDataType & type, const Context & context)
{
    if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(node.get()))
    {
        return convertFieldToType(lit->value, type);
    }
    else if (typeid_cast<ASTFunction *>(node.get()))
    {
        std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(node, context);
        return convertFieldToType(value_raw.first, type, value_raw.second.get());
    }
    else
        throw Exception("Incorrect element of set. Must be literal or constant expression.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
}


void Set::createFromAST(const DataTypes & types, ASTPtr node, const Context & context, bool fill_set_elements)
{
    /// Will form a block with values from the set.

    Block header;
    size_t num_columns = types.size();
    for (size_t i = 0; i < num_columns; ++i)
        header.insert(ColumnWithTypeAndName(types[i]->createColumn(), types[i], "_" + toString(i)));
    setHeader(header);

    MutableColumns columns = header.cloneEmptyColumns();

    Row tuple_values;
    ASTExpressionList & list = typeid_cast<ASTExpressionList &>(*node);
    for (auto & elem : list.children)
    {
        if (num_columns == 1)
        {
            Field value = extractValueFromNode(elem, *types[0], context);

            if (!value.isNull())
                columns[0]->insert(value);
        }
        else if (ASTFunction * func = typeid_cast<ASTFunction *>(elem.get()))
        {
            if (func->name != "tuple")
                throw Exception("Incorrect element of set. Must be tuple.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            size_t tuple_size = func->arguments->children.size();
            if (tuple_size != num_columns)
                throw Exception("Incorrect size of tuple in set: " + toString(tuple_size) + " instead of " + toString(num_columns),
                    ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            size_t i = 0;
            for (; i < tuple_size; ++i)
            {
                Field value = extractValueFromNode(func->arguments->children[i], *types[i], context);

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
    insertFromBlock(block, fill_set_elements);
}


ColumnPtr Set::execute(const Block & block, bool negative) const
{
    size_t num_key_columns = block.columns();

    if (0 == num_key_columns)
        throw Exception("Logical error: no columns passed to Set::execute method.", ErrorCodes::LOGICAL_ERROR);

    auto res = ColumnUInt8::create();
    ColumnUInt8::Container & vec_res = res->getData();
    vec_res.resize(block.safeGetByPosition(0).column->size());

    std::shared_lock lock(rwlock);

    /// If the set is empty.
    if (data_types.empty())
    {
        if (negative)
            memset(&vec_res[0], 1, vec_res.size());
        else
            memset(&vec_res[0], 0, vec_res.size());
        return std::move(res);
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
        key_columns.push_back(block.safeGetByPosition(i).column.get());

        if (!removeNullable(data_types[i])->equals(*removeNullable(block.safeGetByPosition(i).type)))
            throw Exception("Types of column " + toString(i + 1) + " in section IN don't match: "
                + data_types[i]->getName() + " on the right, " + block.safeGetByPosition(i).type->getName() +
                " on the left.", ErrorCodes::TYPE_MISMATCH);

        if (ColumnPtr converted = key_columns.back()->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns.back() = materialized_columns.back().get();
        }
    }

    /// We will check existence in Set only for keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    executeOrdinary(key_columns, vec_res, negative, null_map);

    return std::move(res);
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
    typename Method::State state;
    state.init(key_columns);

    /// NOTE Optimization is not used for consecutive identical values.

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
            vec_res[i] = negative;
        else
        {
            /// Build the key
            typename Method::Key key = state.getKey(key_columns, keys_size, i, key_sizes);
            vec_res[i] = negative ^ method.data.has(key);
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


MergeTreeSetIndex::MergeTreeSetIndex(const SetElements & set_elements, std::vector<KeyTuplePositionMapping> && index_mapping_)
    : ordered_set(),
    indexes_mapping(std::move(index_mapping_))
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

    for (size_t i = 0; i < set_elements.size(); ++i)
    {
        std::vector<FieldWithInfinity> new_set_values;
        for (size_t j = 0; j < indexes_mapping.size(); ++j)
            new_set_values.emplace_back(set_elements[i][indexes_mapping[j].tuple_index]);

        ordered_set.emplace_back(std::move(new_set_values));
    }

    std::sort(ordered_set.begin(), ordered_set.end());
}


/** Return the BoolMask where:
  * 1: the intersection of the set and the range is non-empty
  * 2: the range contains elements not in the set
  */
BoolMask MergeTreeSetIndex::mayBeTrueInRange(const std::vector<Range> & key_ranges, const DataTypes & data_types)
{
    std::vector<FieldWithInfinity> left_point;
    std::vector<FieldWithInfinity> right_point;
    left_point.reserve(indexes_mapping.size());
    right_point.reserve(indexes_mapping.size());

    bool invert_left_infinities = false;
    bool invert_right_infinities = false;

    for (size_t i = 0; i < indexes_mapping.size(); ++i)
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

    /** Because each parallelogram maps to a contiguous sequence of elements
      * layed out in the lexicographically increasing order, the set intersects the range
      * if and only if either bound coincides with an element or at least one element
      * is between the lower bounds
      */
    auto left_lower = std::lower_bound(ordered_set.begin(), ordered_set.end(), left_point);
    auto right_lower = std::lower_bound(ordered_set.begin(), ordered_set.end(), right_point);
    return {left_lower != right_lower
        || (left_lower != ordered_set.end() && *left_lower == left_point)
        || (right_lower != ordered_set.end() && *right_lower == right_point), true};
}

}
