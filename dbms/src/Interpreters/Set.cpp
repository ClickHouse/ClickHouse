#include <Core/Field.h>
#include <Core/FieldVisitors.h>
#include <Core/Row.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>

#include <Common/typeid_cast.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTraits.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/NullableUtils.h>

#include <Storages/MergeTree/PKCondition.h>


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

bool Set::checkSetSizeLimits() const
{
    if (max_rows && data.getTotalRowCount() > max_rows)
        return false;
    if (max_bytes && data.getTotalByteCount() > max_bytes)
        return false;
    return true;
}


template <typename Method>
void NO_INLINE Set::insertFromBlockImpl(
    Method & method,
    const ConstColumnPlainPtrs & key_columns,
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
    const ConstColumnPlainPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    ConstNullMapPtr null_map)
{
    typename Method::State state;
    state.init(key_columns);
    size_t keys_size = key_columns.size();

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
            method.onNewKey(*it, keys_size, i, variants.string_pool);
    }
}


bool Set::insertFromBlock(const Block & block, bool create_ordered_set)
{
    std::unique_lock<std::shared_mutex> lock(rwlock);

    size_t keys_size = block.columns();
    ConstColumnPlainPtrs key_columns;
    key_columns.reserve(keys_size);

    if (empty())
    {
        data_types.clear();
        data_types.reserve(keys_size);
    }

    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns.emplace_back(block.safeGetByPosition(i).column.get());

        if (empty())
            data_types.emplace_back(block.safeGetByPosition(i).type);

        if (auto converted = key_columns.back()->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns.back() = materialized_columns.back().get();
        }
    }

    /** Flatten tuples. For case when written
      *  (a, b) IN (SELECT (a, b) FROM table)
      * instead of more typical
      *  (a, b) IN (SELECT a, b FROM table)
      *
      * Avoid flatten in case then we have more than one column:
      * Ex.: 1, (2, 3) become just 1, 2, 3
      */
    if (keys_size == 1)
    {
        if (const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(key_columns.back()))
        {
            key_columns.pop_back();
            const Columns & tuple_elements = tuple->getColumns();
            for (const auto & elem : tuple_elements)
                key_columns.push_back(elem.get());

            if (empty())
            {
                data_types.pop_back();
                const Block & tuple_block = tuple->getData();
                for (size_t i = 0, size = tuple_block.columns(); i < size; ++i)
                    data_types.push_back(tuple_block.getByPosition(i).type);
            }
        }
    }

    size_t rows = block.rows();

    /// We will insert to the Set only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    /// Choose data structure to use for the set.
    if (empty())
        data.init(data.chooseMethod(key_columns, key_sizes));

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

    if (create_ordered_set)
        for (size_t i = 0; i < rows; ++i)
            ordered_set_elements->push_back((*key_columns[0])[i]); /// ordered_set for index works only for single key, not for tuples

    if (!checkSetSizeLimits())
    {
        if (overflow_mode == OverflowMode::THROW)
            throw Exception("IN-set size exceeded."
                " Rows: " + toString(data.getTotalRowCount()) +
                ", limit: " + toString(max_rows) +
                ". Bytes: " + toString(data.getTotalByteCount()) +
                ", limit: " + toString(max_bytes) + ".",
                ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

        if (overflow_mode == OverflowMode::BREAK)
            return false;

        throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
    }

    return true;
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


void Set::createFromAST(const DataTypes & types, ASTPtr node, const Context & context, bool create_ordered_set)
{
    data_types = types;

    /// Will form a block with values from the set.
    Block block;
    for (size_t i = 0, size = data_types.size(); i < size; ++i)
    {
        ColumnWithTypeAndName col;
        col.type = data_types[i];
        col.column = data_types[i]->createColumn();
        col.name = "_" + toString(i);

        block.insert(std::move(col));
    }

    Row tuple_values;
    ASTExpressionList & list = typeid_cast<ASTExpressionList &>(*node);
    for (ASTs::iterator it = list.children.begin(); it != list.children.end(); ++it)
    {
        if (data_types.size() == 1)
        {
            Field value = extractValueFromNode(*it, *data_types[0], context);

            if (!value.isNull())
                block.safeGetByPosition(0).column->insert(value);
        }
        else if (ASTFunction * func = typeid_cast<ASTFunction *>(it->get()))
        {
            if (func->name != "tuple")
                throw Exception("Incorrect element of set. Must be tuple.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            size_t tuple_size = func->arguments->children.size();
            if (tuple_size != data_types.size())
                throw Exception("Incorrect size of tuple in set.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            size_t j = 0;
            for (; j < tuple_size; ++j)
            {
                Field value = extractValueFromNode(func->arguments->children[j], *data_types[j], context);

                /// If at least one of the elements of the tuple has an impossible (outside the range of the type) value, then the entire tuple too.
                if (value.isNull())
                    break;

                tuple_values[j] = value;
            }

            if (j == tuple_size)
                for (j = 0; j < tuple_size; ++j)
                    block.safeGetByPosition(j).column->insert(tuple_values[j]);
        }
        else
            throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
    }

    if (create_ordered_set)
        ordered_set_elements = OrderedSetElementsPtr(new OrderedSetElements());

    insertFromBlock(block, create_ordered_set);

    if (create_ordered_set)
    {
        std::sort(ordered_set_elements->begin(), ordered_set_elements->end());
        ordered_set_elements->erase(std::unique(ordered_set_elements->begin(), ordered_set_elements->end()), ordered_set_elements->end());
    }
}


ColumnPtr Set::execute(const Block & block, bool negative) const
{
    size_t num_key_columns = block.columns();

    if (0 == num_key_columns)
        throw Exception("Logical error: no columns passed to Set::execute method.", ErrorCodes::LOGICAL_ERROR);

    auto res = std::make_shared<ColumnUInt8>();
    ColumnUInt8::Container_t & vec_res = res->getData();
    vec_res.resize(block.safeGetByPosition(0).column->size());

    std::shared_lock<std::shared_mutex> lock(rwlock);

    /// If the set is empty.
    if (data_types.empty())
    {
        if (negative)
            memset(&vec_res[0], 1, vec_res.size());
        else
            memset(&vec_res[0], 0, vec_res.size());
        return res;
    }

    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(block.safeGetByPosition(0).type.get());

    if (array_type)
    {
        /// Special treatment of Arrays in left hand side of IN:
        ///  check that at least one array element is in Set.
        /// This is deprecated functionality and will be removed.

        if (data_types.size() != 1 || num_key_columns != 1)
            throw Exception("Number of columns in section IN doesn't match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

        if (array_type->getNestedType()->isNullable())
            throw Exception("Array(Nullable(...)) for left hand side of IN is not supported.", ErrorCodes::NOT_IMPLEMENTED);

        if (!array_type->getNestedType()->equals(*data_types[0]))
            throw Exception(std::string() + "Types in section IN don't match: " + data_types[0]->getName() +
                " on the right, " + array_type->getNestedType()->getName() + " on the left.",
                ErrorCodes::TYPE_MISMATCH);

        const IColumn * in_column = block.safeGetByPosition(0).column.get();

        /// The constant column to the left of IN is not supported directly. For this, it first materializes.
        ColumnPtr materialized_column = in_column->convertToFullColumnIfConst();
        if (materialized_column)
            in_column = materialized_column.get();

        if (const ColumnArray * col = typeid_cast<const ColumnArray *>(in_column))
            executeArray(col, vec_res, negative);
        else
            throw Exception("Unexpected array column type: " + in_column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }
    else
    {
        if (data_types.size() != num_key_columns)
        {
            std::stringstream message;
            message << "Number of columns in section IN doesn't match. "
                << num_key_columns << " at left, " << data_types.size() << " at right.";
            throw Exception(message.str(), ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
        }

        /// Remember the columns we will work with. Also check that the data types are correct.
        ConstColumnPlainPtrs key_columns;
        key_columns.reserve(num_key_columns);

        /// The constant columns to the left of IN are not supported directly. For this, they first materialize.
        Columns materialized_columns;

        for (size_t i = 0; i < num_key_columns; ++i)
        {
            key_columns.push_back(block.safeGetByPosition(i).column.get());

            if (DataTypeTraits::removeNullable(data_types[i])->getName() !=
                DataTypeTraits::removeNullable(block.safeGetByPosition(i).type)->getName())
                throw Exception("Types of column " + toString(i + 1) + " in section IN don't match: "
                    + data_types[i]->getName() + " on the right, " + block.safeGetByPosition(i).type->getName() +
                    " on the left.", ErrorCodes::TYPE_MISMATCH);

            if (auto converted = key_columns.back()->convertToFullColumnIfConst())
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
    }

    return res;
}


template <typename Method>
void NO_INLINE Set::executeImpl(
    Method & method,
    const ConstColumnPlainPtrs & key_columns,
    ColumnUInt8::Container_t & vec_res,
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
    const ConstColumnPlainPtrs & key_columns,
    ColumnUInt8::Container_t & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    typename Method::State state;
    state.init(key_columns);
    size_t keys_size = key_columns.size();

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

template <typename Method>
void NO_INLINE Set::executeArrayImpl(
    Method & method,
    const ConstColumnPlainPtrs & key_columns,
    const ColumnArray::Offsets_t & offsets,
    ColumnUInt8::Container_t & vec_res,
    bool negative,
    size_t rows) const
{
    typename Method::State state;
    state.init(key_columns);
    size_t keys_size = key_columns.size();

    size_t prev_offset = 0;
    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        UInt8 res = 0;
        /// For all elements
        for (size_t j = prev_offset; j < offsets[i]; ++j)
        {
            /// Build the key
            typename Method::Key key = state.getKey(key_columns, keys_size, j, key_sizes);
            res |= negative ^ method.data.has(key);
            if (res)
                break;
        }
        vec_res[i] = res;
        prev_offset = offsets[i];
    }
}


void Set::executeOrdinary(
    const ConstColumnPlainPtrs & key_columns,
    ColumnUInt8::Container_t & vec_res,
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

void Set::executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const
{
    size_t rows = key_column->size();
    const ColumnArray::Offsets_t & offsets = key_column->getOffsets();
    const IColumn & nested_column = key_column->getData();

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            executeArrayImpl(*data.NAME, ConstColumnPlainPtrs{&nested_column}, offsets, vec_res, negative, rows); \
            break;
    APPLY_FOR_SET_VARIANTS(M)
#undef M
    }
}


/// Return the BoolMask.
/// The first element is whether the `range` element can be an element of a set.
/// The second element is whether the element in the `range` range is not from the set.
BoolMask Set::mayBeTrueInRange(const Range & range) const
{
    if (!ordered_set_elements)
        throw Exception("Ordered set in not created.");

    if (ordered_set_elements->empty())
        return {false, true};

    /// Range (-inf; + inf)
    if (!range.left_bounded && !range.right_bounded)
        return {true, true};

    const Field & left = range.left;
    const Field & right = range.right;

    /// Range (-inf; right|
    if (!range.left_bounded)
    {
        if (range.right_included)
            return {ordered_set_elements->front() <= right, true};
        else
            return {ordered_set_elements->front() < right, true};
    }

    /// Range |left; +Inf)
    if (!range.right_bounded)
    {
        if (range.left_included)
            return {ordered_set_elements->back() >= left, true};
        else
            return {ordered_set_elements->back() > left, true};
    }

    /// Range from one value [left].
    if (range.left_included && range.right_included && left == right)
    {
        if (std::binary_search(ordered_set_elements->begin(), ordered_set_elements->end(), left))
            return {true, false};
        else
            return {false, true};
    }

    /// The first element of the set that is greater than or equal to `left`.
    auto left_it = std::lower_bound(ordered_set_elements->begin(), ordered_set_elements->end(), left);

    /// If `left` is not in the range (open range), then take the next element in the order of the set.
    if (!range.left_included && left_it != ordered_set_elements->end() && *left_it == left)
        ++left_it;

    /// if the entire range is to the right of the set: `{ set } | range |`
    if (left_it == ordered_set_elements->end())
        return {false, true};

    /// The first element of the set, which is strictly greater than `right`.
    auto right_it = std::upper_bound(ordered_set_elements->begin(), ordered_set_elements->end(), right);

    /// the whole range to the left of the set: `| range | { set }`
    if (right_it == ordered_set_elements->begin())
        return {false, true};

    /// The last element of the set that is less than or equal to `right`.
    --right_it;

    /// If `right` does not enter the range (open range), then take the previous element in the order of the set.
    if (!range.right_included && *right_it == right)
    {
        /// the entire range to the left of the set, although the open range is tangent to the set: `| range) { set }`
        if (right_it == ordered_set_elements->begin())
            return {false, true};

        --right_it;
    }

    /// The range does not contain any keys from the set, although it is located somewhere in the middle relative to its elements: * * * * [ ] * * * *
    if (right_it < left_it)
        return {false, true};

    return {true, true};
}


std::string Set::describe() const
{
    if (!ordered_set_elements)
        return "{}";

    bool first = true;
    std::stringstream ss;

    ss << "{";
    for (const Field & f : *ordered_set_elements)
    {
        ss << (first ? "" : ", ") << applyVisitor(FieldVisitorToString(), f);
        first = false;
    }
    ss << "}";
    return ss.str();
}

}
