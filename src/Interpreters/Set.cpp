#include <optional>

#include <Core/Field.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>

#include <Common/typeid_cast.h>

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
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>

#include <Processors/Chunk.h>

#include <Storages/MergeTree/KeyCondition.h>

#include <base/range.h>
#include <base/sort.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
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
        {
            if ((*null_map)[i])
            {
                if constexpr (build_filter)
                {
                    (*out_filter)[i] = false;
                }
                continue;
            }
        }

        [[maybe_unused]] auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        if constexpr (build_filter)
            (*out_filter)[i] = emplace_result.isInserted();
    }
}


DataTypes Set::getElementTypes(DataTypes types, bool transform_null_in)
{
    for (auto & type : types)
    {
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
            type = low_cardinality_type->getDictionaryType();

        if (!transform_null_in)
            type = removeNullable(type);
    }

    return types;
}


void Set::setHeader(const ColumnsWithTypeAndName & header)
{
    std::lock_guard lock(rwlock);

    if (!data.empty())
        return;

    keys_size = header.size();
    ColumnRawPtrs key_columns;
    key_columns.reserve(keys_size);
    data_types.reserve(keys_size);
    set_elements_types.reserve(keys_size);

    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        materialized_columns.emplace_back(header.at(i).column->convertToFullColumnIfConst());
        key_columns.emplace_back(materialized_columns.back().get());
        data_types.emplace_back(header.at(i).type);
        set_elements_types.emplace_back(header.at(i).type);

        /// Convert low cardinality column to full.
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_types.back().get()))
        {
            data_types.back() = low_cardinality_type->getDictionaryType();
            set_elements_types.back() = low_cardinality_type->getDictionaryType();
            materialized_columns.emplace_back(key_columns.back()->convertToFullColumnIfLowCardinality());
            key_columns.back() = materialized_columns.back().get();
        }
    }

    /// We will insert to the Set only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;
    if (!transform_null_in)
    {
        /// We convert nullable columns to non nullable we also need to update nullable types
        for (size_t i = 0; i < set_elements_types.size(); ++i)
        {
            data_types[i] = removeNullable(data_types[i]);
            set_elements_types[i] = removeNullable(set_elements_types[i]);
        }

        extractNestedColumnsAndNullMap(key_columns, null_map);
    }

    /// Choose data structure to use for the set.
    data.init(SetVariants::chooseMethod(key_columns, key_sizes));
}

void Set::fillSetElements()
{
    fill_set_elements = true;
    set_elements.reserve(keys_size);
    for (const auto & type : set_elements_types)
        set_elements.emplace_back(type->createColumn());
}

bool Set::insertFromBlock(const ColumnsWithTypeAndName & columns)
{
    Columns cols;
    cols.reserve(columns.size());
    for (const auto & column : columns)
        cols.emplace_back(column.column);
    return insertFromColumns(cols);
}

bool Set::insertFromColumns(const Columns & columns)
{
    size_t rows = columns.at(0)->size();

    SetKeyColumns holder;
    /// Filter to extract distinct values from the block.
    if (fill_set_elements)
        holder.filter = ColumnUInt8::create(rows);

    bool inserted = insertFromColumns(columns, holder);
    if (inserted && fill_set_elements)
    {
        if (max_elements_to_fill && max_elements_to_fill < data.getTotalRowCount())
        {
            /// Drop filled elementes
            fill_set_elements = false;
            set_elements.clear();
        }
        else
            appendSetElements(holder);
    }

    return inserted;
}

bool Set::insertFromColumns(const Columns & columns, SetKeyColumns & holder)
{
    std::lock_guard lock(rwlock);

    if (data.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method Set::setHeader must be called before Set::insertFromBlock");

    holder.key_columns.reserve(keys_size);
    holder.materialized_columns.reserve(keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        holder.materialized_columns.emplace_back(columns.at(i)->convertToFullIfNeeded());
        holder.key_columns.emplace_back(holder.materialized_columns.back().get());
    }

    size_t rows = columns.at(0)->size();

    /// We will insert to the Set only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;
    if (!transform_null_in)
        null_map_holder = extractNestedColumnsAndNullMap(holder.key_columns, null_map);

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
        case SetVariants::Type::NAME: \
            insertFromBlockImpl(*data.NAME, holder.key_columns, rows, data, null_map, holder.filter ? &holder.filter->getData() : nullptr); \
            break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    return limits.check(data.getTotalRowCount(), data.getTotalByteCount(), "IN-set", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void Set::appendSetElements(SetKeyColumns & holder)
{
    if (holder.key_columns.size() != keys_size || set_elements.size() != keys_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of key columns for set. Expected {} got {} and {}",
                        keys_size, holder.key_columns.size(), set_elements.size());

    size_t rows = holder.key_columns.at(0)->size();
    for (size_t i = 0; i < keys_size; ++i)
    {
        auto filtered_column = holder.key_columns[i]->filter(holder.filter->getData(), rows);
        if (set_elements[i]->empty())
            set_elements[i] = filtered_column;
        else
            set_elements[i]->insertRangeFrom(*filtered_column, 0, filtered_column->size());
        if (transform_null_in && holder.null_map_holder)
            set_elements[i]->insert(Null{});
    }
}

void Set::checkIsCreated() const
{
    if (!is_created.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to use set before it has been built.");
}

ColumnPtr Set::execute(const ColumnsWithTypeAndName & columns, bool negative) const
{
    size_t num_key_columns = columns.size();

    if (0 == num_key_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns passed to Set::execute method.");

    auto res = ColumnUInt8::create();
    ColumnUInt8::Container & vec_res = res->getData();
    vec_res.resize(columns.at(0).column->size());

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

    checkColumnsNumber(num_key_columns);

    /// Remember the columns we will work with. Also check that the data types are correct.
    ColumnRawPtrs key_columns;
    key_columns.reserve(num_key_columns);

    /// The constant columns to the left of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;
    materialized_columns.reserve(num_key_columns);

    for (size_t i = 0; i < num_key_columns; ++i)
    {
        ColumnPtr result;

        const auto & column_before_cast = columns.at(i);
        ColumnWithTypeAndName column_to_cast
            = {column_before_cast.column->convertToFullColumnIfConst(), column_before_cast.type, column_before_cast.name};

        if (!transform_null_in && data_types[i]->canBeInsideNullable())
        {
            result = castColumnAccurateOrNull(column_to_cast, data_types[i], cast_cache.get());
        }
        else
        {
            result = castColumnAccurate(column_to_cast, data_types[i], cast_cache.get());
        }

        materialized_columns.emplace_back() = result;
        key_columns.emplace_back() = materialized_columns.back().get();
    }

    /// We will check existence in Set only for keys whose components do not contain any NULL value.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;
    if (!transform_null_in)
        null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    executeOrdinary(key_columns, vec_res, negative, null_map);

    return res;
}

bool Set::hasNull() const
{
    checkIsCreated();

    if (!transform_null_in)
        return false;

    if (data_types.size() != 1)
        return false;

    if (!data_types[0]->isNullable())
        return false;

    auto col = data_types[0]->createColumn();
    col->insert(Field());
    auto res = execute({ColumnWithTypeAndName(std::move(col), data_types[0], std::string())}, false);
    return res->getBool(0);
}

bool Set::empty() const
{
    std::shared_lock lock(rwlock);
    return data.empty();
}

size_t Set::getTotalRowCount() const
{
    std::shared_lock lock(rwlock);
    return data.getTotalRowCount();
}

size_t Set::getTotalByteCount() const
{
    std::shared_lock lock(rwlock);
    return data.getTotalByteCount();
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
        {
            vec_res[i] = negative;
        }
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

void Set::checkColumnsNumber(size_t num_key_columns) const
{
    if (data_types.size() != num_key_columns)
    {
        throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                        "Number of columns in section IN doesn't match. {} at left, {} at right.",
                        num_key_columns, data_types.size());
    }
}

bool Set::areTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const
{
    /// Out-of-bound access can happen when same set expression built with different columns.
    /// Caller may call this method to make sure that the set is indeed the one they want
    /// without awaring data_types.size().
    if (set_type_idx >= data_types.size())
        return false;
    return removeNullable(recursiveRemoveLowCardinality(data_types[set_type_idx]))
        ->equals(*removeNullable(recursiveRemoveLowCardinality(other_type)));
}

void Set::checkTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const
{
    if (!this->areTypesEqual(set_type_idx, other_type))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Types of column {} in section IN don't match: "
                        "{} on the left, {} on the right", toString(set_type_idx + 1),
                        other_type->getName(), data_types[set_type_idx]->getName());
}

MergeTreeSetIndex::MergeTreeSetIndex(const Columns & set_elements, std::vector<KeyTuplePositionMapping> && indexes_mapping_)
    : has_all_keys(set_elements.size() == indexes_mapping_.size()), indexes_mapping(std::move(indexes_mapping_))
{
    // std::cerr << "MergeTreeSetIndex::MergeTreeSetIndex "
    //     << set_elements.size() << ' ' << indexes_mapping.size() << std::endl;
    // for (const auto & vv : indexes_mapping)
    //     std::cerr << vv.key_index << ' ' << vv.tuple_index << std::endl;

    ::sort(indexes_mapping.begin(), indexes_mapping.end(),
        [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
        {
            return std::tie(l.key_index, l.tuple_index) < std::tie(r.key_index, r.tuple_index);
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
        String column_name = "_" + toString(i);
        block_to_sort.insert({ordered_set[i], nullptr, column_name});
        sort_description.emplace_back(column_name, 1, 1);
    }

    sortBlock(block_to_sort, sort_description);

    for (size_t i = 0; i < tuple_size; ++i)
        ordered_set[i] = block_to_sort.getByPosition(i).column;
}


/** Return the BoolMask where:
  * 1: the intersection of the set and the range is non-empty
  * 2: the range contains elements not in the set
  */
BoolMask MergeTreeSetIndex::checkInRange(const std::vector<Range> & key_ranges, const DataTypes & data_types, bool single_point) const
{
    size_t tuple_size = indexes_mapping.size();
    // std::cerr << "MergeTreeSetIndex::checkInRange " << single_point << ' ' << tuple_size << ' ' << has_all_keys << std::endl;

    FieldValues left_point;
    FieldValues right_point;
    left_point.reserve(tuple_size);
    right_point.reserve(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
    {
        left_point.emplace_back(ordered_set[i]->cloneEmpty());
        right_point.emplace_back(ordered_set[i]->cloneEmpty());
    }

    bool left_included = true;
    bool right_included = true;

    for (size_t i = 0; i < tuple_size; ++i)
    {
        std::optional<Range> new_range = KeyCondition::applyMonotonicFunctionsChainToRange(
            key_ranges[indexes_mapping[i].key_index],
            indexes_mapping[i].functions,
            data_types[indexes_mapping[i].key_index],
            single_point);

        if (!new_range)
            return {true, true};

        left_point[i].update(new_range->left);
        left_included &= new_range->left_included;
        right_point[i].update(new_range->right);
        right_included &= new_range->right_included;
    }

    /// lhs < rhs return -1
    /// lhs == rhs return 0
    /// lhs > rhs return 1
    auto compare = [](const IColumn & lhs, const FieldValue & rhs, size_t row)
    {
        if (rhs.isNegativeInfinity())
            return 1;
        if (rhs.isPositiveInfinity())
        {
            Field f;
            lhs.get(row, f);
            if (f.isNull())
                return 0; // +Inf == +Inf
            return -1;
        }
        return lhs.compareAt(row, 0, *rhs.column, 1);
    };

    auto less = [this, &compare, tuple_size](size_t row, const auto & point)
    {
        for (size_t i = 0; i < tuple_size; ++i)
        {
            int res = compare(*ordered_set[i], point[i], row);
            if (res)
                return res < 0;
        }
        return false;
    };

    auto equals = [this, &compare, tuple_size](size_t row, const auto & point)
    {
        for (size_t i = 0; i < tuple_size; ++i)
            if (compare(*ordered_set[i], point[i], row) != 0)
                return false;
        return true;
    };

    /** Because each hyperrectangle maps to a contiguous sequence of elements
      * laid out in the lexicographically increasing order, the set intersects the range
      * if and only if either bound coincides with an element or at least one element
      * is between the lower bounds
      */
    auto indices = collections::range(0, size());
    auto left_lower = std::lower_bound(indices.begin(), indices.end(), left_point, less);
    auto right_lower = std::lower_bound(indices.begin(), indices.end(), right_point, less);

    /// A special case of 1-element KeyRange. It's useful for partition pruning.
    bool one_element_range = true;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        auto & left = left_point[i];
        auto & right = right_point[i];
        if (left.isNormal() && right.isNormal())
        {
            if (0 != left.column->compareAt(0, 0, *right.column, 1))
            {
                one_element_range = false;
                break;
            }
        }
        else if ((left.isPositiveInfinity() && right.isPositiveInfinity()) || (left.isNegativeInfinity() && right.isNegativeInfinity()))
        {
            /// Special value equality.
        }
        else
        {
            one_element_range = false;
            break;
        }
    }
    if (one_element_range && has_all_keys)
    {
        /// Here we know that there is one element in range.
        /// The main difference with the normal case is that we can definitely say that
        /// condition in this range is always TRUE (can_be_false = 0) or always FALSE (can_be_true = 0).

        /// Check if it's an empty range
        if (!left_included || !right_included)
            return {false, true};
        if (left_lower != indices.end() && equals(*left_lower, left_point))
            return {true, false};
        return {false, true};
    }

    /// If there are more than one element in the range, it can always be false. Thus we only need to check if it may be true or not.
    /// Given left_lower >= left_point, right_lower >= right_point, find if there may be a match in between left_lower and right_lower.
    if (left_lower + 1 < right_lower)
    {
        /// There is a point in between: left_lower + 1
        return {true, true};
    }
    if (left_lower + 1 == right_lower)
    {
        /// Need to check if left_lower is a valid match, as left_point <= left_lower < right_point <= right_lower.
        /// Note: left_lower is valid.
        if (left_included || !equals(*left_lower, left_point))
            return {true, true};

        /// We are unlucky that left_point fails to cover a point. Now we need to check if right_point can cover right_lower.
        /// Check if there is a match at the right boundary.
        return {right_included && right_lower != indices.end() && equals(*right_lower, right_point), true};
    }
    // left_lower == right_lower
    /// Need to check if right_point is a valid match, as left_point < right_point <= left_lower = right_lower.
    /// Check if there is a match at the left boundary.
    return {right_included && right_lower != indices.end() && equals(*right_lower, right_point), true};
}

bool MergeTreeSetIndex::hasMonotonicFunctionsChain() const
{
    for (const auto & mapping : indexes_mapping)
        if (!mapping.functions.empty())
            return true;
    return false;
}

void FieldValue::update(const Field & x)
{
    if (x.isNegativeInfinity() || x.isPositiveInfinity())
        value = x;
    else
    {
        /// Keep at most one element in column.
        if (!column->empty())
            column->popBack(1);
        column->insert(x);
        value = Field(); // Set back to normal value.
    }
}

}
