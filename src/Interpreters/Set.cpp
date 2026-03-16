#include <optional>
#include <shared_mutex>

#include <Core/Field.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>

#include <Common/Logger.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnDecimal.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <DataTypes/NullableUtils.h>
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

Set::Set(const SizeLimits & limits_, size_t max_elements_to_fill_, bool transform_null_in_)
    :  limits(limits_), transform_null_in(transform_null_in_), max_elements_to_fill(max_elements_to_fill_)
    , log(getLogger("Set")), cast_cache(std::make_unique<InternalCastFunctionCache>())
{}


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
        /// Strip LowCardinality recursively to match what setHeader/insertFromColumns do:
        /// insertFromColumns calls convertToFullIfNeeded which recursively strips LC from
        /// compound types like Tuple(LowCardinality(T), ...).
        type = recursiveRemoveLowCardinality(type);

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
            materialized_columns.emplace_back(key_columns.back()->convertToFullColumnIfLowCardinality());
            key_columns.back() = materialized_columns.back().get();
        }

        /// Strip LowCardinality recursively from set_elements_types so they match what
        /// convertToFullIfNeeded (which is recursive) does to columns in insertFromColumns.
        /// Without this, compound types like Tuple(LowCardinality(T), ...) keep LowCardinality
        /// in the type while the column has it stripped, causing type/column mismatches later.
        set_elements_types.back() = recursiveRemoveLowCardinality(set_elements_types.back());
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

ColumnUInt8::Ptr checkDateTimePrecision(const ColumnWithTypeAndName & column_to_cast)
{
    // Handle nullable columns
    const ColumnNullable * original_nullable_column = typeid_cast<const ColumnNullable *>(column_to_cast.column.get());
    const IColumn * original_nested_column = original_nullable_column
        ? &original_nullable_column->getNestedColumn()
        : column_to_cast.column.get();

    // Check if the original column is of ColumnDecimal<DateTime64> type
    const auto * original_decimal_column = typeid_cast<const ColumnDecimal<DateTime64> *>(original_nested_column);
    if (!original_decimal_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnDecimal for DateTime64");

    // Get the data array from the original column
    const auto & original_data = original_decimal_column->getData();
    size_t vec_res_size = original_data.size();

    // Prepare the precision null map
    auto precision_null_map_column = ColumnUInt8::create(vec_res_size, static_cast<UInt8>(0));
    NullMap & precision_null_map = precision_null_map_column->getData();

    // Determine which rows should be null based on precision loss
    const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(column_to_cast.type.get());
    auto scale = datetime64_type->getScale();
    if (scale >= 1)
    {
        Int64 scale_multiplier = common::exp10_i32(scale);
        for (size_t row = 0; row < vec_res_size; ++row)
        {
            Int64 value = original_data[row];
            if (value % scale_multiplier != 0)
                precision_null_map[row] = 1; // Mark as null due to precision loss
            else
                precision_null_map[row] = 0;
        }
    }

    return precision_null_map_column;
}

ColumnPtr mergeNullMaps(const ColumnPtr & null_map_column1, const ColumnUInt8::Ptr & null_map_column2)
{
    if (!null_map_column1)
        return null_map_column2;
    if (!null_map_column2)
        return null_map_column1;

    const auto & null_map1 = assert_cast<const ColumnUInt8 &>(*null_map_column1).getData();
    const auto & null_map2 = (*null_map_column2).getData();

    size_t size = null_map1.size();
    if (size != null_map2.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Null maps have different sizes");

    auto merged_null_map_column = ColumnUInt8::create(size);
    auto & merged_null_map = merged_null_map_column->getData();

    for (size_t i = 0; i < size; ++i)
        merged_null_map[i] = null_map1[i] || null_map2[i];

    return merged_null_map_column;
}

void Set::processDateTime64Column(
    const ColumnWithTypeAndName & column_to_cast,
    ColumnPtr & result,
    ColumnPtr & null_map_holder,
    ConstNullMapPtr & null_map) const
{
    // Check for sub-second precision and create a null map
    ColumnUInt8::Ptr filtered_null_map_column = checkDateTimePrecision(column_to_cast);

    // Extract existing null map and nested column from the result
    const ColumnNullable * result_nullable_column = typeid_cast<const ColumnNullable *>(result.get());
    const IColumn * nested_result_column = result_nullable_column
        ? &result_nullable_column->getNestedColumn()
        : result.get();

    ColumnPtr existing_null_map_column = result_nullable_column
        ? result_nullable_column->getNullMapColumnPtr()
        : nullptr;

    if (transform_null_in)
    {
        if (!null_map_holder)
            null_map_holder = filtered_null_map_column;
        else
            null_map_holder = mergeNullMaps(null_map_holder, filtered_null_map_column);

        const ColumnUInt8 * null_map_column = checkAndGetColumn<ColumnUInt8>(null_map_holder.get());
        if (!null_map_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Null map must be ColumnUInt8");

        null_map = &null_map_column->getData();
    }
    else
    {
        ColumnPtr merged_null_map_column = mergeNullMaps(existing_null_map_column, filtered_null_map_column);
        result = ColumnNullable::create(nested_result_column->getPtr(), merged_null_map_column);
    }
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

    /// We will check existence in Set only for keys whose components do not contain any NULL value.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;

    for (size_t i = 0; i < num_key_columns; ++i)
    {
        ColumnPtr result;

        const auto & column_before_cast = columns.at(i);
        ColumnWithTypeAndName column_to_cast
            = {column_before_cast.column->convertToFullColumnIfConst(), column_before_cast.type, column_before_cast.name};

        /// Since we have optional support for Nullable(Tuple), if `data_types[i]` is `Tuple(...)` type, then
        /// we will enter the `castColumnAccurateOrNull` path; however, it can lead to casted column type
        /// becomes `Tuple(Nullable(...), Nullable(...))` which will create problems during matching keys in Set.
        /// To avoid that, we do not do `castColumnAccurateOrNull` for Tuple types.
        auto target_type_without_nullable = removeNullable(data_types[i]);
        bool is_tuple_type = typeid_cast<const DataTypeTuple *>(target_type_without_nullable.get()) != nullptr;

        bool use_cast_accurate_or_null = !transform_null_in && data_types[i]->canBeInsideNullable() && !is_tuple_type;

        if (use_cast_accurate_or_null)
        {
            result = castColumnAccurateOrNull(column_to_cast, data_types[i], cast_cache.get());
        }
        else
        {
            /// Special case when transform_null_in = true and type of column is Nullable but type of this key in Set is not Nullable.
            /// For example: SELECT NULL::Nullable(String) IN (SELECT 'abc') SETTINGS transform_null_in = 1;
            /// In this case we cannot just cast Nullable column to non-nullable type because it will fail if column contains nulls.
            /// We should cast nested column and remember the null map to use negative value on rows with null (as key column is not
            /// Nullable, Set cannot contain nulls for this column anyhow).
            if (transform_null_in && column_to_cast.type->isNullable() && !data_types[i]->isNullable())
            {
                auto nested_type = assert_cast<const DataTypeNullable &>(*column_to_cast.type).getNestedType();
                const auto & column_nullable = assert_cast<const ColumnNullable &>(*column_to_cast.column);
                result = castColumnAccurate(ColumnWithTypeAndName(column_nullable.getNestedColumnPtr(), nested_type, column_to_cast.name), data_types[i], cast_cache.get());
                if (!null_map_holder)
                {
                    null_map_holder = column_nullable.getNullMapColumnPtr();
                }
                else
                {
                    MutableColumnPtr mutable_null_map_holder = IColumn::mutate(std::move(null_map_holder));

                    PaddedPODArray<UInt8> & mutable_null_map = assert_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
                    const PaddedPODArray<UInt8> & other_null_map = column_nullable.getNullMapData();
                    for (size_t j = 0, size = mutable_null_map.size(); j < size; ++j)
                        mutable_null_map[j] |= other_null_map[j];

                    null_map_holder = std::move(mutable_null_map_holder);
                }

                null_map = &assert_cast<const ColumnUInt8 &>(*null_map_holder).getData();
            }
            else
            {
                result = castColumnAccurate(column_to_cast, data_types[i], cast_cache.get());
            }
        }

        // If the original column is DateTime64, check for sub-second precision
        if (isDateTime64(column_to_cast.column->getDataType()) && !isDateTime64(removeNullable(result)->getDataType()))
        {
            processDateTime64Column(column_to_cast, result, null_map_holder, null_map);
        }

        // Append the result to materialized columns
        materialized_columns.emplace_back(std::move(result));
        key_columns.emplace_back(materialized_columns.back().get());
    }

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
    ::sort(indexes_mapping.begin(), indexes_mapping.end(),
        [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
        {
            return std::tie(l.key_index, l.tuple_index) < std::tie(r.key_index, r.tuple_index);
        });

    /// Deduplicate key columns. This can make the condition weaker, e.g.:
    ///     (x + 1, x + 2) IN (10, 10)
    /// effectively turns into:
    ///     (x + 1) IN (10)
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

    struct FieldValueRange
    {
        FieldValue left;
        FieldValue right;
        bool left_included = false;
        bool right_included = false;

        explicit FieldValueRange(const IColumn & prototype) : left(prototype.cloneEmpty()), right(prototype.cloneEmpty()) {}
    };

    std::vector<FieldValueRange> ranges;
    ranges.reserve(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
    {
        std::optional<Range> new_range = KeyCondition::applyMonotonicFunctionsChainToRange(
            key_ranges[indexes_mapping[i].key_index],
            indexes_mapping[i].functions,
            data_types[indexes_mapping[i].key_index],
            single_point);

        if (!new_range)
            return {true, true};

        ranges.emplace_back(*ordered_set[i]);
        ranges.back().left.update(new_range->left);
        ranges.back().right.update(new_range->right);
        ranges.back().left_included = new_range->left_included;
        ranges.back().right_included = new_range->right_included;
    }

    /// lhs < rhs return -1
    /// lhs == rhs return 0
    /// lhs > rhs return 1
    auto compare = [](const IColumn & lhs, const FieldValue & rhs, size_t row)
    {
        if (rhs.isNegativeInfinity())
            return +1;
        if (rhs.isPositiveInfinity())
            return lhs.isNullAt(row) ? 0 : -1; // +Inf == +Inf
        return lhs.compareAt(row, 0, *rhs.column, 1);
    };

    /// Because ordered_set is sorted lexicographically, the elements we're looking for are
    /// consecituve. Use binary search to find the range of indices.

    /// The part about left_included/right_included is a little tricky. It was initially implemented
    /// incorrectly:
    ///   begin = lower_bound(..., left_point, tuple_less_unaware_of_includedness);
    ///   if (!all(ranges[..].left_included) && equals(left_point, begin))
    ///       begin += 1;
    /// This breaks on the following example:
    ///   key_ranges = [(0, +inf), (-inf, +inf)]  (the 0 is not included),
    ///   ordered_set = [[0], [0]].
    /// The incorrect implementation would output begin == 0 and conclude that the set element is
    /// inside the range (it isn't). The `begin += 1` won't happen because (0, 0) != (0, -inf).

    auto indices = collections::range(0, size());
    size_t begin = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return true if set[row] is below the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].left, row);

                if (cmp > 0)
                    return false;
                /// Note: if some range has left_included == false then the left ends of all
                /// subsequent ranges' don't matter. (Symmetrically for right.)
                /// It's the only way to make sense of the notion of a range of tuples where the
                /// included/excluded flags are given per element.
                if (cmp < 0 || (cmp == 0 && !ranges[i].left_included))
                    return true;
            }
            return false;
        }) - indices.begin();
    size_t end = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return false if set[row] is above the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].right, row);

                if (cmp > 0 || (cmp == 0 && !ranges[i].right_included))
                    return false;
                if (cmp < 0)
                    return true;
            }
            return true;
        }) - indices.begin();

    if (begin > end)
    {
        /// TODO: Remove the #ifndef and always throw after
        ///       https://github.com/ClickHouse/ClickHouse/issues/90461 is fixed.
        ///       (What happens here is: the applyMonotonicFunctionsChainToRange call above applies
        ///        nonmonotonic functions, and we end up with left > right.)
#ifndef NDEBUG
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid binary search result in MergeTreeSetIndex");
#else
        return {true, true};
#endif
    }

    bool can_be_true = begin < end;

    /// A special case of 1-element KeyRange. It's useful for partition pruning.
    bool at_most_one_element_range = true;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        auto & r = ranges[i];
        if (r.left.isNormal() && r.right.isNormal())
        {
            if (0 != r.left.column->compareAt(0, 0, *r.right.column, 1))
            {
                at_most_one_element_range = false;
                break;
            }
        }
        else if ((r.left.isPositiveInfinity() && r.right.isPositiveInfinity()) || (r.left.isNegativeInfinity() && r.right.isNegativeInfinity()))
        {
            /// Special value equality.
        }
        else
        {
            at_most_one_element_range = false;
            break;
        }
    }
    if (at_most_one_element_range && has_all_keys)
    {
        /// Here we know that there is at most one element in range.
        /// The main difference with the normal case is that we can definitely say that
        /// condition in this range is always TRUE (can_be_false = 0) or always FALSE (can_be_true = 0).
        return {can_be_true, !can_be_true};
    }

    return {can_be_true, true};
}

bool MergeTreeSetIndex::hasMonotonicFunctionsChain() const
{
    for (const auto & mapping : indexes_mapping)
        if (!mapping.functions.empty())
            return true;
    return false;
}

void MergeTreeSetIndex::FieldValue::update(const Field & x)
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
