#include <algorithm>
#include <Processors/Transforms/FilterTransform.h>

#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

static void replaceFilterToConstant(Block & block, const String & filter_column_name)
{
    ConstantFilterDescription constant_filter_description;

    auto filter_column = block.getPositionByName(filter_column_name);
    auto & column_elem = block.safeGetByPosition(filter_column);

    /// Isn't the filter already constant?
    if (column_elem.column)
        constant_filter_description = ConstantFilterDescription(*column_elem.column);

    if (!constant_filter_description.always_false
        && !constant_filter_description.always_true)
    {
        /// Replace the filter column to a constant with value 1.
        FilterDescription filter_description_check(*column_elem.column);
        column_elem.column = column_elem.type->createColumnConst(block.rows(), 1u);
    }
}

static std::shared_ptr<const ChunkSelectFinalIndices> getSelectByFinalIndices(Chunk & chunk)
{
    if (auto select_final_indices_info = std::dynamic_pointer_cast<const ChunkSelectFinalIndices>(chunk.getChunkInfo()))
    {
        const auto & index_column = select_final_indices_info->select_final_indices;
        chunk.setChunkInfo(nullptr);
        if (index_column && index_column->size() != chunk.getNumRows())
            return select_final_indices_info;
    }
    return nullptr;
}

static void
executeSelectByIndices(Columns & columns, std::shared_ptr<const ChunkSelectFinalIndices> & select_final_indices_info, size_t & num_rows)
{
    if (select_final_indices_info)
    {
        const auto & index_column = select_final_indices_info->select_final_indices;

        for (auto & column : columns)
            column = column->index(*index_column, 0);

        num_rows = index_column->size();
    }
}

static std::unique_ptr<IFilterDescription> combineFilterAndIndices(
    std::unique_ptr<FilterDescription> description,
    std::shared_ptr<const ChunkSelectFinalIndices> & select_final_indices_info,
    size_t num_rows)
{
    if (select_final_indices_info)
    {
        const auto * index_column = select_final_indices_info->select_final_indices;

        if (description->hasOne())
        {
            const auto & selected_by_indices = index_column->getData();
            const auto * selected_by_filter = description->data->data();
            /// We will recompute new has_one
            description->has_one = 0;
            /// At this point we know that the filter is not constant, just create a new filter
            auto mutable_holder = ColumnUInt8::create(num_rows, 0);
            auto & data = mutable_holder->getData();
            for (auto idx : selected_by_indices)
                data[idx] = 1;

            /// AND two filters
            auto * begin = data.data();
            const auto * end = begin + num_rows;
#if defined(__AVX2__)
            while (end - begin >= 32)
            {
                _mm256_storeu_si256(
                    reinterpret_cast<__m256i *>(begin),
                    _mm256_and_si256(
                        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(begin)),
                        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(selected_by_filter))));
                description->has_one |= !memoryIsZero(begin, 0, 32);
                begin += 32;
                selected_by_filter += 32;
            }
#elif defined(__SSE2__)
            while (end - begin >= 16)
            {
                _mm_storeu_si128(
                    reinterpret_cast<__m128i *>(begin),
                    _mm_and_si128(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(begin)),
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(selected_by_filter))));
                description->has_one |= !memoryIsZero(begin, 0, 16);
                begin += 16;
                selected_by_filter += 16;
            }
#endif

            while (end - begin >= 8)
            {
                *reinterpret_cast<UInt64 *>(begin) &= *reinterpret_cast<const UInt64 *>(selected_by_filter);
                description->has_one |= *reinterpret_cast<UInt64 *>(begin);
                begin += 8;
                selected_by_filter += 8;
            }

            while (end - begin > 0)
            {
                *begin &= *selected_by_filter;
                description->has_one |= *begin;
                begin++;
                selected_by_filter++;
            }

            description->data_holder = std::move(mutable_holder);
            description->data = &data;
        }
    }
    return std::move(description);
}

static std::unique_ptr<IFilterDescription> combineFilterAndIndices(
    std::unique_ptr<SparseFilterDescription> description,
    std::shared_ptr<const ChunkSelectFinalIndices> & select_final_indices_info,
    size_t num_rows)
{
    /// Iterator interface to decorate data from output of std::set_intersection
    struct Iterator
    {
        UInt8 * data;
        Int64 & pop_cnt;
        explicit Iterator(UInt8 * data_, Int64 & pop_cnt_) : data(data_), pop_cnt(pop_cnt_) {}
        Iterator & operator = (UInt64 index) { data[index] = 1; ++pop_cnt; return *this; }
        Iterator & operator ++ () { return *this; }
        Iterator & operator * () { return *this; }
    };

    if (select_final_indices_info)
    {
        const auto * index_column = select_final_indices_info->select_final_indices;

        if (description->hasOne())
        {
            std::unique_ptr<FilterDescription> res;
            res->has_one = 0;
            const auto & selected_by_indices = index_column->getData();
            const auto & selected_by_filter = description->filter_indices->getData();
            auto mutable_holder = ColumnUInt8::create(num_rows, 0);
            auto & data = mutable_holder->getData();
            Iterator decorator(data.data(), res->has_one);
            std::set_intersection(selected_by_indices.begin(), selected_by_indices.end(), selected_by_filter.begin(), selected_by_filter.end(), decorator);
            res->data_holder = std::move(mutable_holder);
            res->data = &data;
            return res;
        }
    }
    return std::move(description);
}

Block FilterTransform::transformHeader(
    Block header,
    const ActionsDAG * expression,
    const String & filter_column_name,
    bool remove_filter_column)
{
    if (expression)
        header = expression->updateHeader(std::move(header));

    auto filter_type = header.getByName(filter_column_name).type;
    if (!filter_type->onlyNull() && !isUInt8(removeNullable(removeLowCardinality(filter_type))))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Illegal type {} of column {} for filter. Must be UInt8 or Nullable(UInt8).",
            filter_type->getName(), filter_column_name);

    if (remove_filter_column)
        header.erase(filter_column_name);
    else
        replaceFilterToConstant(header, filter_column_name);

    return header;
}

FilterTransform::FilterTransform(
    const Block & header_,
    ExpressionActionsPtr expression_,
    String filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_,
    std::shared_ptr<std::atomic<size_t>> rows_filtered_)
    : ISimpleTransform(
            header_,
            transformHeader(header_, expression_ ? &expression_->getActionsDAG() : nullptr, filter_column_name_, remove_filter_column_),
            true)
    , expression(std::move(expression_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
    , rows_filtered(rows_filtered_)
{
    transformed_header = getInputPort().getHeader();
    if (expression)
        expression->execute(transformed_header);
    filter_column_position = transformed_header.getPositionByName(filter_column_name);

    auto & column = transformed_header.getByPosition(filter_column_position).column;
    if (column)
        constant_filter_description = ConstantFilterDescription(*column);
}

IProcessor::Status FilterTransform::prepare()
{
    if (!on_totals
        && (constant_filter_description.always_false
            /// Optimization for `WHERE column in (empty set)`.
            /// The result will not change after set was created, so we can skip this check.
            /// It is implemented in prepare() stop pipeline before reading from input port.
            || (!are_prepared_sets_initialized && expression && expression->checkColumnIsAlwaysFalse(filter_column_name))))
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    auto status = ISimpleTransform::prepare();

    /// Until prepared sets are initialized, output port will be unneeded, and prepare will return PortFull.
    if (status != IProcessor::Status::PortFull)
        are_prepared_sets_initialized = true;

    return status;
}


void FilterTransform::removeFilterIfNeed(Chunk & chunk) const
{
    if (chunk && remove_filter_column)
        chunk.erase(filter_column_position);
}

void FilterTransform::transform(Chunk & chunk)
{
    auto chunk_rows_before = chunk.getNumRows();
    doTransform(chunk);
    if (rows_filtered)
        *rows_filtered += chunk_rows_before - chunk.getNumRows();
}

void FilterTransform::doTransform(Chunk & chunk)
{
    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    DataTypes types;
    auto select_final_indices_info = getSelectByFinalIndices(chunk);

    {
        Block block = getInputPort().getHeader().cloneWithColumns(columns);
        columns.clear();

        if (expression)
            expression->execute(block, num_rows_before_filtration);

        columns = block.getColumns();
        types = block.getDataTypes();
    }

    if (constant_filter_description.always_true || on_totals)
    {
        executeSelectByIndices(columns, select_final_indices_info, num_rows_before_filtration);
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    size_t num_columns = columns.size();
    ColumnPtr filter_column = columns[filter_column_position];

    /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
        *  and now - are calculated. That is, not all cases are covered by the code above.
        * This happens if the function returns a constant for a non-constant argument.
        * For example, `ignore` function.
        */
    constant_filter_description = ConstantFilterDescription(*filter_column);

    if (constant_filter_description.always_false)
        return; /// Will finish at next prepare call

    if (constant_filter_description.always_true)
    {
        executeSelectByIndices(columns, select_final_indices_info, num_rows_before_filtration);
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    std::unique_ptr<IFilterDescription> filter_description;
    if (filter_column->isSparse())
        filter_description = combineFilterAndIndices(
            std::make_unique<SparseFilterDescription>(*filter_column), select_final_indices_info, num_rows_before_filtration);
    else
        filter_description = combineFilterAndIndices(
            std::make_unique<FilterDescription>(*filter_column), select_final_indices_info, num_rows_before_filtration);


    if (!filter_description->has_one)
        return;

    /** Let's find out how many rows will be in result.
      * To do this, we filter out the first non-constant column
      *  or calculate number of set bytes in the filter.
      */
    size_t first_non_constant_column = num_columns;
    size_t min_size_in_memory = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < num_columns; ++i)
    {
        DataTypePtr type_not_null = removeNullableOrLowCardinalityNullable(types[i]);
        if (i != filter_column_position && !isColumnConst(*columns[i]) && type_not_null->isValueRepresentedByNumber())
        {
            size_t size_in_memory = type_not_null->getSizeOfValueInMemory() + (isNullableOrLowCardinalityNullable(types[i]) ? 1 : 0);
            if (size_in_memory < min_size_in_memory)
            {
                min_size_in_memory = size_in_memory;
                first_non_constant_column = i;
            }
        }
    }
    (void)min_size_in_memory; /// Suppress error of clang-analyzer-deadcode.DeadStores

    size_t num_filtered_rows = 0;
    if (first_non_constant_column != num_columns)
    {
        columns[first_non_constant_column] = filter_description->filter(*columns[first_non_constant_column], -1);
        num_filtered_rows = columns[first_non_constant_column]->size();
    }
    else
        num_filtered_rows = filter_description->countBytesInFilter();

    /// If the current block is completely filtered out, let's move on to the next one.
    if (num_filtered_rows == 0)
        /// SimpleTransform will skip it.
        return;

    /// If all the rows pass through the filter.
    if (num_filtered_rows == num_rows_before_filtration)
    {
        if (!remove_filter_column)
        {
            /// Replace the column with the filter by a constant.
            auto & type = transformed_header.getByPosition(filter_column_position).type;
            columns[filter_column_position] = type->createColumnConst(num_filtered_rows, 1u);
        }

        /// No need to touch the rest of the columns.
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    /// Filter the rest of the columns.
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & current_type = transformed_header.safeGetByPosition(i).type;
        auto & current_column = columns[i];

        if (i == filter_column_position)
        {
            /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
            /// NOTE User could pass column with something different than 0 and 1 for filter.
            /// Example:
            ///  SELECT materialize(100) AS x WHERE x
            /// will work incorrectly.
            current_column = current_type->createColumnConst(num_filtered_rows, 1u);
            continue;
        }

        if (i == first_non_constant_column)
            continue;

        if (isColumnConst(*current_column))
            current_column = current_column->cut(0, num_filtered_rows);
        else
            current_column = filter_description->filter(*current_column, num_filtered_rows);
    }

    chunk.setColumns(std::move(columns), num_filtered_rows);
    removeFilterIfNeed(chunk);
}


}
