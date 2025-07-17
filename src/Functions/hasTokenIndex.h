#pragma once

#include <algorithm>
#include <Columns/ColumnVector.h>
#include <Common/StringSearcher.h>
#include "Storages/MergeTree/MergeTreeIndices.h"
#include "base/defines.h"

#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>

#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <Interpreters/Context.h>

#include <Storages/MergeTree/MergeTreeIndexGin.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/SelectQueryInfo.h>
#include <iterator>
#include <print>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_INITIALIZED;
}

template <typename T>
concept FunctionIndexConcept = requires(T t)
{
    { T::name } -> std::convertible_to<std::string_view>;
    typename T::ResultType;
    //{ t.executeImplIndex() } -> std::same_as<typename T::ResultType>;

} && std::is_arithmetic_v<typename T::ResultType>;



template <FunctionIndexConcept Impl>
class FunctionIndex : public IFunction
{
    /// Helper function to extract the index and conditions safety.
    /// This performs all the checks and searches locally, so external code shouldn't check for them.
    static std::pair<const MergeTreeIndexPtr, const MergeTreeIndexConditionGin *> extractIndexAndCondition(
        const std::shared_ptr<const UsefulSkipIndexes> skip_indexes, String index_name)
    {
        auto it = std::ranges::find_if(
            skip_indexes->useful_indices,
            [index_name](const UsefulSkipIndexes::DataSkippingIndexAndCondition & index_and_condition)
            {
                return (index_and_condition.index->index.name == index_name);
            }
        );

        if (it == skip_indexes->useful_indices.end()) [[unlikely]]
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Index named: {} does not exist.", index_name);

        const MergeTreeIndexPtr index_helper = it->index;
        if (index_helper == nullptr) [[unlikely]]
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Index named: {} is registered as null.", index_name);

        if (it->index->index.type != "text") [[unlikely]]
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Index named: {} is not a text index.", index_name);

        const MergeTreeIndexConditionGin * gin_filter_condition = dynamic_cast<const MergeTreeIndexConditionGin *>(it->condition.get());
        if (gin_filter_condition == nullptr) [[unlikely]]
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Condition for text index {} is incorrect.", index_name);

        return {index_helper, gin_filter_condition};
    }

    template <typename T>
    const ColumnVector<T> * extractColumnAndCheck(const ColumnsWithTypeAndName & arguments, size_t pos) const
    {
        const ColumnVector<T> * col_index_vector = checkAndGetColumn<ColumnVector<T>>(arguments[pos].column.get());

        if (!col_index_vector) [[unlikely]]
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be {}.",
                arguments[pos].type->getName(),
                pos + 1,
                getName(),
                col_index_vector->getFamilyName());

        return col_index_vector;
    }



public:
    static constexpr auto name = Impl::name;
    using ResultType = typename Impl::ResultType;
    ContextPtr context;

    explicit FunctionIndex(ContextPtr _context)
        : context(_context)
    {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionIndex<Impl>>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeNumber<ResultType>>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.size() != getNumberOfArguments())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} expects at least 2 arguments", getName());

        const ColumnWithTypeAndName & index_argument = arguments[0];
        const ColumnWithTypeAndName & token_argument = arguments[1];
        const ColumnWithTypeAndName & part_index_argument = arguments[2];
        const ColumnWithTypeAndName & part_offset_argument = arguments[3];

        chassert(index_argument.column->size() == input_rows_count);
        chassert(token_argument.column->size() == input_rows_count);
        chassert(part_index_argument.column->size() == input_rows_count);
        chassert(part_offset_argument.column->size() == input_rows_count);

        auto col_res = ColumnVector<ResultType>::create(input_rows_count, ResultType());

        /// This is a not totally save method to ensure that this works.
        /// Apparently when input_rows_count == 0 the indexes are not constructed yet.
        /// This seems to happen when calling interpreter_with_analyzer->GetQueryPlan();
        if (input_rows_count == 0)
            return col_res;

        /// Remember that the skip_indexes_mutex is a hold mutex.
        auto const [skip_indexes, ranges_in_parts, skip_indexes_mutex] = context->getSkippingIndices();
        chassert(skip_indexes != nullptr);
        chassert(ranges_in_parts != nullptr);

        const String index_name = checkAndGetColumnConstStringOrFixedString(index_argument.column.get())->getValue<String>();
        const String token = checkAndGetColumnConstStringOrFixedString(token_argument.column.get())->getValue<String>();

        const ColumnUInt64 * col_part_index_vector = extractColumnAndCheck<UInt64>(arguments, 2);
        const ColumnUInt64 * col_part_offset_vector = extractColumnAndCheck<UInt64>(arguments, 3);

        // Find index and condition iterator
        const auto [index_helper, gin_filter_condition] = extractIndexAndCondition(skip_indexes, index_name);
        const std::shared_ptr<const GinFilter> filter = gin_filter_condition->getGinFilter(token);

        const size_t index_granularity = index_helper->index.granularity;

        // Now search the part
        size_t part_idx = col_part_index_vector->getElement(0);

        const auto & part_ranges = ranges_in_parts->at(part_idx);
        const DataPartPtr part = part_ranges.data_part;

        const UInt64 first_row = col_part_offset_vector->getElement(0);
        const UInt64 last_row = col_part_offset_vector->getElement(input_rows_count - 1);

        const size_t first_mark = part->index_granularity->getMarkRangeForRowOffset(first_row).begin;
        const size_t last_mark = part->index_granularity->getMarkRangeForRowOffset(last_row).begin;

        // std::println("Called HasTokenIndex({}, {}, [{}], [{} -> {}]) diff: {} input_rows: {}",
        //     index_name, token, part_idx, first_row, last_row, last_row - first_row, input_rows_count);

        // Check that we have the right boundary marks
        chassert(first_row == part->index_granularity->getMarkStartingRow(first_mark));
        chassert(last_row + 1 == part->index_granularity->getMarkStartingRow(last_mark) + part->index_granularity->getMarkRows(last_mark));
        const MarkRange full_input_range(first_mark, last_mark);

        // for (const auto & range : part_ranges.ranges)
        // {
        //     MarkRange index_range(
        //         range.begin / index_granularity,
        //         (range.end + index_granularity - 1) / index_granularity);
        //     index_ranges.push_back(index_range);
        // }

        MarkRanges index_ranges;
        for (const auto & range : part_ranges.ranges)
        {
            if (range.end < full_input_range.begin)
                continue;

            if (full_input_range.end < range.begin) /// The range is on the right.
                break;

            MarkRange index_range(
                range.begin / index_granularity,
                (range.end + index_granularity - 1) / index_granularity);
            index_ranges.push_back(index_range);
        }

        //PostingsCacheForStore cache_in_store(index_helper->getFileName(), part->getDataPartStoragePtr());
        auto [cache_in_store, cache_in_store_mutex] = context->getPostingsCacheForStore(part_idx, index_name);
        chassert(cache_in_store);

        const size_t marks_count = part->index_granularity->getMarksCountWithoutFinal();

        const size_t index_marks_count = (marks_count + index_granularity - 1) / index_granularity;

        MergeTreeIndexReader reader(
            index_helper, part,
            index_marks_count,
            index_ranges,
            context->getIndexMarkCache().get(),
            context->getIndexUncompressedCache().get(),
            context->getVectorSimilarityIndexCache().get(),
            MergeTreeReaderSettings::Create(context, SelectQueryInfo()));

        MergeTreeIndexGranulePtr granule = nullptr;
        size_t last_index_mark = 0;

        auto & vec_res = col_res->getData();

        size_t idx = 0;
        size_t offset = col_part_offset_vector->getElement(idx);

        for (const MarkRange& irange : index_ranges)
        {
            for (size_t index_mark = irange.begin; index_mark < irange.end; ++index_mark)
            {
                if (index_mark < first_mark)
                    continue;
                if (index_mark > last_mark)
                    break;

                const size_t mark_start = part->index_granularity->getMarkStartingRow(index_mark);
                const size_t mark_size = part->index_granularity->getMarkRows(index_mark);
                const size_t mark_end = mark_start + mark_size;

                chassert(mark_size > 0); /// yes let's be paranoiac.

                /// This mark FULLY overlaps with a hole in the offsets?
                if (mark_end < offset)
                    continue;

                if (index_mark != irange.begin || !granule || last_index_mark != irange.begin)
                    reader.read(index_mark, granule);

                const MergeTreeIndexGranuleGinPtr granule_gin = std::dynamic_pointer_cast<MergeTreeIndexGranuleGin>(granule);
                if (!granule_gin)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "GinFilter index condition got a granule with the wrong type.");

                const std::vector<uint32_t> matching_rows = granule_gin->gin_filter.getIndices(filter.get(), cache_in_store.get());

                /// col_part_offset_vector may have some "holes" but will be always strictly increasing, so in the worst case the distance
                /// to the next mark start will be smaller or equal to mark_size.
                /// If we detect a performance issue here, then we could use bisection as the array is sorted and the boundaries are
                /// known.
                size_t next_idx = idx;
                while(next_idx < input_rows_count && col_part_offset_vector->getElement(next_idx) < mark_end)
                    ++next_idx;

                /// Extra boundary check.
                chassert(col_part_offset_vector->getElement(next_idx - 1) >= (matching_rows.back() - 1));

                for (uint32_t row : matching_rows)
                {
                    const size_t match_offset = row - 1;

                    /// This is the same than an std::lower_bound, but simpler (and faster ;)
                    size_t lower_bound_offset = col_part_offset_vector->getElement(idx);
                    while (idx < next_idx && lower_bound_offset < match_offset)
                        lower_bound_offset = col_part_offset_vector->getElement(++idx);

                    if (idx == next_idx)
                        break;

                    chassert(lower_bound_offset >= match_offset);

                    if (lower_bound_offset == match_offset)
                    {
                        chassert(vec_res[idx] == 0);
                        vec_res[idx] = 1;
                    }
                    /// if (lower_bound_offset > match_offset) continue; /// there is a hole... just continue
                }

                idx = next_idx;
                offset = (next_idx < input_rows_count) ? col_part_offset_vector->getElement(next_idx) : 0;

                // if (matching_rows.size() > 0)
                // {
                //     std::println("  Mark: {}:  [{} : {}] diff: {} tokens: {}",
                //         index_mark,
                //         mark_start,
                //         mark_end,
                //         mark_size,
                //         matching_rows.size()
                //     );

                //     //counter += indices.size();
                // }
            }

            last_index_mark = irange.end - 1;
        }

        // RangesInDataParts parts_with_ranges = indexInfo.parts;

        // skip_indexes = indexes->skip_indexes
        // const auto & index_and_condition = skip_indexes.useful_indices[idx];

        // MergeTreeIndexPtr index = index_and_condition.index;
        // MergeTreeIndexConditionPtr condition = index_and_condition.condition;

        // auto & ranges = parts_with_ranges[part_index];

        // index_helper = index_and_condition.index;
        // part = ranges.data_part
        // ranges = ranges.ranges

        // for idx in skip_indexes

        return col_res;
    }
};


struct HasTokenIndexImpl
{
    static constexpr auto name = "hasTokenIndex";
    using ResultType = UInt8;

};


using FunctionHasTokenIndex = FunctionIndex<HasTokenIndexImpl>;

}
