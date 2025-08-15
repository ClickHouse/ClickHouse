#pragma once

#include <algorithm>
#include <Columns/ColumnVector.h>
#include <Common/StringSearcher.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <base/defines.h>

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
#include <Interpreters/IndexContextInfo.h>

#include <Storages/MergeTree/MergeTreeIndexGin.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/SelectQueryInfo.h>
#include <algorithm>
#include <Functions/FunctionsStringSearchBase.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}


template <typename T>
concept FunctionIndexConcept = requires(T t)
{
    { T::name } -> std::convertible_to<std::string_view>;
    typename T::ResultType;
    //{ t.executeImplIndex() } -> std::same_as<typename T::ResultType>;

} && std::is_arithmetic_v<typename T::ResultType>;


template <FunctionIndexConcept Impl>
class FunctionSearchTextIndex : public FunctionsStringSearchBase
{
    /// Helper function to extract the index and conditions safely.
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


    static void postingArrayToOutput(
        const std::vector<UInt32> matching_rows,
        const ColumnVector<UInt64> * col_part_offset_vector,
        PaddedPODArray<typename Impl::ResultType> & result)
    {
        chassert(!matching_rows.empty());
        const PaddedPODArray<UInt64> & offsets = col_part_offset_vector->getData();

        const UInt64 * it = std::lower_bound(offsets.begin(), offsets.end(), matching_rows.front() - 1);
        chassert(it != offsets.end());

        const UInt64 * end_it = std::upper_bound(it, offsets.end(), matching_rows.back());

        for (UInt32 row : matching_rows)
        {
            const size_t match_offset = row - 1;

            if (*it > match_offset)
                continue;

            if (*it < match_offset)
                it = std::lower_bound(it, end_it, match_offset);

            if (it == end_it)
                break;

            if (*it == match_offset)
            {
                const size_t idx = std::distance(offsets.begin(), it);
                chassert(result[idx] == 0);
                result[idx] = 1;
                std::advance(it, 1);

                if (it == end_it)
                    break;

            }
        }
    }


public:
    static constexpr auto name = Impl::name;
    using ResultType = typename Impl::ResultType;

    explicit FunctionSearchTextIndex(ContextPtr _context)
        : FunctionsStringSearchBase(_context, FunctionsStringSearchBase::Info::Optimized)
    {}

    static FunctionPtr create(ContextPtr _context) { return std::make_shared<FunctionSearchTextIndex<Impl>>(_context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeNumber<ResultType>>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        // Early exits
        if (arguments.size() != getNumberOfArguments())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} expects at least 2 arguments", getName());

        auto col_res = ColumnVector<ResultType>::create(input_rows_count, ResultType());

        /// This is a not totally save method to ensure that this works.
        /// Apparently when input_rows_count == 0 the indexes are not constructed yet.
        /// This seems to happen when calling interpreter_with_analyzer->GetQueryPlan();
        if (input_rows_count == 0)
            return col_res;

        // Read inputs
        const ColumnWithTypeAndName & index_argument = arguments[0];
        const ColumnWithTypeAndName & token_argument = arguments[1];
        const ColumnWithTypeAndName & part_index_argument = arguments[2];
        const ColumnWithTypeAndName & part_offset_argument = arguments[3];

        chassert(index_argument.column->size() == input_rows_count);
        chassert(token_argument.column->size() == input_rows_count);
        chassert(part_index_argument.column->size() == input_rows_count);
        chassert(part_offset_argument.column->size() == input_rows_count);

        // parse inputs
        const String index_name = checkAndGetColumnConstStringOrFixedString(index_argument.column.get())->getValue<String>();
        const String token = checkAndGetColumnConstStringOrFixedString(token_argument.column.get())->getValue<String>();
        const ColumnVector<UInt64> * col_part_index_vector = extractColumnAndCheck<UInt64>(arguments, 2);
        const ColumnVector<UInt64> * col_part_offset_vector = extractColumnAndCheck<UInt64>(arguments, 3);

        // Now search the part
        const size_t part_idx = col_part_index_vector->getElement(0);
        chassert(part_idx == col_part_index_vector->getElement(input_rows_count - 1)); // We assume to work on one part at the time

        /// Remember that the index_info_shared_lock is a shared mutex hold from here up to function end
        auto [index_context_info, index_info_shared_lock] = context->getIndexInfo();

        const IndexContextInfo::PartInfo &part_info = index_context_info->part_info_vector[part_idx].value();
        const std::shared_ptr<const PostingsCacheForStore> cache_in_store = part_info.postings_cache_for_store_part.at(index_name);
        chassert(cache_in_store);

        // Find index and condition iterator
        const auto [index_helper, gin_filter_condition] = extractIndexAndCondition(index_context_info->skip_indexes, index_name);

        const size_t index_granularity = index_helper->index.granularity;
        const std::shared_ptr<const GinFilter> filter = gin_filter_condition->getGinFilter(token);

        const DataPartPtr part = part_info.data_part;

        const UInt64 first_row = col_part_offset_vector->getElement(0);
        const UInt64 last_row = col_part_offset_vector->getElement(input_rows_count - 1);

        const size_t first_mark = part->index_granularity->getMarkRangeForRowOffset(first_row).begin;
        const size_t last_mark = part->index_granularity->getMarkRangeForRowOffset(last_row).begin;

        // std::println("\nCalled HasTokenIndex({}, {}, [{}: {} {}], [{} -> {}]) diff: {} input_rows: {}",
        //     index_name, token, part_idx, first_mark, last_mark, first_row, last_row, last_row - first_row, input_rows_count);

        // Check that we have the right boundary marks
        chassert(first_row >= part->index_granularity->getMarkStartingRow(first_mark));
        chassert(last_row < part->index_granularity->getMarkStartingRow(last_mark) + part->index_granularity->getMarkRows(last_mark));

        size_t idx = 0;
        MarkRanges index_ranges;
        std::map<size_t, MarkRanges> ranges_map;

        for (const auto & range : part_info.ranges)
        {
            /// For the reader
            index_ranges.emplace_back(
                range.begin / index_granularity,
                (range.end + index_granularity - 1) / index_granularity);

            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                if (mark < first_mark)
                    continue;
                if (mark > last_mark)
                    break;

                const size_t mark_start = part->index_granularity->getMarkStartingRow(mark);
                const size_t mark_size = part->index_granularity->getMarkRows(mark);
                const size_t mark_end = mark_start + mark_size;

                /// This mark is not consecutive with the last one because some intermediate ones were filtered/skipped
                /// In that case we assume that there was not a match in that mark and go on
                size_t offset = (idx < input_rows_count) ? col_part_offset_vector->getElement(idx) : last_row + 1;
                while (offset < mark_start)
                    offset = col_part_offset_vector->getElement(++idx);

                /// This mark FULLY overlaps with a hole in the offsets?
                if (offset > mark_end)
                    continue;

                const size_t current_index_mark = mark / index_granularity;

                ranges_map[current_index_mark].attachOrMergeLastRange(MarkRange(mark_start, mark_end));
            }
        }

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

        for (const auto &[index_mark, subranges] : ranges_map)
        {
            MergeTreeIndexGranulePtr granule = nullptr;
            reader.read(index_mark, granule);

            const MergeTreeIndexGranuleGinPtr granule_gin = std::dynamic_pointer_cast<MergeTreeIndexGranuleGin>(granule);
            if (!granule_gin)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "GinFilter index condition got a granule with the wrong type.");

            const std::vector<UInt32> matching_rows
                = granule_gin->gin_filter.getIndices(filter.get(), cache_in_store.get(), subranges);

            if (!matching_rows.empty())
                postingArrayToOutput(matching_rows, col_part_offset_vector, col_res->getData());
        }

        return col_res;
    }
};


struct HasTokenIndexImpl
{
    static constexpr auto name = "_hasToken_index";
    using ResultType = UInt8;

};


using FunctionHasTokenIndex = FunctionSearchTextIndex<HasTokenIndexImpl>;

}
