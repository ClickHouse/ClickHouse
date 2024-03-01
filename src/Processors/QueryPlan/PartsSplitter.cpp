#include <algorithm>
#include <memory>
#include <numeric>
#include <queue>
#include <unordered_map>
#include <vector>

#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/Transforms/FilterSortedStreamByRange.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Common/FieldVisitorsAccurateComparison.h>

using namespace DB;

namespace
{

using Values = std::vector<Field>;

std::string toString(const Values & value)
{
    return fmt::format("({})", fmt::join(value, ", "));
}

int compareValues(const Values & lhs, const Values & rhs)
{
    chassert(lhs.size() == rhs.size());

    for (size_t i = 0; i < lhs.size(); ++i)
    {
        if (applyVisitor(FieldVisitorAccurateLess(), lhs[i], rhs[i]))
            return -1;

        if (!applyVisitor(FieldVisitorAccurateEquals(), lhs[i], rhs[i]))
            return 1;
    }

    return 0;
}

/// Adaptor to access PK values from index.
class IndexAccess
{
public:
    explicit IndexAccess(const RangesInDataParts & parts_) : parts(parts_) { }

    Values getValue(size_t part_idx, size_t mark) const
    {
        const auto & index = parts[part_idx].data_part->getIndex();
        Values values(index.size());
        for (size_t i = 0; i < values.size(); ++i)
        {
            index[i]->get(mark, values[i]);
            if (values[i].isNull())
                values[i] = POSITIVE_INFINITY;
        }
        return values;
    }

    std::optional<size_t> findRightmostMarkLessThanValueInRange(size_t part_index, Values value, size_t range_begin, size_t range_end) const
    {
        size_t left = range_begin;
        size_t right = range_end;

        while (left < right)
        {
            size_t middle = left + (right - left) / 2;
            int compare_result = compareValues(getValue(part_index, middle), value);
            if (compare_result != -1)
                right = middle;
            else
                left = middle + 1;
        }

        if (right == range_begin)
            return {};

        return right - 1;
    }

    std::optional<size_t> findRightmostMarkLessThanValueInRange(size_t part_index, Values value, MarkRange mark_range) const
    {
        return findRightmostMarkLessThanValueInRange(part_index, value, mark_range.begin, mark_range.end);
    }

    std::optional<size_t> findLeftmostMarkGreaterThanValueInRange(size_t part_index, Values value, size_t range_begin, size_t range_end) const
    {
        size_t left = range_begin;
        size_t right = range_end;

        while (left < right)
        {
            size_t middle = left + (right - left) / 2;
            int compare_result = compareValues(getValue(part_index, middle), value);
            if (compare_result != 1)
                left = middle + 1;
            else
                right = middle;
        }

        if (left == range_end)
            return {};

        return left;
    }

    std::optional<size_t> findLeftmostMarkGreaterThanValueInRange(size_t part_index, Values value, MarkRange mark_range) const
    {
        return findLeftmostMarkGreaterThanValueInRange(part_index, value, mark_range.begin, mark_range.end);
    }

    size_t getMarkRows(size_t part_idx, size_t mark) const { return parts[part_idx].data_part->index_granularity.getMarkRows(mark); }

private:
    const RangesInDataParts & parts;
};

class RangesInDataPartsBuilder
{
public:
    explicit RangesInDataPartsBuilder(const RangesInDataParts & initial_ranges_in_data_parts_) : initial_ranges_in_data_parts(initial_ranges_in_data_parts_) { }

    void addRange(size_t part_index, MarkRange mark_range)
    {
        auto [it, inserted] = part_index_to_current_ranges_in_data_parts_index.emplace(part_index, ranges_in_data_parts.size());

        if (inserted)
        {
            ranges_in_data_parts.emplace_back(
                initial_ranges_in_data_parts[part_index].data_part,
                initial_ranges_in_data_parts[part_index].alter_conversions,
                initial_ranges_in_data_parts[part_index].part_index_in_query,
                MarkRanges{mark_range});
            part_index_to_initial_ranges_in_data_parts_index[it->second] = part_index;
            return;
        }

        ranges_in_data_parts[it->second].ranges.push_back(mark_range);
    }

    RangesInDataParts & getCurrentRangesInDataParts()
    {
        return ranges_in_data_parts;
    }

private:
    std::unordered_map<size_t, size_t> part_index_to_current_ranges_in_data_parts_index;
    std::unordered_map<size_t, size_t> part_index_to_initial_ranges_in_data_parts_index;
    RangesInDataParts ranges_in_data_parts;
    const RangesInDataParts & initial_ranges_in_data_parts;
};

struct PartsRangesIterator
{
    enum class EventType : uint8_t
    {
        RangeStart = 0,
        RangeEnd,
    };

    [[maybe_unused]] bool operator<(const PartsRangesIterator & other) const
    {
        int compare_result = compareValues(value, other.value);
        if (compare_result == -1)
            return true;
        else if (compare_result == 1)
            return false;

        if (part_index == other.part_index)
        {
            /// Within the same part we should process events in order of mark numbers,
            /// because they already ordered by value and range ends have greater mark numbers than the beginnings.
            /// Otherwise we could get invalid ranges with the right bound that is less than the left bound.
            const auto ev_mark = event == EventType::RangeStart ? range.begin : range.end;
            const auto other_ev_mark = other.event == EventType::RangeStart ? other.range.begin : other.range.end;

            // Start event always before end event
            if (ev_mark == other_ev_mark)
                return event < other.event;

            return ev_mark < other_ev_mark;
        }

        if (event == other.event)
            return part_index < other.part_index;

        // Start event always before end event
        return event < other.event;
    }

    [[maybe_unused]] bool operator==(const PartsRangesIterator & other) const
    {
        if (value.size() != other.value.size())
            return false;

        for (size_t i = 0; i < value.size(); ++i)
            if (!applyVisitor(FieldVisitorAccurateEquals(), value[i], other.value[i]))
                return false;

        return range == other.range && part_index == other.part_index && event == other.event;
    }

    [[maybe_unused]] bool operator>(const PartsRangesIterator & other) const
    {
        if (operator<(other) || operator==(other))
            return false;

        return true;
    }

    Values value;
    MarkRange range;
    size_t part_index;
    EventType event;
};

struct SplitPartsRangesResult
{
    RangesInDataParts non_intersecting_parts_ranges;
    RangesInDataParts intersecting_parts_ranges;
};

SplitPartsRangesResult splitPartsRanges(RangesInDataParts ranges_in_data_parts, const LoggerPtr & logger)
{
    /** Split ranges in data parts into intersecting ranges in data parts and non intersecting ranges in data parts.
      *
      * For each marks range we will create 2 events (RangeStart, RangeEnd), add these events into array and sort them by primary key index
      * value at this event.
      *
      * After that we will scan sorted events and maintain current intersecting parts ranges.
      * If current intersecting parts ranges is 1, for each event (RangeStart, RangeEnd) we can extract non intersecting range
      * from single part range.
      *
      * There can be 4 possible cases:
      *
      * 1. RangeStart after RangeStart:
      *
      * Example:
      *
      * range 1 [----            ...
      * range 2      [(value_1)    ...
      *
      * In this scenario we can extract non intersecting part of range 1. This non intersecting part will have start
      * of range 1 and end with rightmost mark from range 1 that contains value less than value_1.
      *
      * 2. RangeStart after RangeEnd:
      *
      * Example:
      *
      * range 1   [              ----              ...
      * range 2   [   (value_1)]
      * range 3                      [(value_2)    ...
      *
      * In this case we can extract non intersecting part of range 1. This non intersecting part will have start
      * of leftmost mark from range 1 that contains value greater than value_1 and end with rightmost mark from range 1
      * that contains value less than value_2.
      *
      * 3. RangeEnd after RangeStart:
      *
      * Example:
      *
      * range 1   [----]
      *
      * In this case we can extract range 1 as non intersecting.
      *
      * 4. RangeEnd after RangeEnd
      *
      * Example:
      *
      * range 1    [    ...              ----]
      * range 2    [    ...    (value_1)]
      *
      * In this case we can extract non intersecting part of range 1. This non intersecting part will have start
      * of leftmost mark from range 1 that contains value greater than value_1 and end with range 1 end.
      *
      * Additional details:
      *
      * 1. If part level is 0, we must process all ranges from this part, because they can contain duplicate primary keys.
      * 2. If non intersecting range is small, it is better to not add it to non intersecting ranges, to avoid expensive seeks.
      */

    IndexAccess index_access(ranges_in_data_parts);
    std::vector<PartsRangesIterator> parts_ranges;

    for (size_t part_index = 0; part_index < ranges_in_data_parts.size(); ++part_index)
    {
        for (const auto & range : ranges_in_data_parts[part_index].ranges)
        {
            const auto & index_granularity = ranges_in_data_parts[part_index].data_part->index_granularity;
            parts_ranges.push_back(
                {index_access.getValue(part_index, range.begin), range, part_index, PartsRangesIterator::EventType::RangeStart});

            const bool value_is_defined_at_end_mark = range.end < index_granularity.getMarksCount();
            if (!value_is_defined_at_end_mark)
                continue;

            parts_ranges.push_back(
                {index_access.getValue(part_index, range.end), range, part_index, PartsRangesIterator::EventType::RangeEnd});
        }
    }

    std::sort(parts_ranges.begin(), parts_ranges.end());

    RangesInDataPartsBuilder intersecting_ranges_in_data_parts_builder(ranges_in_data_parts);
    RangesInDataPartsBuilder non_intersecting_ranges_in_data_parts_builder(ranges_in_data_parts);

    static constexpr size_t min_number_of_marks_for_non_intersecting_range = 2;

    auto add_non_intersecting_range = [&](size_t part_index, MarkRange mark_range)
    {
        non_intersecting_ranges_in_data_parts_builder.addRange(part_index, mark_range);
    };

    auto add_intersecting_range = [&](size_t part_index, MarkRange mark_range)
    {
        intersecting_ranges_in_data_parts_builder.addRange(part_index, mark_range);
    };

    std::unordered_map<size_t, MarkRange> part_index_start_to_range;

    chassert(!parts_ranges.empty());
    chassert(parts_ranges[0].event == PartsRangesIterator::EventType::RangeStart);
    part_index_start_to_range[parts_ranges[0].part_index] = parts_ranges[0].range;

    size_t parts_ranges_size = parts_ranges.size();
    for (size_t i = 1; i < parts_ranges_size; ++i)
    {
        auto & previous_part_range = parts_ranges[i - 1];
        auto & current_part_range = parts_ranges[i];
        size_t intersecting_parts = part_index_start_to_range.size();
        bool range_start = current_part_range.event == PartsRangesIterator::EventType::RangeStart;

        if (range_start)
        {
            auto [it, inserted] = part_index_start_to_range.emplace(current_part_range.part_index, current_part_range.range);
            chassert(inserted);

            if (intersecting_parts != 1)
                continue;

            if (previous_part_range.event == PartsRangesIterator::EventType::RangeStart)
            {
                /// If part level is 0, we must process whole previous part because it can contain duplicate primary keys
                if (ranges_in_data_parts[previous_part_range.part_index].data_part->info.level == 0)
                    continue;

                /// Case 1 Range Start after Range Start
                size_t begin = previous_part_range.range.begin;
                std::optional<size_t> end_optional = index_access.findRightmostMarkLessThanValueInRange(previous_part_range.part_index,
                    current_part_range.value,
                    previous_part_range.range);

                if (!end_optional)
                    continue;

                size_t end = *end_optional;

                if (end - begin >= min_number_of_marks_for_non_intersecting_range)
                {
                    part_index_start_to_range[previous_part_range.part_index].begin = end;
                    add_non_intersecting_range(previous_part_range.part_index, MarkRange{begin, end});
                }

                continue;
            }

            auto other_interval_it = part_index_start_to_range.begin();
            for (; other_interval_it != part_index_start_to_range.end(); ++other_interval_it)
            {
                if (other_interval_it != it)
                    break;
            }

            chassert(other_interval_it != part_index_start_to_range.end());
            size_t other_interval_part_index = other_interval_it->first;
            MarkRange other_interval_range = other_interval_it->second;

            /// If part level is 0, we must process whole other intersecting part because it can contain duplicate primary keys
            if (ranges_in_data_parts[other_interval_part_index].data_part->info.level == 0)
                continue;

            /// Case 2 Range Start after Range End
            std::optional<size_t> begin_optional = index_access.findLeftmostMarkGreaterThanValueInRange(other_interval_part_index,
                previous_part_range.value,
                other_interval_range);
            if (!begin_optional)
                continue;

            std::optional<size_t> end_optional = index_access.findRightmostMarkLessThanValueInRange(other_interval_part_index,
                current_part_range.value,
                other_interval_range);
            if (!end_optional)
                continue;

            size_t begin = *end_optional;
            size_t end = *end_optional;

            if (end - begin >= min_number_of_marks_for_non_intersecting_range)
            {
                other_interval_it->second.begin = end;
                add_intersecting_range(other_interval_part_index, MarkRange{other_interval_range.begin, begin});
                add_non_intersecting_range(other_interval_part_index, MarkRange{begin, end});
            }
            continue;
        }

        chassert(current_part_range.event == PartsRangesIterator::EventType::RangeEnd);

        /** If there are more than 1 part ranges that we are currently processing
          * that means that this part range is intersecting with other range.
          *
          * If part level is 0, we must process whole part because it can contain duplicate primary keys.
          */
        if (intersecting_parts != 1 || ranges_in_data_parts[current_part_range.part_index].data_part->info.level == 0)
        {
            add_intersecting_range(current_part_range.part_index, part_index_start_to_range[current_part_range.part_index]);
            part_index_start_to_range.erase(current_part_range.part_index);
            continue;
        }

        if (previous_part_range.event == PartsRangesIterator::EventType::RangeStart)
        {
            chassert(current_part_range.part_index == previous_part_range.part_index);
            chassert(current_part_range.range == previous_part_range.range);

            /// Case 3 Range End after Range Start
            non_intersecting_ranges_in_data_parts_builder.addRange(current_part_range.part_index, current_part_range.range);
            part_index_start_to_range.erase(current_part_range.part_index);
            continue;
        }

        chassert(previous_part_range.event == PartsRangesIterator::EventType::RangeEnd);
        chassert(previous_part_range.part_index != current_part_range.part_index);

        /// Case 4 Range End after Range End
        std::optional<size_t> begin_optional = index_access.findLeftmostMarkGreaterThanValueInRange(current_part_range.part_index,
            previous_part_range.value,
            current_part_range.range);
        size_t end = current_part_range.range.end;

        if (begin_optional && end - *begin_optional >= min_number_of_marks_for_non_intersecting_range)
        {
            size_t begin = *begin_optional;
            add_intersecting_range(current_part_range.part_index, MarkRange{part_index_start_to_range[current_part_range.part_index].begin, begin});
            add_non_intersecting_range(current_part_range.part_index, MarkRange{begin, end});
        }
        else
        {
            add_intersecting_range(current_part_range.part_index, MarkRange{part_index_start_to_range[current_part_range.part_index].begin, end});
        }

        part_index_start_to_range.erase(current_part_range.part_index);
    }

    /// Process parts ranges with undefined value at end mark
    bool is_intersecting = part_index_start_to_range.size() > 1;
    for (const auto & [part_index, mark_range] : part_index_start_to_range)
    {
        if (is_intersecting)
            add_intersecting_range(part_index, mark_range);
        else
            add_non_intersecting_range(part_index, mark_range);
    }

    auto && non_intersecting_ranges_in_data_parts = std::move(non_intersecting_ranges_in_data_parts_builder.getCurrentRangesInDataParts());
    auto && intersecting_ranges_in_data_parts = std::move(intersecting_ranges_in_data_parts_builder.getCurrentRangesInDataParts());

    std::stable_sort(
        non_intersecting_ranges_in_data_parts.begin(),
        non_intersecting_ranges_in_data_parts.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.part_index_in_query < rhs.part_index_in_query; });

    std::stable_sort(
        intersecting_ranges_in_data_parts.begin(),
        intersecting_ranges_in_data_parts.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.part_index_in_query < rhs.part_index_in_query; });

    LOG_TEST(logger, "Non intersecting ranges in data parts {}", non_intersecting_ranges_in_data_parts.getDescriptions().describe());
    LOG_TEST(logger, "Intersecting ranges in data parts {}", intersecting_ranges_in_data_parts.getDescriptions().describe());

    return {std::move(non_intersecting_ranges_in_data_parts), std::move(intersecting_ranges_in_data_parts)};
}

std::pair<std::vector<RangesInDataParts>, std::vector<Values>> splitIntersectingPartsRangesIntoLayers(RangesInDataParts intersecting_ranges_in_data_parts,
    size_t max_layers,
    const LoggerPtr & logger)
{
    // We will advance the iterator pointing to the mark with the smallest PK value until
    // there will be not less than rows_per_layer rows in the current layer (roughly speaking).
    // Then we choose the last observed value as the new border, so the current layer will consists
    // of granules with values greater than the previous mark and less or equal than the new border.

    IndexAccess index_access(intersecting_ranges_in_data_parts);
    std::priority_queue<PartsRangesIterator, std::vector<PartsRangesIterator>, std::greater<>> parts_ranges_queue;

    for (size_t part_index = 0; part_index < intersecting_ranges_in_data_parts.size(); ++part_index)
    {
        for (const auto & range : intersecting_ranges_in_data_parts[part_index].ranges)
        {
            const auto & index_granularity = intersecting_ranges_in_data_parts[part_index].data_part->index_granularity;
            parts_ranges_queue.push(
                {index_access.getValue(part_index, range.begin), range, part_index, PartsRangesIterator::EventType::RangeStart});

            const bool value_is_defined_at_end_mark = range.end < index_granularity.getMarksCount();
            if (!value_is_defined_at_end_mark)
                continue;

            parts_ranges_queue.push(
                {index_access.getValue(part_index, range.end), range, part_index, PartsRangesIterator::EventType::RangeEnd});
        }
    }

    /// The beginning of currently started (but not yet finished) range of marks of a part in the current layer.
    std::unordered_map<size_t, size_t> current_part_range_begin;
    /// The current ending of a range of marks of a part in the current layer.
    std::unordered_map<size_t, size_t> current_part_range_end;

    /// Determine borders between layers.
    std::vector<Values> borders;
    std::vector<RangesInDataParts> result_layers;

    size_t total_intersecting_rows_count = intersecting_ranges_in_data_parts.getRowsCountAllParts();
    const size_t rows_per_layer = std::max<size_t>(total_intersecting_rows_count / max_layers, 1);

    while (!parts_ranges_queue.empty())
    {
        // New layer should include last granules of still open ranges from the previous layer,
        // because they may already contain values greater than the last border.
        size_t rows_in_current_layer = 0;
        size_t marks_in_current_layer = 0;

        // Intersection between the current and next layers is just the last observed marks of each still open part range. Ratio is empirical.
        auto layers_intersection_is_too_big = [&]()
        {
            const auto intersected_parts = current_part_range_end.size();
            return marks_in_current_layer < intersected_parts * 2;
        };

        RangesInDataPartsBuilder current_layer_builder(intersecting_ranges_in_data_parts);
        result_layers.emplace_back();

        while (rows_in_current_layer < rows_per_layer || layers_intersection_is_too_big() || result_layers.size() == max_layers)
        {
            // We're advancing iterators until a new value showed up.
            Values last_value;
            while (!parts_ranges_queue.empty() && (last_value.empty() || last_value == parts_ranges_queue.top().value))
            {
                auto current = parts_ranges_queue.top();
                parts_ranges_queue.pop();
                const auto part_index = current.part_index;

                if (current.event == PartsRangesIterator::EventType::RangeEnd)
                {
                    current_layer_builder.addRange(part_index, MarkRange{current_part_range_begin[part_index], current.range.end});
                    current_part_range_begin.erase(part_index);
                    current_part_range_end.erase(part_index);
                    continue;
                }

                last_value = std::move(current.value);
                rows_in_current_layer += index_access.getMarkRows(part_index, current.range.begin);
                ++marks_in_current_layer;

                current_part_range_begin.try_emplace(part_index, current.range.begin);
                current_part_range_end[part_index] = current.range.begin;

                if (current.range.begin + 1 < current.range.end)
                {
                    ++current.range.begin;
                    current.value = index_access.getValue(part_index, current.range.begin);
                    parts_ranges_queue.push(std::move(current));
                }
            }

            if (parts_ranges_queue.empty())
                break;

            if (rows_in_current_layer >= rows_per_layer && !layers_intersection_is_too_big() && result_layers.size() < max_layers)
                borders.push_back(last_value);
        }

        for (const auto & [part_index, last_mark] : current_part_range_end)
        {
            current_layer_builder.addRange(part_index, MarkRange{current_part_range_begin[part_index], last_mark + 1});
            current_part_range_begin[part_index] = current_part_range_end[part_index];
        }

        result_layers.back() = std::move(current_layer_builder.getCurrentRangesInDataParts());
    }

    size_t result_layers_size = result_layers.size();
    LOG_TEST(logger, "Split intersecting ranges into {} layers", result_layers_size);

    for (size_t i = 0; i < result_layers_size; ++i)
    {
        auto & layer = result_layers[i];

        LOG_TEST(logger, "Layer {} {} filter values in ({}, {}])",
            i,
            layer.getDescriptions().describe(),
            i ? ::toString(borders[i - 1]) : "-inf", i < borders.size() ? ::toString(borders[i]) : "+inf");

        std::stable_sort(
            layer.begin(),
            layer.end(),
            [](const auto & lhs, const auto & rhs) { return lhs.part_index_in_query < rhs.part_index_in_query; });
    }

    return {std::move(result_layers), std::move(borders)};
}


/// Will return borders.size()+1 filters in total, i-th filter will accept rows with PK values within the range (borders[i-1], borders[i]].
ASTs buildFilters(const KeyDescription & primary_key, const std::vector<Values> & borders)
{
    auto add_and_condition = [&](ASTPtr & result, const ASTPtr & foo) { result = (!result) ? foo : makeASTFunction("and", result, foo); };

    /// Produces ASTPtr to predicate (pk_col0, pk_col1, ... , pk_colN) > (value[0], value[1], ... , value[N]), possibly with conversions.
    /// For example, if table PK is (a, toDate(d)), where `a` is UInt32 and `d` is DateTime, and PK columns values are (8192, 19160),
    /// it will build the following predicate: greater(tuple(a, toDate(d)), tuple(8192, cast(19160, 'Date'))).
    auto lexicographically_greater = [&](const Values & values) -> ASTPtr
    {
        ASTs pks_ast;
        ASTs values_ast;
        for (size_t i = 0; i < values.size(); ++i)
        {
            const auto & type = primary_key.data_types.at(i);

            // PK may contain functions of the table columns, so we need the actual PK AST with all expressions it contains.
            auto pk_ast = primary_key.expression_list_ast->children.at(i);

            // If PK is nullable, prepend a null mask column for > comparison.
            // Also transform the AST into assumeNotNull(pk) so that the result type is not-nullable.
            if (type->isNullable())
            {
                pks_ast.push_back(makeASTFunction("isNull", pk_ast));
                values_ast.push_back(std::make_shared<ASTLiteral>(values[i].isNull() ? 1 : 0));
                pk_ast = makeASTFunction("assumeNotNull", pk_ast);
            }

            pks_ast.push_back(pk_ast);

            // If value is null, the comparison is already complete by looking at the null mask column.
            // Here we put the pk_ast as a placeholder: (pk_null_mask, pk_ast_not_null) > (value_is_null?, pk_ast_not_null).
            if (values[i].isNull())
            {
                values_ast.push_back(pk_ast);
            }
            else
            {
                ASTPtr component_ast = std::make_shared<ASTLiteral>(values[i]);
                auto decayed_type = removeNullable(removeLowCardinality(primary_key.data_types.at(i)));
                // Values of some types (e.g. Date, DateTime) are stored in columns as numbers and we get them as just numbers from the index.
                // So we need an explicit Cast for them.
                if (isColumnedAsNumber(decayed_type->getTypeId()) && !isNumber(decayed_type->getTypeId()))
                    component_ast = makeASTFunction("cast", std::move(component_ast), std::make_shared<ASTLiteral>(decayed_type->getName()));

                values_ast.push_back(std::move(component_ast));
            }
        }

        ASTPtr pk_columns_as_tuple = makeASTFunction("tuple", pks_ast);
        ASTPtr values_as_tuple = makeASTFunction("tuple", values_ast);

        return makeASTFunction("greater", pk_columns_as_tuple, values_as_tuple);
    };

    ASTs filters(borders.size() + 1);
    for (size_t layer = 0; layer <= borders.size(); ++layer)
    {
        if (layer > 0)
            add_and_condition(filters[layer], lexicographically_greater(borders[layer - 1]));
        if (layer < borders.size())
            add_and_condition(filters[layer], makeASTFunction("not", lexicographically_greater(borders[layer])));
    }
    return filters;
}
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void reorderColumns(ActionsDAG & dag, const Block & header, const std::string & filter_column)
{
    std::unordered_map<std::string_view, const ActionsDAG::Node *> inputs_map;
    for (const auto * input : dag.getInputs())
        inputs_map[input->result_name] = input;

    for (const auto & col : header)
    {
        auto & input = inputs_map[col.name];
        if (!input)
            input = &dag.addInput(col);
    }

    ActionsDAG::NodeRawConstPtrs new_outputs;
    new_outputs.reserve(header.columns() + 1);

    new_outputs.push_back(&dag.findInOutputs(filter_column));
    for (const auto & col : header)
    {
        auto & input = inputs_map[col.name];
        new_outputs.push_back(input);
    }

    dag.getOutputs() = std::move(new_outputs);
}

SplitPartsWithRangesByPrimaryKeyResult splitPartsWithRangesByPrimaryKey(
    const KeyDescription & primary_key,
    ExpressionActionsPtr sorting_expr,
    RangesInDataParts parts,
    size_t max_layers,
    ContextPtr context,
    ReadingInOrderStepGetter && in_order_reading_step_getter,
    bool split_parts_ranges_into_intersecting_and_non_intersecting_final,
    bool split_intersecting_parts_ranges_into_layers)
{
    if (max_layers <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "max_layer should be greater than 1");

    auto logger = getLogger("PartsSplitter");

    SplitPartsWithRangesByPrimaryKeyResult result;

    RangesInDataParts intersecting_parts_ranges = std::move(parts);

    if (split_parts_ranges_into_intersecting_and_non_intersecting_final)
    {
        SplitPartsRangesResult split_result = splitPartsRanges(intersecting_parts_ranges, logger);
        result.non_intersecting_parts_ranges = std::move(split_result.non_intersecting_parts_ranges);
        intersecting_parts_ranges = std::move(split_result.intersecting_parts_ranges);
    }

    if (!split_intersecting_parts_ranges_into_layers)
    {
        result.merging_pipes.emplace_back(in_order_reading_step_getter(intersecting_parts_ranges));
        return result;
    }

    auto && [layers, borders] = splitIntersectingPartsRangesIntoLayers(intersecting_parts_ranges, max_layers, logger);
    auto filters = buildFilters(primary_key, borders);
    result.merging_pipes.resize(layers.size());

    for (size_t i = 0; i < layers.size(); ++i)
    {
        result.merging_pipes[i] = in_order_reading_step_getter(std::move(layers[i]));
        result.merging_pipes[i].addSimpleTransform([sorting_expr](const Block & header)
                                    { return std::make_shared<ExpressionTransform>(header, sorting_expr); });

        auto & filter_function = filters[i];
        if (!filter_function)
            continue;

        auto syntax_result = TreeRewriter(context).analyze(filter_function, primary_key.expression->getRequiredColumnsWithTypes());
        auto actions = ExpressionAnalyzer(filter_function, syntax_result, context).getActionsDAG(false);
        reorderColumns(*actions, result.merging_pipes[i].getHeader(), filter_function->getColumnName());
        ExpressionActionsPtr expression_actions = std::make_shared<ExpressionActions>(std::move(actions));
        auto description = fmt::format(
            "filter values in ({}, {}]", i ? ::toString(borders[i - 1]) : "-inf", i < borders.size() ? ::toString(borders[i]) : "+inf");
        result.merging_pipes[i].addSimpleTransform(
            [&](const Block & header)
            {
                auto step = std::make_shared<FilterSortedStreamByRange>(header, expression_actions, filter_function->getColumnName(), true);
                step->setDescription(description);
                return step;
            });
    }

    return result;
}

}
