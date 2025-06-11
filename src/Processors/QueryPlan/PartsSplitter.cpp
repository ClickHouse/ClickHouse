#include <Processors/QueryPlan/PartsSplitter.h>

#include <Core/Field.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterSortedStreamByRange.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Common/FieldAccurateComparison.h>

#include <boost/functional/hash.hpp>

#include <fmt/ranges.h>

using namespace DB;

namespace
{

using Values = std::vector<Field>;

std::string toString(const Values & value)
{
    return fmt::format("({})", fmt::join(value, ", "));
}

/** We rely that FieldVisitorAccurateLess will have strict weak ordering for any Field values including
  * NaN, Null and containers (Array, Tuple, Map) that contain NaN or Null. But right now it does not properly
  * support NaN and Nulls inside containers, because it uses Field operator< or accurate::lessOp for comparison
  * that compares Nulls and NaNs differently than FieldVisitorAccurateLess.
  * TODO: Update Field operator< to compare NaNs and Nulls the same way as FieldVisitorAccurateLess.
  */
bool isSafePrimaryDataKeyType(const IDataType & data_type)
{
    auto type_id = data_type.getTypeId();
    switch (type_id)
    {
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::BFloat16:
        case TypeIndex::Nullable:
        case TypeIndex::ObjectDeprecated:
        case TypeIndex::Object:
        case TypeIndex::Variant:
        case TypeIndex::Dynamic:
            return false;
        case TypeIndex::Array:
        {
            const auto & data_type_array = static_cast<const DataTypeArray &>(data_type);
            return isSafePrimaryDataKeyType(*data_type_array.getNestedType());
        }
        case TypeIndex::Tuple:
        {
            const auto & data_type_tuple = static_cast<const DataTypeTuple &>(data_type);
            const auto & data_type_tuple_elements = data_type_tuple.getElements();
            for (const auto & data_type_tuple_element : data_type_tuple_elements)
                if (!isSafePrimaryDataKeyType(*data_type_tuple_element))
                    return false;

            return true;
        }
        case TypeIndex::LowCardinality:
        {
            const auto & data_type_low_cardinality = static_cast<const DataTypeLowCardinality &>(data_type);
            return isSafePrimaryDataKeyType(*data_type_low_cardinality.getDictionaryType());
        }
        case TypeIndex::Map:
        {
            const auto & data_type_map = static_cast<const DataTypeMap &>(data_type);
            return isSafePrimaryDataKeyType(*data_type_map.getKeyType()) && isSafePrimaryDataKeyType(*data_type_map.getValueType());
        }
        default:
        {
            break;
        }
    }

    return true;
}

bool isSafePrimaryKey(const KeyDescription & primary_key)
{
    for (const auto & type : primary_key.data_types)
    {
        if (!isSafePrimaryDataKeyType(*type))
            return false;
    }

    return true;
}

int compareValues(const Values & lhs, const Values & rhs, bool in_reverse_order)
{
    size_t size = std::min(lhs.size(), rhs.size());

    if (in_reverse_order)
    {
        for (size_t i = 0; i < size; ++i)
        {
            if (accurateLess(rhs[i], lhs[i]))
                return -1;

            if (!accurateEquals(rhs[i], lhs[i]))
                return 1;
        }
    }
    else
    {
        for (size_t i = 0; i < size; ++i)
        {
            if (accurateLess(lhs[i], rhs[i]))
                return -1;

            if (!accurateEquals(lhs[i], rhs[i]))
                return 1;
        }
    }

    return 0;
}

/// Adaptor to access PK values from index.
class IndexAccess
{
public:
    explicit IndexAccess(const RangesInDataParts & parts_, size_t max_columns_in_index = std::numeric_limits<size_t>::max())
        : parts(parts_), loaded_columns(max_columns_in_index)
    {
        /// Indices might be reloaded during the process and the reload might produce a different value
        /// (change in `primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns`). Also, some suffix of index
        /// columns might not be loaded (same setting) so we keep a reference to the current indices and
        /// track the minimal subset of loaded columns across all parts.
        indices.reserve(parts.size());
        for (const auto & part : parts)
            indices.push_back(part.data_part->getIndex());

        for (const auto & index : indices)
            loaded_columns = std::min(loaded_columns, index->size());
    }

    Values getValue(size_t part_idx, size_t mark) const
    {
        const auto & index = indices[part_idx];
        chassert(index->size() >= loaded_columns);
        Values values(loaded_columns);
        for (size_t i = 0; i < loaded_columns; ++i)
        {
            index->at(i)->get(mark, values[i]);
            if (values[i].isNull())
                values[i] = POSITIVE_INFINITY;
        }
        return values;
    }

    std::optional<size_t> findRightmostMarkLessThanValueInRange(
        size_t part_index, Values value, size_t range_begin, size_t range_end, bool in_reverse_order) const
    {
        size_t left = range_begin;
        size_t right = range_end;

        while (left < right)
        {
            size_t middle = left + (right - left) / 2;
            int compare_result = compareValues(getValue(part_index, middle), value, in_reverse_order);
            if (compare_result != -1)
                right = middle;
            else
                left = middle + 1;
        }

        if (right == range_begin)
            return {};

        return right - 1;
    }

    std::optional<size_t>
    findRightmostMarkLessThanValueInRange(size_t part_index, Values value, MarkRange mark_range, bool in_reverse_order) const
    {
        return findRightmostMarkLessThanValueInRange(part_index, value, mark_range.begin, mark_range.end, in_reverse_order);
    }

    std::optional<size_t> findLeftmostMarkGreaterThanValueInRange(
        size_t part_index, Values value, size_t range_begin, size_t range_end, bool in_reverse_order) const
    {
        size_t left = range_begin;
        size_t right = range_end;

        while (left < right)
        {
            size_t middle = left + (right - left) / 2;
            int compare_result = compareValues(getValue(part_index, middle), value, in_reverse_order);
            if (compare_result != 1)
                left = middle + 1;
            else
                right = middle;
        }

        if (left == range_end)
            return {};

        return left;
    }

    std::optional<size_t>
    findLeftmostMarkGreaterThanValueInRange(size_t part_index, Values value, MarkRange mark_range, bool in_reverse_order) const
    {
        return findLeftmostMarkGreaterThanValueInRange(part_index, value, mark_range.begin, mark_range.end, in_reverse_order);
    }

    size_t getMarkRows(size_t part_idx, size_t mark) const
    {
        return parts[part_idx].data_part->index_granularity->getMarkRows(mark);
    }
private:
    const RangesInDataParts & parts;
    std::vector<IMergeTreeDataPart::IndexPtr> indices;
    size_t loaded_columns;
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
                initial_ranges_in_data_parts[part_index].parent_part,
                initial_ranges_in_data_parts[part_index].part_index_in_query,
                initial_ranges_in_data_parts[part_index].part_starting_offset_in_query,
                MarkRanges{mark_range});
            part_index_to_initial_ranges_in_data_parts_index[it->second] = part_index;
            return;
        }

        if (ranges_in_data_parts[it->second].ranges.back().end == mark_range.begin)
            ranges_in_data_parts[it->second].ranges.back().end = mark_range.end;
        else
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
        int compare_result = compareValues(value, other.value, in_reverse_order);
        if (compare_result == -1)
            return true;
        if (compare_result == 1)
            return false;

        if (event == other.event)
        {
            if (!selected && other.selected)
                return true;
            if (selected && !other.selected)
                return false;
            if (part_index == other.part_index)
            {
                /// Within the same part we should process events in order of mark numbers,
                /// because they already ordered by value and range ends have greater mark numbers than the beginnings.
                /// Otherwise we could get invalid ranges with the right bound that is less than the left bound.
                const auto ev_mark = event == EventType::RangeStart ? range.begin : range.end;
                const auto other_ev_mark = other.event == EventType::RangeStart ? other.range.begin : other.range.end;
                return ev_mark < other_ev_mark;
            }

            return part_index < other.part_index;
        }

        // Start event always before end event
        return event < other.event;
    }

    [[maybe_unused]] bool operator==(const PartsRangesIterator & other) const
    {
        if (value.size() != other.value.size())
            return false;

        for (size_t i = 0; i < value.size(); ++i)
            if (!accurateEquals(value[i], other.value[i]))
                return false;

        return range == other.range && part_index == other.part_index && event == other.event;
    }

    [[maybe_unused]] bool operator>(const PartsRangesIterator & other) const
    {
        if (operator<(other) || operator==(other))
            return false;

        return true;
    }

    void dump(WriteBuffer & buffer) const
    {
        buffer << "Part index " << part_index;
        buffer << " event " << (event == PartsRangesIterator::EventType::RangeStart ? "Range Start" : "Range End");
        buffer << " range begin " << range.begin;
        buffer << " end " << range.end;
        buffer << " value " << ::toString(value) << '\n';
    }

    [[maybe_unused]] String toString() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);
        return buffer.str();
    }

    Values value;
    bool in_reverse_order;
    MarkRange range;
    size_t part_index;
    EventType event;
    bool selected; /// Whether this range was selected or rejected in skip index filtering
};

struct PartRangeIndex
{
    explicit PartRangeIndex(PartsRangesIterator & ranges_iterator)
        : part_index(ranges_iterator.part_index)
        , range(ranges_iterator.range)
    {}

    bool operator==(const PartRangeIndex & other) const
    {
        return std::tie(part_index, range.begin, range.end) == std::tie(other.part_index, other.range.begin, other.range.end);
    }

    bool operator<(const PartRangeIndex & other) const
    {
        return std::tie(part_index, range.begin, range.end) < std::tie(other.part_index, other.range.begin, other.range.end);
    }

    size_t part_index;
    MarkRange range;
};

struct PartRangeIndexHash
{
    size_t operator()(const PartRangeIndex & part_range_index) const noexcept
    {
        size_t result = 0;

        boost::hash_combine(result, part_range_index.part_index);
        boost::hash_combine(result, part_range_index.range.begin);
        boost::hash_combine(result, part_range_index.range.end);

        return result;
    }
};

struct SplitPartsRangesResult
{
    RangesInDataParts non_intersecting_parts_ranges;
    RangesInDataParts intersecting_parts_ranges;
};

void dump(const std::vector<PartsRangesIterator> & ranges_iterators, WriteBuffer & buffer)
{
    for (const auto & range_iterator : ranges_iterators)
        range_iterator.dump(buffer);
}

String toString(const std::vector<PartsRangesIterator> & ranges_iterators)
{
    WriteBufferFromOwnString buffer;
    dump(ranges_iterators, buffer);
    return buffer.str();
}

SplitPartsRangesResult splitPartsRanges(RangesInDataParts ranges_in_data_parts, bool in_reverse_order, const LoggerPtr & logger)
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
                {index_access.getValue(part_index, range.begin),
                 in_reverse_order,
                 range,
                 part_index,
                 PartsRangesIterator::EventType::RangeStart,
                 false});

            const bool value_is_defined_at_end_mark = range.end < index_granularity->getMarksCount();
            if (!value_is_defined_at_end_mark)
                continue;

            parts_ranges.push_back(
                {index_access.getValue(part_index, range.end),
                 in_reverse_order,
                 range,
                 part_index,
                 PartsRangesIterator::EventType::RangeEnd,
                 false});
        }
    }

    LOG_TEST(logger, "Parts ranges before sort {}", toString(parts_ranges));

    ::sort(parts_ranges.begin(), parts_ranges.end());

    LOG_TEST(logger, "Parts ranges after sort {}", toString(parts_ranges));

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

    std::unordered_map<PartRangeIndex, MarkRange, PartRangeIndexHash> part_index_start_to_range;

    chassert(!parts_ranges.empty());
    chassert(parts_ranges[0].event == PartsRangesIterator::EventType::RangeStart);
    part_index_start_to_range[PartRangeIndex(parts_ranges[0])] = parts_ranges[0].range;

    size_t parts_ranges_size = parts_ranges.size();
    for (size_t i = 1; i < parts_ranges_size; ++i)
    {
        auto & previous_part_range = parts_ranges[i - 1];
        PartRangeIndex previous_part_range_index(previous_part_range);
        auto & current_part_range = parts_ranges[i];
        PartRangeIndex current_part_range_index(current_part_range);
        size_t intersecting_parts = part_index_start_to_range.size();
        bool range_start = current_part_range.event == PartsRangesIterator::EventType::RangeStart;

        if (range_start)
        {
            auto [it, inserted] = part_index_start_to_range.emplace(current_part_range_index, current_part_range.range);
            if (!inserted)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "PartsSplitter expected unique range");

            if (intersecting_parts != 1)
                continue;

            if (previous_part_range.event == PartsRangesIterator::EventType::RangeStart)
            {
                /// If part level is 0, we must process whole previous part because it can contain duplicate primary keys
                if (ranges_in_data_parts[previous_part_range.part_index].data_part->info.level == 0)
                    continue;

                /// Case 1 Range Start after Range Start
                size_t begin = previous_part_range.range.begin;
                std::optional<size_t> end_optional = index_access.findRightmostMarkLessThanValueInRange(
                    previous_part_range.part_index, current_part_range.value, previous_part_range.range, in_reverse_order);

                if (!end_optional)
                    continue;

                size_t end = *end_optional;

                if (end - begin >= min_number_of_marks_for_non_intersecting_range)
                {
                    part_index_start_to_range[previous_part_range_index].begin = end;
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

            if (!(other_interval_it != part_index_start_to_range.end() && other_interval_it != it))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "PartsSplitter expected single other interval");

            size_t other_interval_part_index = other_interval_it->first.part_index;
            MarkRange other_interval_range = other_interval_it->second;

            /// If part level is 0, we must process whole other intersecting part because it can contain duplicate primary keys
            if (ranges_in_data_parts[other_interval_part_index].data_part->info.level == 0)
                continue;

            /// Case 2 Range Start after Range End
            std::optional<size_t> begin_optional = index_access.findLeftmostMarkGreaterThanValueInRange(
                other_interval_part_index, previous_part_range.value, other_interval_range, in_reverse_order);
            if (!begin_optional)
                continue;

            std::optional<size_t> end_optional = index_access.findRightmostMarkLessThanValueInRange(
                other_interval_part_index, current_part_range.value, other_interval_range, in_reverse_order);
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
        chassert(part_index_start_to_range.contains(current_part_range_index));

        /** If there are more than 1 part ranges that we are currently processing
          * that means that this part range is intersecting with other range.
          *
          * If part level is 0, we must process whole part because it can contain duplicate primary keys.
          */
        if (intersecting_parts != 1 || ranges_in_data_parts[current_part_range.part_index].data_part->info.level == 0)
        {
            add_intersecting_range(current_part_range.part_index, part_index_start_to_range[current_part_range_index]);
            part_index_start_to_range.erase(current_part_range_index);
            continue;
        }

        if (previous_part_range.event == PartsRangesIterator::EventType::RangeStart)
        {
            chassert(current_part_range.part_index == previous_part_range.part_index);
            chassert(current_part_range.range == previous_part_range.range);

            /// Case 3 Range End after Range Start
            add_non_intersecting_range(current_part_range.part_index, current_part_range.range);
            part_index_start_to_range.erase(current_part_range_index);
            continue;
        }

        chassert(previous_part_range.event == PartsRangesIterator::EventType::RangeEnd);

        /// Case 4 Range End after Range End
        std::optional<size_t> begin_optional = index_access.findLeftmostMarkGreaterThanValueInRange(
            current_part_range.part_index, previous_part_range.value, current_part_range.range, in_reverse_order);
        size_t end = current_part_range.range.end;

        if (begin_optional && end - *begin_optional >= min_number_of_marks_for_non_intersecting_range)
        {
            size_t begin = *begin_optional;
            add_intersecting_range(current_part_range.part_index, MarkRange{part_index_start_to_range[current_part_range_index].begin, begin});
            add_non_intersecting_range(current_part_range.part_index, MarkRange{begin, end});
        }
        else
        {
            add_intersecting_range(current_part_range.part_index, MarkRange{part_index_start_to_range[current_part_range_index].begin, end});
        }

        part_index_start_to_range.erase(current_part_range_index);
    }

    /// Process parts ranges with undefined value at end mark
    /// The last parts ranges could be non-intersect only if: (1) there is only one part range left, (2) it belongs to a non-L0 part,
    /// and (3) the begin value of this range is larger than the largest end value of all previous ranges. This is too complicated
    /// to check, so we just add the last part ranges to the intersecting ranges.
    for (const auto & [part_range_index, mark_range] : part_index_start_to_range)
        add_intersecting_range(part_range_index.part_index, mark_range);

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
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SplitPartsByRanges splitIntersectingPartsRangesIntoLayers(
    RangesInDataParts ranges_in_data_parts,
    size_t max_layers,
    size_t max_columns_in_index,
    bool in_reverse_order,
    const LoggerPtr & logger)
{
    /** We will advance the iterator pointing to the mark with the smallest PK value until
      * there will be not less than rows_per_layer rows in the current layer (roughly speaking).
      * Then we choose the last observed value as the new border, so the current layer will consists
      * of granules with values greater than the previous mark and less or equal than the new border.
      *
      * We use PartRangeIndex to track currently processing ranges, because after sort, RangeStart event is always placed
      * before Range End event and it is possible to encounter overlapping Range Start events for the same part.
      */
    IndexAccess index_access(ranges_in_data_parts, max_columns_in_index);

    using PartsRangesIteratorWithIndex = std::pair<PartsRangesIterator, PartRangeIndex>;
    std::priority_queue<PartsRangesIteratorWithIndex, std::vector<PartsRangesIteratorWithIndex>, std::greater<>> parts_ranges_queue;

    for (size_t part_index = 0; part_index < ranges_in_data_parts.size(); ++part_index)
    {
        for (const auto & range : ranges_in_data_parts[part_index].ranges)
        {
            const auto & index_granularity = ranges_in_data_parts[part_index].data_part->index_granularity;
            PartsRangesIterator parts_range_start{
                index_access.getValue(part_index, range.begin),
                in_reverse_order,
                range,
                part_index,
                PartsRangesIterator::EventType::RangeStart,
                false};
            PartRangeIndex parts_range_start_index(parts_range_start);
            parts_ranges_queue.push({std::move(parts_range_start), std::move(parts_range_start_index)});

            const bool value_is_defined_at_end_mark = range.end < index_granularity->getMarksCount();
            if (!value_is_defined_at_end_mark)
                continue;

            PartsRangesIterator parts_range_end{
                index_access.getValue(part_index, range.end),
                in_reverse_order,
                range,
                part_index,
                PartsRangesIterator::EventType::RangeEnd,
                false};
            PartRangeIndex parts_range_end_index(parts_range_end);
            parts_ranges_queue.push({std::move(parts_range_end), std::move(parts_range_end_index)});
        }
    }

    /// The beginning of currently started (but not yet finished) range of marks of a part in the current layer.
    std::unordered_map<PartRangeIndex, size_t, PartRangeIndexHash> current_part_range_begin;
    /// The current ending of a range of marks of a part in the current layer.
    std::unordered_map<PartRangeIndex, size_t, PartRangeIndexHash> current_part_range_end;

    /// Determine borders between layers.
    std::vector<Values> borders;
    std::vector<RangesInDataParts> result_layers;

    size_t total_intersecting_rows_count = ranges_in_data_parts.getRowsCountAllParts();
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

        RangesInDataPartsBuilder current_layer_builder(ranges_in_data_parts);
        result_layers.emplace_back();

        while (rows_in_current_layer < rows_per_layer || layers_intersection_is_too_big() || result_layers.size() == max_layers)
        {
            // We're advancing iterators until a new value showed up.
            Values last_value;
            while (!parts_ranges_queue.empty() && (last_value.empty() || last_value == parts_ranges_queue.top().first.value))
            {
                auto [current, current_range_index] = parts_ranges_queue.top();
                PartRangeIndex current_part_range_index(current);
                parts_ranges_queue.pop();

                const auto part_index = current.part_index;

                if (current.event == PartsRangesIterator::EventType::RangeEnd)
                {
                    current_layer_builder.addRange(part_index, MarkRange{current_part_range_begin[current_range_index], current.range.end});
                    current_part_range_begin.erase(current_range_index);
                    current_part_range_end.erase(current_range_index);
                    continue;
                }

                last_value = std::move(current.value);
                rows_in_current_layer += index_access.getMarkRows(part_index, current.range.begin);
                ++marks_in_current_layer;

                current_part_range_begin.try_emplace(current_range_index, current.range.begin);
                current_part_range_end[current_range_index] = current.range.begin;

                if (current.range.begin + 1 < current.range.end)
                {
                    ++current.range.begin;
                    current.value = index_access.getValue(part_index, current.range.begin);
                    parts_ranges_queue.push({std::move(current), current_range_index});
                }
            }

            if (parts_ranges_queue.empty())
                break;

            if (rows_in_current_layer >= rows_per_layer && !layers_intersection_is_too_big() && result_layers.size() < max_layers)
                borders.push_back(last_value);
        }

        for (const auto & [current_range_index, last_mark] : current_part_range_end)
        {
            current_layer_builder.addRange(current_range_index.part_index, MarkRange{current_part_range_begin[current_range_index], last_mark + 1});
            current_part_range_begin[current_range_index] = current_part_range_end[current_range_index];
        }

        result_layers.back() = std::move(current_layer_builder.getCurrentRangesInDataParts());
    }

    size_t result_layers_size = result_layers.size();
    LOG_TEST(logger, "Split intersecting ranges into {} layers", result_layers_size);

    for (size_t i = 0; i < result_layers_size; ++i)
    {
        auto & layer = result_layers[i];

        if (in_reverse_order)
        {
            LOG_TEST(logger, "Layer {} {} filter values in [{}, {}))",
                i,
                layer.getDescriptions().describe(),
                i < borders.size() ? ::toString(borders[i]) : "-inf", i ? ::toString(borders[i - 1]) : "+inf");
        }
        else
        {
            LOG_TEST(logger, "Layer {} {} filter values in ({}, {}])",
                i,
                layer.getDescriptions().describe(),
                i ? ::toString(borders[i - 1]) : "-inf", i < borders.size() ? ::toString(borders[i]) : "+inf");
        }

        std::stable_sort(
            layer.begin(),
            layer.end(),
            [](const auto & lhs, const auto & rhs) { return lhs.part_index_in_query < rhs.part_index_in_query; });
    }

    return {std::move(result_layers), std::move(borders)};
}


/// Will return borders.size()+1 filters in total, i-th filter will accept rows with PK values within the range (borders[i-1], borders[i]].
static ASTs buildFilters(const KeyDescription & primary_key, const std::vector<Values> & borders, bool in_reverse_order)
{
    auto add_and_condition = [&](ASTPtr & result, const ASTPtr & foo) { result = (!result) ? foo : makeASTFunction("and", result, foo); };

    /// Produces ASTPtr to predicate (pk_col0, pk_col1, ... , pk_colN) > (value[0], value[1], ... , value[N]), possibly with conversions.
    /// For example, if table PK is (a, toDate(d)), where `a` is UInt32 and `d` is DateTime, and PK columns values are (8192, 19160),
    /// it will build the following predicate: greater(tuple(a, toDate(d)), tuple(8192, cast(19160, 'Date'))).
    /// If @in_reverse_order == true, compare values in reverse order.
    auto lexicographically_greater = [&](const Values & values) -> ASTPtr
    {
        ASTs pks_ast;
        ASTs values_ast;
        for (size_t i = 0; i < values.size(); ++i)
        {
            const auto & type = primary_key.data_types.at(i);

            // PK may contain functions of the table columns, so we need the actual PK AST with all expressions it contains.
            auto pk_ast = primary_key.expression_list_ast->children.at(i)->clone();

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
                component_ast = makeASTFunction("cast", std::move(component_ast), std::make_shared<ASTLiteral>(decayed_type->getName()));

                values_ast.push_back(std::move(component_ast));
            }
        }

        ASTPtr pk_columns_as_tuple = makeASTFunction("tuple", pks_ast);
        ASTPtr values_as_tuple = makeASTFunction("tuple", values_ast);

        return makeASTFunction(in_reverse_order ? "less" : "greater", pk_columns_as_tuple, values_as_tuple);
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

RangesInDataParts findPKRangesForFinalAfterSkipIndexImpl(RangesInDataParts & ranges_in_data_parts, bool cannot_sort_primary_key, const LoggerPtr & logger)
{
    IndexAccess index_access(ranges_in_data_parts);
    std::vector<PartsRangesIterator> selected_ranges;
    std::vector<PartsRangesIterator> rejected_ranges;

    RangesInDataPartsBuilder result(ranges_in_data_parts);

    auto skip_and_return_all_part_ranges = [&]()
    {
        RangesInDataParts all_part_ranges(std::move(ranges_in_data_parts));
        for (auto & all_part_range : all_part_ranges)
        {
            const auto & index_granularity = all_part_range.data_part->index_granularity;
            all_part_range.ranges = MarkRanges{{MarkRange{0, index_granularity->getMarksCountWithoutFinal()}}};
        }
        return all_part_ranges;
    };

    if (cannot_sort_primary_key) /// just expand to all parts + ranges
    {
        return skip_and_return_all_part_ranges();
    }

    PartsRangesIterator selected_upper_bound;
    std::vector<std::vector<size_t>> part_selected_ranges(ranges_in_data_parts.size(), std::vector<size_t>());
    for (size_t part_index = 0; part_index < ranges_in_data_parts.size(); ++part_index)
    {
        const auto & index_granularity = ranges_in_data_parts[part_index].data_part->index_granularity;
        for (const auto & range : ranges_in_data_parts[part_index].ranges)
        {
            const bool value_is_defined_at_end_mark = range.end < index_granularity->getMarksCount();
            if (!value_is_defined_at_end_mark)
            {
                return skip_and_return_all_part_ranges();
            }

            selected_ranges.push_back(
                {index_access.getValue(part_index, range.begin), false, range, part_index,
                    PartsRangesIterator::EventType::RangeStart, true});

            const auto & range_end_value = index_access.getValue(part_index, range.end);
            if (selected_upper_bound.value.empty() || (compareValues(range_end_value, selected_upper_bound.value, false) > 0))
                selected_upper_bound = {range_end_value, false, range, part_index, PartsRangesIterator::EventType::RangeStart, true};

            for (auto i = range.begin; i < range.end; ++i)
               part_selected_ranges[part_index].push_back(i);
        }
    }

    if (selected_ranges.empty())
        return result.getCurrentRangesInDataParts();

    ::sort(selected_ranges.begin(), selected_ranges.end());

    const PartsRangesIterator selected_lower_bound = selected_ranges[0];

    for (size_t part_index = 0; part_index < ranges_in_data_parts.size(); ++part_index)
    {
        const auto & index_granularity = ranges_in_data_parts[part_index].data_part->index_granularity;
        const auto & part_lower_bound = index_access.getValue(part_index, 0);
        const auto & part_upper_bound = index_access.getValue(part_index, index_granularity->getMarksCountWithoutFinal());
        if ((compareValues(selected_lower_bound.value, part_upper_bound, false) > 0) ||
            (compareValues(selected_upper_bound.value, part_lower_bound, false) < 0))
        {
            continue; /// early exit, intersection infeasible in this part
        }

        auto candidates_start = index_access.findLeftmostMarkGreaterThanValueInRange(part_index, selected_lower_bound.value, MarkRange{0, index_granularity->getMarksCountWithoutFinal() + 1}, false);
        if (!candidates_start)
            continue; /// no intersection possible in this part
        if (candidates_start.value() > 0)
            candidates_start = candidates_start.value() - 1;

        auto candidates_end = index_access.findLeftmostMarkGreaterThanValueInRange(part_index, selected_upper_bound.value, MarkRange{0, index_granularity->getMarksCountWithoutFinal() + 1}, false);
        if (!candidates_end)
            candidates_end = index_granularity->getMarksCountWithoutFinal();

        for (auto range_begin = candidates_start.value(); range_begin <= candidates_end.value(); range_begin++)
        {
            if (std::binary_search(part_selected_ranges[part_index].begin(), part_selected_ranges[part_index].end(), range_begin))
                continue;
            MarkRange rejected_range(range_begin, range_begin + 1);
            rejected_ranges.push_back(
                {index_access.getValue(part_index, rejected_range.begin), false, rejected_range, part_index,
                    PartsRangesIterator::EventType::RangeStart, false});
        }
    }

    ::sort(rejected_ranges.begin(), rejected_ranges.end());

    std::vector<PartsRangesIterator>::iterator selected_ranges_iter = selected_ranges.begin();
    std::vector<PartsRangesIterator>::iterator rejected_ranges_iter = rejected_ranges.begin();
    size_t more_ranges_added = 0;

    while (selected_ranges_iter != selected_ranges.end() && rejected_ranges_iter != rejected_ranges.end())
    {
        auto selected_range_start = selected_ranges_iter->value;
        auto selected_range_end = index_access.getValue(selected_ranges_iter->part_index, selected_ranges_iter->range.end);
        auto rejected_range_start = rejected_ranges_iter->value;

        int result1 = compareValues(rejected_range_start, selected_range_start, false);
        int result2 = compareValues(rejected_range_start, selected_range_end, false);

        if (result1 == 0 || result2 == 0 || (result1 > 0 && result2 < 0)) /// rejected_range_start inside [selected_range]
        {
            result.addRange(rejected_ranges_iter->part_index, rejected_ranges_iter->range);
            rejected_ranges_iter++;
            more_ranges_added++;
        }
        else if (result1 > 0) /// rejected_range_start beyond [selected_range]
        {
            result.addRange(selected_ranges_iter->part_index, selected_ranges_iter->range);
            selected_ranges_iter++;
        }
        else
        {
            auto rejected_range_end = index_access.getValue(rejected_ranges_iter->part_index, rejected_ranges_iter->range.end);
            int result3 = compareValues(rejected_range_end, selected_range_start, false);
            int result4 = compareValues(rejected_range_end, selected_range_end, false);
            /// rejected_range_end inside [selected range] OR [rejected range] encompasses [selected range]
            if (result3 == 0 || result4 == 0 || (result3 > 0 && result4 < 0) || (result1 < 0 && result4 > 0))
            {
                result.addRange(rejected_ranges_iter->part_index, rejected_ranges_iter->range);
                more_ranges_added++;
            }
            rejected_ranges_iter++;
        }
    }

    while (selected_ranges_iter != selected_ranges.end())
    {
        result.addRange(selected_ranges_iter->part_index, selected_ranges_iter->range);
        selected_ranges_iter++;
    }

    auto result_final_ranges = result.getCurrentRangesInDataParts();
    std::stable_sort(
        result_final_ranges.begin(),
        result_final_ranges.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.part_index_in_query < rhs.part_index_in_query; });
    for (auto & result_final_range : result_final_ranges)
    {
        std::sort(result_final_range.ranges.begin(), result_final_range.ranges.end());
    }

    LOG_TRACE(logger, "findPKRangesForFinalAfterSkipIndex : processed {} parts, initially selected {} ranges & rejected {}, more {} ranges added", ranges_in_data_parts.size(), selected_ranges.size(), rejected_ranges.size(), more_ranges_added);

    return result_final_ranges;
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
    const KeyDescription & sorting_key,
    ExpressionActionsPtr sorting_expr,
    RangesInDataParts parts,
    size_t max_layers,
    ContextPtr context,
    ReadingInOrderStepGetter && in_order_reading_step_getter,
    bool split_parts_ranges_into_intersecting_and_non_intersecting_final,
    bool split_intersecting_parts_ranges_into_layers)
{
    auto logger = getLogger("PartsSplitter");

    SplitPartsWithRangesByPrimaryKeyResult result;

    RangesInDataParts intersecting_parts_ranges = std::move(parts);

    auto create_merging_pipe = [&](const auto & ranges)
    {
        auto pipe = in_order_reading_step_getter(ranges);

        pipe.addSimpleTransform([sorting_expr](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, sorting_expr);
        });

        return pipe;
    };

    if (!isSafePrimaryKey(primary_key))
    {
        if (!intersecting_parts_ranges.empty())
            result.merging_pipes.emplace_back(create_merging_pipe(intersecting_parts_ranges));
        return result;
    }

    bool in_reverse_order = false;
    size_t num_primary_keys = primary_key.expression_list_ast->children.size();
    if (!sorting_key.reverse_flags.empty())
    {
        chassert(sorting_key.reverse_flags.size() >= num_primary_keys);
        in_reverse_order = sorting_key.reverse_flags[0];
        for (size_t i = 1; i < num_primary_keys; ++i)
        {
            /// It's not possible to split parts when some keys are in ascending
            /// order while others are in descending order.
            if (in_reverse_order != sorting_key.reverse_flags[i])
            {
                result.merging_pipes.emplace_back(create_merging_pipe(intersecting_parts_ranges));
                return result;
            }
        }
    }

    if (split_parts_ranges_into_intersecting_and_non_intersecting_final)
    {
        SplitPartsRangesResult split_result = splitPartsRanges(intersecting_parts_ranges, in_reverse_order, logger);
        result.non_intersecting_parts_ranges = std::move(split_result.non_intersecting_parts_ranges);
        intersecting_parts_ranges = std::move(split_result.intersecting_parts_ranges);
    }

    if (!split_intersecting_parts_ranges_into_layers)
    {
        if (!intersecting_parts_ranges.empty())
            result.merging_pipes.emplace_back(create_merging_pipe(intersecting_parts_ranges));
        return result;
    }

    if (max_layers <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "max_layer should be greater than 1");

    auto split_ranges = splitIntersectingPartsRangesIntoLayers(intersecting_parts_ranges, max_layers, primary_key.column_names.size(), in_reverse_order, logger);
    result.merging_pipes = readByLayers(std::move(split_ranges), primary_key, create_merging_pipe, in_reverse_order, context);
    return result;
}

Pipes readByLayers(
    SplitPartsByRanges split_ranges,
    const KeyDescription & primary_key,
    ReadingInOrderStepGetter && step_getter,
    bool in_reverse_order,
    ContextPtr context)
{
    auto && [layers, borders] = std::move(split_ranges);
    auto filters = buildFilters(primary_key, borders, in_reverse_order);
    Pipes merging_pipes(layers.size());

    for (size_t i = 0; i < layers.size(); ++i)
    {
        merging_pipes[i] = step_getter(layers[i]);

        auto & filter_function = filters[i];
        if (!filter_function)
            continue;

        auto syntax_result = TreeRewriter(context).analyze(filter_function, primary_key.expression->getRequiredColumnsWithTypes());
        auto actions = ExpressionAnalyzer(filter_function, syntax_result, context).getActionsDAG(false);
        reorderColumns(actions, merging_pipes[i].getHeader(), filter_function->getColumnName());
        ExpressionActionsPtr expression_actions = std::make_shared<ExpressionActions>(std::move(actions));
        auto description = in_reverse_order ? fmt::format(
                                                  "filter values in [{}, {})",
                                                  i < borders.size() ? ::toString(borders[i]) : "-inf",
                                                  i ? ::toString(borders[i - 1]) : "+inf")
                                            : fmt::format(
                                                  "filter values in ({}, {}]",
                                                  i ? ::toString(borders[i - 1]) : "-inf",
                                                  i < borders.size() ? ::toString(borders[i]) : "+inf");
        merging_pipes[i].addSimpleTransform(
            [&](const Block & header)
            {
                auto step = std::make_shared<FilterSortedStreamByRange>(header, expression_actions, filter_function->getColumnName(), true);
                step->setDescription(description);
                return step;
            });
    }

    return merging_pipes;
}

RangesInDataParts findPKRangesForFinalAfterSkipIndex(
    const KeyDescription & primary_key,
    const KeyDescription & sorting_key,
    RangesInDataParts & ranges_in_data_parts,
    const LoggerPtr & logger)
{
    bool cannot_sort_primary_key = false;
    if (!isSafePrimaryKey(primary_key) || !sorting_key.reverse_flags.empty())
    {
        LOG_TRACE(logger, "Primary key is not sortable, expanding PK range to entire due to exact_mode.");
        cannot_sort_primary_key = true;
    }
    return findPKRangesForFinalAfterSkipIndexImpl(ranges_in_data_parts, cannot_sort_primary_key, logger);
}
}
