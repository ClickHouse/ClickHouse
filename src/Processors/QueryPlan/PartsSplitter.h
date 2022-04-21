#pragma once

#include <algorithm>
#include <numeric>
#include <queue>
#include <set>
#include <vector>

#include <Core/Field.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/MergeTree/RangesInDataPart.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct IIndexAccess
{
    struct Value : std::vector<Field>
    {
        using Base = std::vector<Field>;
        using Base::Base;

        std::string toString() const { return fmt::format("({})", fmt::join(*this, ", ")); }

        bool operator<(const Value & other) const { return std::lexicographical_compare(begin(), end(), other.begin(), other.end()); }
    };

    virtual ~IIndexAccess() = default;

    virtual Value getValue(size_t part_idx, size_t mark) const = 0;

    virtual size_t getMarkRows(size_t part_idx, size_t mark) const = 0;

    virtual size_t getTotalRowCount() const = 0;
};

class IndexAccess : public IIndexAccess
{
public:
    explicit IndexAccess(const RangesInDataParts & parts_) : parts(parts_) { }

    Value getValue(size_t part_idx, size_t mark) const override
    {
        const auto & index = parts[part_idx].data_part->index;
        Value value(index.size());
        for (size_t i = 0; i < value.size(); ++i)
        {
            index[i]->get(mark, value[i]);
            // NULL_LAST
            if (value[i].isNull())
                value[i] = POSITIVE_INFINITY;
        }
        return value;
    }

    size_t getMarkRows(size_t part_idx, size_t mark) const override
    {
        return parts[part_idx].data_part->index_granularity.getMarkRows(mark);
    }

    size_t getTotalRowCount() const override
    {
        return std::accumulate(
            parts.begin(), parts.end(), static_cast<size_t>(0), [](size_t sum, const auto & part) { return sum + part.getRowsCount(); });
    }

private:
    const RangesInDataParts & parts;
};

class PartsSplitter
{
public:
    using IndexValue = IIndexAccess::Value;

private:
    struct RangeInDataPart : MarkRange
    {
        size_t part_idx;

        const MarkRange & base() const { return static_cast<const MarkRange &>(*this); }

        bool operator<(const RangeInDataPart & other) const { return std::tie(base(), part_idx) < std::tie(other.base(), other.part_idx); }
    };

    /// Returns the rightmost mark where PK value is less than border.
    size_t splitByBorder(size_t part_idx, const MarkRange & range, const IndexValue & border)
    {
        auto left = range.begin;
        auto right = range.end;
        while (left + 1 < right)
        {
            auto mid = (left + right) / 2;
            auto value = index_access->getValue(part_idx, mid);
            if (value < border)
                left = mid;
            else
                right = mid;
        }
        return left;
    }

public:
    PartsSplitter(
        const KeyDescription & primary_key_,
        const RangesInDataParts & parts_,
        size_t max_num_streams,
        std::unique_ptr<IIndexAccess> index_access_)
        : primary_key(primary_key_), parts(parts_), index_access(std::move(index_access_))
    {
        if (max_num_streams <= 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "max_num_streams should be greater than 1.");
        rows_per_layer = std::max<size_t>(index_access->getTotalRowCount() / max_num_streams, 1);
    }

    std::vector<IndexValue> chooseBorders()
    {
        // We will advance the iterator pointing to the mark with the smallest value until there will be not less than rows_per_layer rows in the current layer.
        // Then we choose the smallest value under the iterators as the new border (42 for the case depicted on the diagram above).

        struct Iterator
        {
            IndexValue value;
            RangeInDataPart range;

            bool operator<(const Iterator & other) const { return value > other.value; }
        };

        std::vector<IndexValue> borders;
        std::priority_queue<Iterator> range_queue;
        for (size_t part_idx = 0; part_idx < parts.size(); ++part_idx)
            for (const auto & range : parts[part_idx].ranges)
                range_queue.push({index_access->getValue(part_idx, range.begin), {range, part_idx}});

        while (!range_queue.empty())
        {
            size_t rows_in_current_layer = 0;
            while (rows_in_current_layer < rows_per_layer && !range_queue.empty())
            {
                auto minimal = range_queue.top();
                range_queue.pop();
                rows_in_current_layer += index_access->getMarkRows(minimal.range.part_idx, minimal.range.begin);
                if (rows_in_current_layer >= rows_per_layer)
                    borders.push_back(std::move(minimal.value));
                if (minimal.range.begin + 1 < minimal.range.end)
                {
                    minimal.range.begin++;
                    minimal.value = index_access->getValue(minimal.range.part_idx, minimal.range.begin);
                    range_queue.push(std::move(minimal));
                }
            }
        }
        LOG_DEBUG(&Poco::Logger::get("PartsSplitter"), "Collected {} borders:", borders.size());
        for (const auto & border : borders)
            LOG_DEBUG(&Poco::Logger::get("PartsSplitter"), "{}", border.toString());
        return borders;
    }

    // Will return exactly borders.size()+1 layers (some of them might be empty).
    std::vector<RangesInDataParts> divideIntoLayers(const std::vector<IndexValue> & borders)
    {
        // Here we use a standard sweep line technique. We consider two kinds of events: a beginning of a part's range and a border.
        // All events are ordered by the index value corresponding to the mark on which the event happens.
        // Processing of events is very simple: when we met another border we form a current layer from all the ranges that started
        // between the current border and the previous one.

        struct Event
        {
            enum class Type
            {
                Border,
                RangeBeginning,
            };

            Type type;
            IndexValue value;
            std::optional<RangeInDataPart> range; // if it is not a border

            explicit Event(IndexValue value_) : type(Type::Border), value(std::move(value_)) { }

            Event(IndexValue value_, size_t part_idx, MarkRange range_)
                : type(Type::RangeBeginning), value(std::move(value_)), range({std::move(range_), part_idx})
            {
            }

            bool operator<(const Event & other) const
            {
                return std::tie(value, type, range) < std::tie(other.value, other.type, other.range);
            }
        };

        std::multiset<Event> event_queue{borders.begin(), borders.end()}; // in practice borders may have equal values
        for (size_t part_idx = 0; part_idx < parts.size(); ++part_idx)
            for (const auto & range : parts[part_idx].ranges)
                event_queue.emplace(index_access->getValue(part_idx, range.begin), part_idx, range);

        std::vector<RangesInDataParts> result_layers(1);
        std::set<RangeInDataPart> started_ranges;
        while (!event_queue.empty())
        {
            const auto event = *event_queue.begin();
            event_queue.erase(event_queue.begin());
            if (event.type == Event::Type::RangeBeginning)
            {
                started_ranges.emplace(event.range.value());
            }
            else
            {
                //                                                                                                            +------ borders[i]
                //                                                                                                            v
                //                                                                                                            X=4
                //
                //                                               PK values:                  111111111111               3333334444556...
                //                                                                           |   |   |                  |---|---|---|--...--|---
                //                                                   marks:                  0   1   2                  5   6   7   8       12
                //                                                                         begin    end               begin                end
                //                                                                                                          ^
                //                                                                                                          +---------------------- split_mark
                //
                // current_layer (Filter: borders[i-1] <= pk < borders[i]):                  |---|---|---               |---|---
                //
                //    next_layer (Filter: borders[i] <= pk < borders[i+1]):                                                 |---|---|--...--|---
                //
                // Every already started but not yet finished range should be divided into two subranges:
                // one will go to the current layer and should contain all rows with primary key values less than border
                // and another one with all rows with PK values not less than border should be returned back into the queue.
                for (const auto & range : started_ranges)
                {
                    const auto part_idx = range.part_idx;
                    const auto split_mark = splitByBorder(part_idx, range, event.value);
                    result_layers.back().emplace_back(
                        parts[part_idx].data_part, part_idx, MarkRanges{{range.begin, std::min<size_t>(split_mark + 1, range.end)}});
                    const auto subrange_for_next_layer = MarkRange{split_mark, range.end};
                    if (subrange_for_next_layer.begin < subrange_for_next_layer.end)
                    {
                        auto new_begin_value = index_access->getValue(part_idx, subrange_for_next_layer.begin);
                        event_queue.emplace(std::move(new_begin_value), part_idx, subrange_for_next_layer);
                    }
                }
                started_ranges.clear();
                result_layers.emplace_back();
            }
        }
        for (const auto & range : started_ranges)
            result_layers.back().emplace_back(parts[range.part_idx].data_part, range.part_idx, MarkRanges{range});

        for (size_t layer = 0; layer < result_layers.size(); ++layer)
        {
            LOG_DEBUG(&Poco::Logger::get("PartsSplitter"), "Layer {}, ranges:", layer);
            for (const auto & range : result_layers[layer])
                for (const auto & mark_range : range.ranges)
                    LOG_DEBUG(
                        &Poco::Logger::get("PartsSplitter"),
                        "part_idx={}, begin={}, end={}",
                        range.part_index_in_query,
                        mark_range.begin,
                        mark_range.end);
        }

        return result_layers;
    }

    /// Will return borders.size()+1 filters in total, i-th filter will accept values from the range [borders[i-1], borders[i]).
    std::vector<ASTPtr> buildFilters(const std::vector<IndexValue> & borders)
    {
        auto add_and_condition = [&](ASTPtr & result, const ASTPtr & foo) { result = !result ? foo : makeASTFunction("and", result, foo); };
        auto add_or_condition = [&](ASTPtr & result, const ASTPtr & foo) { result = !result ? foo : makeASTFunction("or", result, foo); };

        const auto & types = primary_key.data_types;
        auto lexicographically_less = [&](const IndexValue & value)
        {
            ASTPtr result;
            ASTPtr prefix_of_equal;
            for (size_t i = 0; i < value.size(); ++i)
            {
                auto primary_key_column_ast = primary_key.expression_list_ast->children.at(i);
                ASTPtr value_ast = std::make_shared<ASTLiteral>(value[i]);
                if (isColumnedAsNumber(types.at(i)->getTypeId()) && !isNumber(types.at(i)->getTypeId()))
                    value_ast = makeASTFunction("cast", std::move(value_ast), std::make_shared<ASTLiteral>(types.at(i)->getName()));

                ASTPtr current = makeASTFunction("less", primary_key_column_ast, value_ast); // pk_col[i] < value[i]
                if (prefix_of_equal)
                    add_and_condition(
                        current, prefix_of_equal); // pk_col[i] < value[i] && pk_col[i-1] == value[i-1] && ... && pk_col[0] == value[0]
                add_or_condition(
                    result,
                    current); // (pk_col[i] < value[i] && pk_col[i-1] == value[i-1] && ... ) || same for pk_col[i-1] || ... || same for pk_col[0]

                add_and_condition(prefix_of_equal, makeASTFunction("equals", primary_key_column_ast, value_ast));
            }
            return result;
        };

        std::vector<ASTPtr> filters(borders.size() + 1);
        for (size_t layer = 0; layer <= borders.size(); ++layer)
        {
            if (layer > 0)
                add_and_condition(filters[layer], makeASTFunction("not", lexicographically_less(borders[layer - 1])));
            if (layer < borders.size())
                add_and_condition(filters[layer], lexicographically_less(borders[layer]));
        }
        return filters;
    }

private:
    const KeyDescription & primary_key;
    const RangesInDataParts & parts;
    std::unique_ptr<IIndexAccess> index_access;
    size_t rows_per_layer;
};

}
