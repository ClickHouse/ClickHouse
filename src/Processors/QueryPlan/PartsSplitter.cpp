#include <algorithm>
#include <memory>
#include <numeric>
#include <queue>
#include <unordered_map>
#include <vector>

#include <Core/Field.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/MergeTree/RangesInDataPart.h>

using namespace DB;

namespace
{

using Value = std::vector<Field>;

std::string toString(const Value & value)
{
    return fmt::format("({})", fmt::join(value, ", "));
}

class IndexAccess
{
public:
    explicit IndexAccess(const RangesInDataParts & parts_) : parts(parts_) { }

    Value getValue(size_t part_idx, size_t mark) const
    {
        const auto & index = parts[part_idx].data_part->index;
        Value value(index.size());
        for (size_t i = 0; i < value.size(); ++i)
            index[i]->get(mark, value[i]);
        return value;
    }

    size_t getMarkRows(size_t part_idx, size_t mark) const { return parts[part_idx].data_part->index_granularity.getMarkRows(mark); }

    size_t getTotalRowCount() const
    {
        size_t total = 0;
        for (const auto & part : parts)
            total += part.getRowsCount();
        return total;
    }

private:
    const RangesInDataParts & parts;
};

struct RangeInDataPart : MarkRange
{
    size_t part_idx;
};

struct PartsRangesIterator
{
    enum class EventType
    {
        RangeBeginning,
        RangeEnding,
    };

    bool operator<(const PartsRangesIterator & other) const { return std::tie(value, event) > std::tie(other.value, other.event); }

    Value value;
    RangeInDataPart range;
    EventType event;
};


std::pair<std::vector<Value>, std::vector<RangesInDataParts>> split(RangesInDataParts parts, size_t max_layers)
{
    // We will advance the iterator pointing to the mark with the smallest value until there will be not less than rows_per_layer rows in the current layer (roughly speaking).
    // Then we choose the last observed value as the new border, so the current layer will consists of granules with values greater than the previous mark and less or equal
    // than the new border.

    const auto index_access = std::make_unique<IndexAccess>(parts);
    std::priority_queue<PartsRangesIterator> parts_ranges_queue;
    for (size_t part_idx = 0; part_idx < parts.size(); ++part_idx)
    {
        for (const auto & range : parts[part_idx].ranges)
        {
            parts_ranges_queue.push(
                {index_access->getValue(part_idx, range.begin), {range, part_idx}, PartsRangesIterator::EventType::RangeBeginning});
            const auto & index_granularity = parts[part_idx].data_part->index_granularity;
            if (index_granularity.hasFinalMark() && range.end + 1 == index_granularity.getMarksCount())
                parts_ranges_queue.push(
                    {index_access->getValue(part_idx, range.end), {range, part_idx}, PartsRangesIterator::EventType::RangeEnding});
        }
    }

    /// the beginning of currently started (but not yet finished) range of marks of a part in the current layer
    std::unordered_map<size_t, size_t> current_part_range_begin;
    /// the current ending of a range of marks of a part in the current layer
    std::unordered_map<size_t, size_t> current_part_range_end;

    /// determine borders between layers
    std::vector<Value> borders;
    std::vector<RangesInDataParts> result_layers;

    const size_t rows_per_layer = std::max<size_t>(index_access->getTotalRowCount() / max_layers, 1);

    while (!parts_ranges_queue.empty())
    {
        // new layer should include last granules of still open ranges from the previous layer, because they may already contain values greater than the last border
        size_t rows_in_current_layer = 0;
        size_t marks_in_current_layer = 0;

        // intersection between the current and next layers is just the last observed marks of each still open part range. ratio is empirical
        auto layers_intersection_is_too_big = [&]()
        {
            const auto intersected_parts = current_part_range_end.size();
            return marks_in_current_layer < intersected_parts * 2;
        };

        result_layers.emplace_back();

        while (rows_in_current_layer < rows_per_layer || layers_intersection_is_too_big() || result_layers.size() == max_layers)
        {
            // we're advancing iterators until a new value showed up
            Value last_value;
            while (!parts_ranges_queue.empty() && (last_value.empty() || last_value == parts_ranges_queue.top().value))
            {
                auto current = parts_ranges_queue.top();
                parts_ranges_queue.pop();
                const auto part_idx = current.range.part_idx;

                if (current.event == PartsRangesIterator::EventType::RangeEnding)
                {
                    result_layers.back().emplace_back(
                        parts[part_idx].data_part,
                        parts[part_idx].part_index_in_query,
                        MarkRanges{{current_part_range_begin[part_idx], current.range.end}});
                    current_part_range_begin.erase(part_idx);
                    current_part_range_end.erase(part_idx);
                    continue;
                }

                last_value = std::move(current.value);
                rows_in_current_layer += index_access->getMarkRows(part_idx, current.range.begin);
                marks_in_current_layer++;
                current_part_range_begin.try_emplace(part_idx, current.range.begin);
                current_part_range_end[part_idx] = current.range.begin;
                if (current.range.begin + 1 < current.range.end)
                {
                    current.range.begin++;
                    current.value = index_access->getValue(part_idx, current.range.begin);
                    parts_ranges_queue.push(std::move(current));
                }
            }
            if (parts_ranges_queue.empty())
                break;
            if (rows_in_current_layer >= rows_per_layer && !layers_intersection_is_too_big() && result_layers.size() < max_layers)
                borders.push_back(last_value);
        }
        for (const auto & [part_idx, last_mark] : current_part_range_end)
        {
            result_layers.back().emplace_back(
                parts[part_idx].data_part,
                parts[part_idx].part_index_in_query,
                MarkRanges{{current_part_range_begin[part_idx], last_mark + 1}});
            current_part_range_begin[part_idx] = current_part_range_end[part_idx];
        }
    }
    for (auto & layer : result_layers)
    {
        std::stable_sort(
            layer.begin(),
            layer.end(),
            [](const auto & lhs, const auto & rhs) { return lhs.part_index_in_query < rhs.part_index_in_query; });
    }

    LOG_DEBUG(&Poco::Logger::get("PartsSplitter"), "Collected {} borders:", borders.size());
    for (const auto & border : borders)
        LOG_DEBUG(&Poco::Logger::get("PartsSplitter"), "{}", ::toString(border));

    for (size_t layer = 0; layer < result_layers.size(); ++layer)
    {
        LOG_DEBUG(&Poco::Logger::get("PartsSplitter"), "Layer {}, ranges:", layer);
        for (const auto & range : result_layers[layer])
        {
            LOG_DEBUG(&Poco::Logger::get("PartsSplitter"), "part_idx={}", range.part_index_in_query);
            for (const auto & mark_range : range.ranges)
                LOG_DEBUG(&Poco::Logger::get("PartsSplitter"), "begin={}, end={}", mark_range.begin, mark_range.end);
        }
    }
    return std::make_pair(std::move(borders), std::move(result_layers));
}


/// Will return borders.size()+1 filters in total, i-th filter will accept values from the range [borders[i-1], borders[i]).
std::vector<ASTPtr> buildFilters(const KeyDescription & primary_key, const std::vector<Value> & borders)
{
    auto add_and_condition = [&](ASTPtr & result, const ASTPtr & foo) { result = !result ? foo : makeASTFunction("and", result, foo); };
    auto add_or_condition = [&](ASTPtr & result, const ASTPtr & foo) { result = !result ? foo : makeASTFunction("or", result, foo); };

    const auto & types = primary_key.data_types;
    auto lexicographically_greater = [&](const Value & value)
    {
        ASTPtr result;
        ASTPtr prefix_of_equal;
        for (size_t i = 0; i < value.size(); ++i)
        {
            auto primary_key_column_ast = primary_key.expression_list_ast->children.at(i);
            ASTPtr value_ast = std::make_shared<ASTLiteral>(value[i]);
            if (isColumnedAsNumber(types.at(i)->getTypeId()) && !isNumber(types.at(i)->getTypeId()))
                value_ast = makeASTFunction("cast", std::move(value_ast), std::make_shared<ASTLiteral>(types.at(i)->getName()));

            ASTPtr current = makeASTFunction("greater", primary_key_column_ast, value_ast); // pk_col[i] < value[i]
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

Pipes buildPipesForReading(
    const KeyDescription & primary_key,
    RangesInDataParts parts,
    size_t max_layers,
    ContextPtr context,
    ReadingStepGetter && reading_step_getter)
{
    if (max_layers <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "max_layer should be greater than 1.");

    auto && [borders, result_layers] = split(std::move(parts), max_layers);
    auto filters = buildFilters(primary_key, borders);
    Pipes pipes(result_layers.size());
    for (size_t i = 0; i < result_layers.size(); ++i)
    {
        pipes[i] = reading_step_getter(std::move(result_layers[i]));
        auto & filter_function = filters[i];
        if (!filter_function)
            continue;
        auto syntax_result = TreeRewriter(context).analyze(filter_function, primary_key.expression->getRequiredColumnsWithTypes());
        auto actions = ExpressionAnalyzer(filter_function, syntax_result, context).getActionsDAG(false);
        ExpressionActionsPtr expression_actions = std::make_shared<ExpressionActions>(std::move(actions));
        auto description = fmt::format(
            "filter values in [{}, {})", i ? ::toString(borders[i - 1]) : "-inf", i < borders.size() ? ::toString(borders[i]) : "+inf");
        pipes[i].addSimpleTransform(
            [&](const Block & header)
            {
                auto step = std::make_shared<FilterTransform>(header, expression_actions, filter_function->getColumnName(), true);
                step->setDescription(description);
                return step;
            });
    }
    return pipes;
}

}
