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
#include <Processors/Transforms/FilterSortedStreamByRange.h>
#include <Storages/MergeTree/RangesInDataPart.h>

using namespace DB;

namespace
{

using Values = std::vector<Field>;

std::string toString(const Values & value)
{
    return fmt::format("({})", fmt::join(value, ", "));
}

/// Adaptor to access PK values from index.
class IndexAccess
{
public:
    explicit IndexAccess(const RangesInDataParts & parts_) : parts(parts_) { }

    Values getValue(size_t part_idx, size_t mark) const
    {
        const auto & index = parts[part_idx].data_part->index;
        Values values(index.size());
        for (size_t i = 0; i < values.size(); ++i)
            index[i]->get(mark, values[i]);
        return values;
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


/// Splits parts into layers, each layer will contain parts subranges with PK values from its own range.
/// Will try to produce exactly max_layer layers but may return less if data is distributed in not a very parallelizable way.
std::pair<std::vector<Values>, std::vector<RangesInDataParts>> split(RangesInDataParts parts, size_t max_layers)
{
    // We will advance the iterator pointing to the mark with the smallest PK value until there will be not less than rows_per_layer rows in the current layer (roughly speaking).
    // Then we choose the last observed value as the new border, so the current layer will consists of granules with values greater than the previous mark and less or equal
    // than the new border.

    struct PartsRangesIterator
    {
        struct MarkRangeWithPartIdx : MarkRange
        {
            size_t part_idx;
        };

        enum class EventType
        {
            RangeStart,
            RangeEnd,
        };

        bool operator<(const PartsRangesIterator & other) const { return std::tie(value, event) > std::tie(other.value, other.event); }

        Values value;
        MarkRangeWithPartIdx range;
        EventType event;
    };

    const auto index_access = std::make_unique<IndexAccess>(parts);
    std::priority_queue<PartsRangesIterator> parts_ranges_queue;
    for (size_t part_idx = 0; part_idx < parts.size(); ++part_idx)
    {
        for (const auto & range : parts[part_idx].ranges)
        {
            parts_ranges_queue.push(
                {index_access->getValue(part_idx, range.begin), {range, part_idx}, PartsRangesIterator::EventType::RangeStart});
            const auto & index_granularity = parts[part_idx].data_part->index_granularity;
            if (index_granularity.hasFinalMark() && range.end + 1 == index_granularity.getMarksCount())
                parts_ranges_queue.push(
                    {index_access->getValue(part_idx, range.end), {range, part_idx}, PartsRangesIterator::EventType::RangeEnd});
        }
    }

    /// The beginning of currently started (but not yet finished) range of marks of a part in the current layer.
    std::unordered_map<size_t, size_t> current_part_range_begin;
    /// The current ending of a range of marks of a part in the current layer.
    std::unordered_map<size_t, size_t> current_part_range_end;

    /// Determine borders between layers.
    std::vector<Values> borders;
    std::vector<RangesInDataParts> result_layers;

    const size_t rows_per_layer = std::max<size_t>(index_access->getTotalRowCount() / max_layers, 1);

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

        result_layers.emplace_back();

        while (rows_in_current_layer < rows_per_layer || layers_intersection_is_too_big() || result_layers.size() == max_layers)
        {
            // We're advancing iterators until a new value showed up.
            Values last_value;
            while (!parts_ranges_queue.empty() && (last_value.empty() || last_value == parts_ranges_queue.top().value))
            {
                auto current = parts_ranges_queue.top();
                parts_ranges_queue.pop();
                const auto part_idx = current.range.part_idx;

                if (current.event == PartsRangesIterator::EventType::RangeEnd)
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

    return {std::move(borders), std::move(result_layers)};
}


/// Will return borders.size()+1 filters in total, i-th filter will accept rows with PK values within the range [borders[i-1], borders[i]).
ASTs buildFilters(const KeyDescription & primary_key, const std::vector<Values> & borders)
{
    auto add_and_condition = [&](ASTPtr & result, const ASTPtr & foo) { result = (!result) ? foo : makeASTFunction("and", result, foo); };

    /// Produces ASTPtr to predicate (pk_col0, pk_col1, ... , pk_colN) > (value[0], value[1], ... , value[N]), possibly with conversions.
    /// For example, if table PK is (a, toDate(d)), where `a` is UInt32 and `d` is DateTime, and PK columns values are (8192, 19160),
    /// it will build the following predicate: greater(tuple(a, toDate(d)), tuple(8192, cast(19160, 'Date'))).
    auto lexicographically_greater = [&](const Values & value)
    {
        // PK may contain functions of the table columns, so we need the actual PK AST with all expressions it contains.
        ASTPtr pk_columns_as_tuple = makeASTFunction("tuple", primary_key.expression_list_ast->children);

        ASTPtr value_ast = std::make_shared<ASTExpressionList>();
        for (size_t i = 0; i < value.size(); ++i)
        {
            const auto & types = primary_key.data_types;
            ASTPtr component_ast = std::make_shared<ASTLiteral>(value[i]);
            // Values of some types (e.g. Date, DateTime) are stored in columns as numbers and we get them as just numbers from the index.
            // So we need an explicit Cast for them.
            if (isColumnedAsNumber(types.at(i)->getTypeId()) && !isNumber(types.at(i)->getTypeId()))
                component_ast = makeASTFunction("cast", std::move(component_ast), std::make_shared<ASTLiteral>(types.at(i)->getName()));
            value_ast->children.push_back(std::move(component_ast));
        }
        ASTPtr values_as_tuple = makeASTFunction("tuple", value_ast->children);

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

Pipes buildPipesForReadingByPKRanges(
    const KeyDescription & primary_key,
    RangesInDataParts parts,
    size_t max_layers,
    ContextPtr context,
    ReadingInOrderStepGetter && reading_step_getter)
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
        auto pk_expression = std::make_shared<ExpressionActions>(primary_key.expression->getActionsDAG().clone());
        pipes[i].addSimpleTransform([pk_expression](const Block & header)
                                    { return std::make_shared<ExpressionTransform>(header, pk_expression); });
        pipes[i].addSimpleTransform(
            [&](const Block & header)
            {
                auto step = std::make_shared<FilterSortedStreamByRange>(header, expression_actions, filter_function->getColumnName(), true);
                step->setDescription(description);
                return step;
            });
    }
    return pipes;
}

}
