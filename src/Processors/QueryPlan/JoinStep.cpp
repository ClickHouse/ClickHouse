#include <IO/Operators.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <IO/Operators.h>
// #include "Common/FieldVisitorToString.h"
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnSet.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Processors/Transforms/ColumnPermuteTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

std::vector<std::pair<String, String>> describeJoinActions(const JoinPtr & join)
{
    std::vector<std::pair<String, String>> description;
    const auto & table_join = join->getTableJoin();

    description.emplace_back("Type", toString(table_join.kind()));
    description.emplace_back("Strictness", toString(table_join.strictness()));
    description.emplace_back("Algorithm", join->getName());

    if (table_join.strictness() == JoinStrictness::Asof)
        description.emplace_back("ASOF inequality", toString(table_join.getAsofInequality()));

    if (!table_join.getClauses().empty())
        description.emplace_back("Clauses", TableJoin::formatClauses(table_join.getClauses(), true /*short_format*/));

    return description;
}

std::vector<size_t> getPermutationForBlock(
    const Block & block,
    const Block & lhs_block,
    const Block & rhs_block,
    const NameSet & name_filter)
{
    std::vector<size_t> permutation;
    permutation.reserve(block.columns());
    Block::NameMap name_map = block.getNamesToIndexesMap();

    bool is_trivial = true;
    for (const auto & other_block : {lhs_block, rhs_block})
    {
        for (const auto & col : other_block)
        {
            if (!name_filter.contains(col.name))
                continue;
            if (auto it = name_map.find(col.name); it != name_map.end())
            {
                is_trivial = is_trivial && it->second == permutation.size();
                permutation.push_back(it->second);
            }
        }
    }

    if (is_trivial && permutation.size() == block.columns())
        return {};

    return permutation;
}

ColumnsWithTypeAndName squashBlocks(const Names & keys, const BlocksList & blocks)
{
    ColumnsWithTypeAndName squashed;
    std::vector<size_t> positions;

    auto log = getLogger("squashBlocks");

    // std::cerr << "===== " << blocks.front().dumpStructure() << std::endl;
    for (const auto & name : keys)
    {
        // std::cerr << ".... " << name << std::endl;
        positions.push_back(blocks.front().getPositionByName(name));
    }

    bool first = true;
    for (const auto & block : blocks)
    {
        // std::cerr << "===== " << block.dumpStructure() << std::endl;
        // for (size_t row = 0; row < block.getByPosition(0).column->size(); ++row)
        //     LOG_TRACE(log, "{} : {}", row, applyVisitor(FieldVisitorToString(), (*block.getByPosition(0).column)[row]));

        if (first)
        {
            first = false;
            for (size_t pos : positions)
            {
                squashed.push_back(blocks.front().getByPosition(pos));
                squashed.back().column = recursiveRemoveSparse(squashed.back().column);
            }
            continue;
        }

        for (size_t i = 0; i < positions.size(); ++i)
        {
            auto & sq_col = squashed[i];
            auto col_mutable = IColumn::mutate(std::move(sq_col.column));

            auto rhs_col = recursiveRemoveSparse(block.getByPosition(positions[i]).column);
            size_t rows = rhs_col->size();

            col_mutable->insertRangeFrom(*rhs_col, 0, rows);
            sq_col.column = std::move(col_mutable);
        }
    }

    return squashed;
}

}

void DynamicJoinFilters::filterDynamicPartsByFilledJoin(const IJoin & join)
{
    auto log = getLogger("DynamicJoinFilter");
    LOG_TRACE(log, "DynamicJoinFilters::filterDynamicPartsByFilledJoin parts {}", parts != nullptr);
    if (!parts)
        return;

    const auto * hash_join = typeid_cast<const HashJoin *>(&join);
    if (!hash_join)
        return;

    const auto & blocks = hash_join->getJoinedData()->right_key_columns_for_filter;
    if (blocks.empty())
        return;

    const auto & settings = context->getSettingsRef();

    for (size_t i = 0; i < clauses.size(); ++i)
    {
        const auto & clause = clauses[i];
        auto squashed = squashBlocks(clause.keys, blocks[i]);

        // std::cerr << "Right join data rows " << squashed.front().column->size() << std::endl;

        auto set = std::make_shared<FutureSetFromTuple>(squashed, settings);
        clause.set->setData(std::move(set));
    }

    const auto & primary_key = metadata->getPrimaryKey();
    const Names & primary_key_column_names = primary_key.column_names;

    KeyCondition key_condition(&actions, context, primary_key_column_names, primary_key.expression);
    LOG_TRACE(log, "Key condition: {}", key_condition.toString());

    auto parts_with_lock = parts->parts_ranges_ptr->get();

    size_t prev_marks = 0;
    size_t post_marks = 0;

    for (auto & part_range : parts_with_lock.parts_ranges)
    {
        MarkRanges filtered_ranges;
        for (auto & range : part_range.ranges)
        {
            prev_marks += range.getNumberOfMarks();

            // std::cerr << "Range " << range.begin << ' ' << range.end << " has final mark " << part_range.data_part->index_granularity->hasFinalMark() << std::endl;
            auto new_ranges = MergeTreeDataSelectExecutor::markRangesFromPKRange(
                part_range.data_part,
                range.begin,
                range.end,
                metadata,
                key_condition,
                {}, nullptr, settings, log);

            for (auto & new_range : new_ranges)
            {
                // std::cerr << "New Range " << new_range.begin << ' ' << new_range.end << std::endl;
                if (new_range.getNumberOfMarks())
                {
                    post_marks += new_range.getNumberOfMarks();
                    filtered_ranges.push_back(new_range);
                }
            }
        }

        part_range.ranges = std::move(filtered_ranges);
    }

    LOG_TRACE(log, "Reading {}/{} marks after filtration.", post_marks, prev_marks);
}


JoinStep::JoinStep(
    const Header & left_header_,
    const Header & right_header_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t min_block_size_bytes_,
    size_t max_streams_,
    NameSet required_output_,
    bool keep_left_read_in_order_,
    bool use_new_analyzer_)
    : join(std::move(join_))
    , max_block_size(max_block_size_)
    , min_block_size_bytes(min_block_size_bytes_)
    , max_streams(max_streams_)
    , required_output(std::move(required_output_))
    , keep_left_read_in_order(keep_left_read_in_order_)
    , use_new_analyzer(use_new_analyzer_)
{
    updateInputHeaders({left_header_, right_header_});
}

void JoinStep::setDynamicFilter(DynamicJoinFiltersPtr dynamic_filter_)
{
    dynamic_filter = std::move(dynamic_filter_);
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    Block lhs_header = pipelines[0]->getHeader();
    Block rhs_header = pipelines[1]->getHeader();

    if (swap_streams)
        std::swap(pipelines[0], pipelines[1]);

    std::unique_ptr<QueryPipelineBuilder> joined_pipeline;
    if (join->pipelineType() == JoinPipelineType::YShaped)
    {
        joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
            std::move(pipelines[0]), std::move(pipelines[1]), join, join_algorithm_header, max_block_size, &processors);
        joined_pipeline->resize(max_streams);
    }
    else
    {
        auto finish_callback = [filter = this->dynamic_filter, algo = this->join]()
        {
            // LOG_TRACE(getLogger("JoinStep"), "Finish callback called. Filter {}", filter != nullptr);
            if (filter)
                filter->filterDynamicPartsByFilledJoin(*algo);
        };

        joined_pipeline = QueryPipelineBuilder::joinPipelinesRightLeft(
            std::move(pipelines[0]),
            std::move(pipelines[1]),
            join,
            std::move(finish_callback),
            join_algorithm_header,
            max_block_size,
            min_block_size_bytes,
            max_streams,
            keep_left_read_in_order,
            &processors);
    }

    if (!use_new_analyzer)
        return joined_pipeline;

    auto column_permutation = getPermutationForBlock(joined_pipeline->getHeader(), lhs_header, rhs_header, required_output);
    if (!column_permutation.empty())
    {
        joined_pipeline->addSimpleTransform([&column_permutation](const Block & header)
        {
            return std::make_shared<ColumnPermuteTransform>(header, column_permutation);
        });
    }

    if (join->supportParallelJoin())
    {
        joined_pipeline->addSimpleTransform([&](const Block & header)
                                     { return std::make_shared<SimpleSquashingChunksTransform>(header, 0, min_block_size_bytes); });
    }

    return joined_pipeline;
}

bool JoinStep::allowPushDownToRight() const
{
    return join->pipelineType() == JoinPipelineType::YShaped || join->pipelineType() == JoinPipelineType::FillRightFirst;
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    for (const auto & [name, value] : describeJoinActions(join))
        settings.out << prefix << name << ": " << value << '\n';
    if (swap_streams)
        settings.out << prefix << "Swapped: true\n";

    if (dynamic_filter)
    {
        settings.out << prefix << "Dynamic Filter\n";
        auto expression = std::make_shared<ExpressionActions>(dynamic_filter->actions.clone());
        expression->describeActions(settings.out, prefix);
    }
}

void JoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join))
        map.add(name, value);
    if (swap_streams)
        map.add("Swapped", true);
}

void JoinStep::setJoin(JoinPtr join_, bool swap_streams_)
{
    join_algorithm_header.clear();
    swap_streams = swap_streams_;
    join = std::move(join_);
    updateOutputHeader();
}

void JoinStep::updateOutputHeader()
{
    if (join_algorithm_header)
        return;

    const auto & header = swap_streams ? input_headers[1] : input_headers[0];

    Block result_header = JoiningTransform::transformHeader(header, join);
    join_algorithm_header = result_header;

    if (!use_new_analyzer)
    {
        if (swap_streams)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot swap streams without new analyzer");
        output_header = result_header;
        return;
    }

    auto column_permutation = getPermutationForBlock(result_header, input_headers[0], input_headers[1], required_output);
    if (!column_permutation.empty())
        result_header = ColumnPermuteTransform::permute(result_header, column_permutation);

    output_header = result_header;
}

static ITransformingStep::Traits getStorageJoinTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilledJoinStep::FilledJoinStep(const Header & input_header_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_header_,
        JoiningTransform::transformHeader(input_header_, join_),
        getStorageJoinTraits())
    , join(std::move(join_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FilledJoinStep expects Join to be filled");
}

void FilledJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    bool default_totals = false;
    if (!pipeline.hasTotals() && join->getTotals())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(header, *output_header, join, max_block_size, on_totals, default_totals, counter);
    });
}

void FilledJoinStep::updateOutputHeader()
{
    output_header = JoiningTransform::transformHeader(input_headers.front(), join);
}

void FilledJoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    for (const auto & [name, value] : describeJoinActions(join))
        settings.out << prefix << name << ": " << value << '\n';
}

void FilledJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join))
        map.add(name, value);
}

}
