#include <Processors/QueryPlan/JoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnSet.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

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

}

JoinStep::JoinStep(
    const Header & left_header_,
    const Header & right_header_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_)
    : join(std::move(join_)), max_block_size(max_block_size_), max_streams(max_streams_), keep_left_read_in_order(keep_left_read_in_order_)
{
    updateInputHeaders({left_header_, right_header_});
}

void JoinStep::setDynamicParts(
    DynamiclyFilteredPartsRangesPtr dynamic_parts_,
    ActionsDAG dynamic_filter_,
    ColumnSet * column_set_,
    ContextPtr context_,
    StorageMetadataPtr metdata_)
{
    dynamic_parts = std::move(dynamic_parts_);
    dynamic_filter = std::move(dynamic_filter_);
    column_set = column_set_;
    context = std::move(context_);
    metdata = std::move(metdata_);
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    if (join->pipelineType() == JoinPipelineType::YShaped)
    {
        auto joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
            std::move(pipelines[0]), std::move(pipelines[1]), join, *output_header, max_block_size, &processors);
        joined_pipeline->resize(max_streams);
        return joined_pipeline;
    }

    auto finish_callback = [
        algo = this->join,
        parts = this->dynamic_parts,
        col_set = this->column_set,
        filter = std::make_shared<ActionsDAG>(std::move(this->dynamic_filter)),
        ctx = this->context,
        metadata_snapshot = this->metdata]()
    {
        if (!parts)
            return;

        const auto * hash_join = typeid_cast<const HashJoin *>(algo.get());
        if (!hash_join)
            return;

        const auto & blocks = hash_join->getJoinedData()->right_key_columns_for_filter;
        if (blocks.empty())
            return;

        ColumnsWithTypeAndName squashed;
        std::vector<size_t> positions;
        const auto & table_join = hash_join->getTableJoin();
        const auto & clause = table_join.getClauses().front();
        // std::cerr << "===== " << blocks.front().dumpStructure() << std::endl;
        for (const auto & name : clause.key_names_right)
        {
            // std::cerr << ".... " << name << std::endl;
            if (blocks.front().has(name))
                positions.push_back(blocks.front().getPositionByName(name));
        }

        if (positions.empty())
            return;

        bool first = true;
        for (const auto & block : blocks)
        {
            if (first)
            {
                first = false;
                for (size_t pos : positions)
                    squashed.push_back(blocks.front().getByPosition(pos));
                continue;
            }

            for (size_t i = 0; i < positions.size(); ++i)
            {
                auto & sq_col = squashed[i];
                auto col_mutable = IColumn::mutate(std::move(sq_col.column));

                const auto & rhs_col = block.getByPosition(positions[i]);
                size_t rows = rhs_col.column->size();

                col_mutable->insertRangeFrom(*rhs_col.column, 0, rows);
                sq_col.column = std::move(col_mutable);
            }
        }

        // std::cerr << "Right join data rows " << squashed.front().column->size() << std::endl;

        auto set = std::make_shared<FutureSetFromTuple>(squashed, ctx->getSettingsRef());
        col_set->setData(std::move(set));

        // std::cerr << ".... ccc " << reinterpret_cast<const void *>(col_set) << std::endl;

        const auto & primary_key = metadata_snapshot->getPrimaryKey();
        const Names & primary_key_column_names = primary_key.column_names;

        KeyCondition key_condition(filter.get(), ctx, primary_key_column_names, primary_key.expression);
        // std::cerr << "======== " << key_condition.toString() << std::endl;

        const auto & settings = ctx->getSettingsRef();
        auto log = getLogger("DynamicJoinFilter");

        auto parts_with_lock = parts->parts_ranges_ptr->get();
        for (auto & part_range : parts_with_lock.parts_ranges)
        {
            MarkRanges filtered_ranges;
            for (auto & range : part_range.ranges)
            {
                // std::cerr << "Range " << range.begin << ' ' << range.end << std::endl;
                auto new_ranges = MergeTreeDataSelectExecutor::markRangesFromPKRange(
                    part_range.data_part,
                    range.begin,
                    range.end,
                    metadata_snapshot,
                    key_condition,
                    {}, nullptr, settings, log);

                for (auto & new_range : new_ranges)
                {
                    // std::cerr << "New Range " << new_range.begin << ' ' << new_range.end << std::endl;
                    if (new_range.getNumberOfMarks())
                        filtered_ranges.push_back(new_range);
                }
            }

            part_range.ranges = std::move(filtered_ranges);
        }
    };

    return QueryPipelineBuilder::joinPipelinesRightLeft(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        std::move(finish_callback),
        *output_header,
        max_block_size,
        max_streams,
        keep_left_read_in_order,
        &processors);
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

    if (dynamic_parts)
    {
        settings.out << prefix << "Dynamic Filter\n";
        auto expression = std::make_shared<ExpressionActions>(dynamic_filter.clone());
        expression->describeActions(settings.out, prefix);
    }
}

void JoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join))
        map.add(name, value);
}

void JoinStep::updateOutputHeader()
{
    output_header = JoiningTransform::transformHeader(input_headers.front(), join);
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
