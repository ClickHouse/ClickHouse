#include <Processors/QueryPlan/EvaluateCommonSubqueryStep.h>

#include <Processors/Transforms/EvaluateCommonSubqueryTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/ITransformingStep.h>

#include <cstddef>
#include <unordered_set>

namespace DB
{

EvaluateCommonSubqueryStep::EvaluateCommonSubqueryStep(
    const SharedHeader & header_,
    ColumnIdentifiers columns_to_save_,
    StoragePtr storage_,
    ContextPtr context_
) : ITransformingStep(header_, header_, Traits{
        .data_stream_traits = {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true
        },
        .transform_traits = {
            .preserves_number_of_rows = true
        }
    })
    , storage(std::move(storage_))
    , context(std::move(context_))
    , columns_to_save(std::move(columns_to_save_))
{

    if (!storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage is not set for EvaluateCommonSubqueryStep");
}

void EvaluateCommonSubqueryStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &  /*settings*/)
{
    const auto & input_header = getInputHeaders().front();

    std::vector<size_t> columns_to_save_indices;
    columns_to_save_indices.reserve(columns_to_save.size());
    for (const auto & column : columns_to_save)
    {
        auto index = input_header->getPositionByName(column);
        columns_to_save_indices.push_back(index);
    }

    auto common_header = std::make_shared<Block>();
    for (size_t index : columns_to_save_indices)
        common_header->insert(input_header->getByPosition(index));

    pipeline.addSimpleTransform(
        [this, &common_header, &columns_to_save_indices](const SharedHeader & in_header)
        {
            return std::make_shared<EvaluateCommonSubqueryTransform>(
                in_header,
                common_header,
                storage,
                context,
                columns_to_save_indices
            );
        });
}

}
