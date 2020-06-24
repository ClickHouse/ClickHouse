#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPipeline.h>
#include <Processors/LimitTransform.h>

namespace DB
{

static ITransformingStep::DataStreamTraits getTraits()
{
    return ITransformingStep::DataStreamTraits
    {
            .preserves_distinct_columns = true
    };
}

LimitStep::LimitStep(
    const DataStream & input_stream_,
    size_t limit_, size_t offset_,
    bool always_read_till_end_,
    bool with_ties_,
    SortDescription description_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , limit(limit_), offset(offset_)
    , always_read_till_end(always_read_till_end_)
    , with_ties(with_ties_), description(std::move(description_))
{

}

void LimitStep::transformPipeline(QueryPipeline & pipeline)
{
    auto transform = std::make_shared<LimitTransform>(
        pipeline.getHeader(), limit, offset, pipeline.getNumStreams(), always_read_till_end, with_ties, description);

    pipeline.addPipe({std::move(transform)});
}

Strings LimitStep::describeActions() const
{
    Strings res;
    res.emplace_back("Limit " + std::to_string(limit));
    res.emplace_back("Offset " + std::to_string(offset));

    if (with_ties || always_read_till_end)
    {
        String str;
        if (with_ties)
            str += "WITH TIES";

        if (always_read_till_end)
        {
            if (!str.empty())
                str += ", ";

            str += "Reads all data";
        }

        res.emplace_back(str);
    }

    return res;
}

}
