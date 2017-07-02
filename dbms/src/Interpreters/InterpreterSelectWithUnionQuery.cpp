#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    QueryProcessingStage::Enum to_stage_,
    size_t subquery_depth_)
    : query_ptr(query_ptr_),
    context(context_),
    to_stage(to_stage_),
    subquery_depth(subquery_depth_)
{
    size_t num_selects = query_ptr->children.size();
    nested_interpreters.reserve(num_selects);

    if (!num_selects)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    for (const auto & select : query_ptr->children)
        nested_interpreters.emplace_back(std::make_unique<InterpreterSelectQuery>(select, context, to_stage, subquery_depth));
}


InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const Names & required_column_names,
    QueryProcessingStage::Enum to_stage_,
    size_t subquery_depth_)
    : query_ptr(query_ptr_),
    context(context_),
    to_stage(to_stage_),
    subquery_depth(subquery_depth_)
{
    size_t num_selects = query_ptr->children.size();
    nested_interpreters.reserve(num_selects);

    if (!num_selects)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    for (const auto & select : query_ptr->children)
        nested_interpreters.emplace_back(std::make_unique<InterpreterSelectQuery>(select, context, required_column_names, to_stage, subquery_depth));
}


InterpreterSelectWithUnionQuery::~InterpreterSelectWithUnionQuery() = default;


DataTypes InterpreterSelectWithUnionQuery::getReturnTypes()
{
    return nested_interpreters.front()->getReturnTypes();
}

Block InterpreterSelectWithUnionQuery::getSampleBlock()
{
    return nested_interpreters.front()->getSampleBlock();
}

Block InterpreterSelectWithUnionQuery::getSampleBlock(
    const ASTPtr & query_ptr,
    const Context & context)
{
    if (query_ptr->children.empty())
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    return InterpreterSelectQuery::getSampleBlock(query_ptr->children.front(), context);
}


BlockInputStreams InterpreterSelectWithUnionQuery::executeWithoutUnion()
{
    BlockInputStreams nested_streams;

    for (auto & interpreter : nested_interpreters)
    {
        BlockInputStreams streams = interpreter->executeWithoutUnion();
        nested_streams.insert(nested_streams.end(), streams.begin(), streams.end());
    }

    return nested_streams;
}


BlockIO InterpreterSelectWithUnionQuery::execute()
{
    BlockInputStreams nested_streams = executeWithoutUnion();
    BlockInputStreamPtr result_stream;

    if (nested_streams.empty())
    {
        result_stream = std::make_shared<NullBlockInputStream>();
    }
    else if (nested_streams.size() == 1)
    {
        result_stream = nested_streams.front();
    }
    else
    {
        const Settings & settings = context.getSettingsRef();

        result_stream = std::make_shared<UnionBlockInputStream<>>(nested_streams, nullptr /* TODO stream_with_non_joined_data */, settings.max_threads);
        nested_streams.clear();
    }

    /// Constraints on the result, the quota on the result, and also callback for progress.
    if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(result_stream.get()))
    {
        /// Constraints apply only to the final result.
        if (to_stage == QueryProcessingStage::Complete)
        {
            const Settings & settings = context.getSettingsRef();

            IProfilingBlockInputStream::LocalLimits limits;
            limits.mode = IProfilingBlockInputStream::LIMITS_CURRENT;
            limits.max_rows_to_read = settings.limits.max_result_rows;
            limits.max_bytes_to_read = settings.limits.max_result_bytes;
            limits.read_overflow_mode = settings.limits.result_overflow_mode;

            stream->setLimits(limits);
            stream->setQuota(context.getQuota());
        }
    }

    BlockIO res;
    res.in = result_stream;
    res.in_sample = getSampleBlock();

    return res;
}

}
