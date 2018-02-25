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


BlockInputStreams InterpreterSelectWithUnionQuery::executeWithMultipleStreams()
{
    BlockInputStreams nested_streams;

    for (auto & interpreter : nested_interpreters)
    {
        BlockInputStreams streams = interpreter->executeWithMultipleStreams();
        nested_streams.insert(nested_streams.end(), streams.begin(), streams.end());
    }

    return nested_streams;
}


BlockIO InterpreterSelectWithUnionQuery::execute()
{
    BlockInputStreams nested_streams = executeWithMultipleStreams();
    BlockInputStreamPtr result_stream;

    if (nested_streams.empty())
    {
        result_stream = std::make_shared<NullBlockInputStream>(getSampleBlock());
    }
    else if (nested_streams.size() == 1)
    {
        result_stream = nested_streams.front();
        nested_streams.clear();
    }
    else
    {
        const Settings & settings = context.getSettingsRef();
        result_stream = std::make_shared<UnionBlockInputStream<>>(nested_streams, nullptr, settings.max_threads);
        nested_streams.clear();
    }

    BlockIO res;
    res.in = result_stream;
    return res;
}


void InterpreterSelectWithUnionQuery::ignoreWithTotals()
{
    for (auto & interpreter : nested_interpreters)
        interpreter->ignoreWithTotals();
}

}
