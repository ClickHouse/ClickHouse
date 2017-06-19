#include <Interpreters/InterpreterSelectWithUnionQuery.h>


namespace DB
{

InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
    size_t subquery_depth_ = 0)
    : query_ptr(query_ptr_),
    context(context_),
    to_stage(to_stage_),
    subquery_depth(subquery_depth_)
{
}


InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const Names & required_column_names,
    QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
    size_t subquery_depth_ = 0)
    : query_ptr(query_ptr_),
    context(context_),
    to_stage(to_stage_),
    subquery_depth(subquery_depth_)
{
}

}
