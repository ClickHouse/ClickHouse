#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterSelectQuery;


/** Interprets one or multiple SELECT queries inside UNION ALL chain.
  */
class InterpreterSelectWithUnionQuery : public IInterpreter
{
public:
    InterpreterSelectWithUnionQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
        size_t subquery_depth_ = 0);

    InterpreterSelectWithUnionQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const Names & required_column_names,
        QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
        size_t subquery_depth_ = 0);

    ~InterpreterSelectWithUnionQuery();

    BlockIO execute() override;

    /// Execute the query without union of streams.
    BlockInputStreams executeWithoutUnion();

    DataTypes getReturnTypes();
    Block getSampleBlock();

    static Block getSampleBlock(
        const ASTPtr & query_ptr_,
        const Context & context_);

private:
    ASTPtr query_ptr;
    Context context;
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;

    std::vector<std::unique_ptr<InterpreterSelectQuery>> nested_interpreters;
};

}
