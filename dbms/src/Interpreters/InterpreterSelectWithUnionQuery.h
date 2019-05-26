#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>


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
        const SelectQueryOptions &,
        const Names & required_result_column_names = {});

    ~InterpreterSelectWithUnionQuery() override;

    BlockIO execute() override;

    /// Execute the query without union of streams.
    BlockInputStreams executeWithMultipleStreams();

    Block getSampleBlock();

    static Block getSampleBlock(
        const ASTPtr & query_ptr_,
        const Context & context_);

    void ignoreWithTotals();

    ASTPtr getQuery() const { return query_ptr; }

private:
    const SelectQueryOptions options;
    ASTPtr query_ptr;
    Context context;

    std::vector<std::unique_ptr<InterpreterSelectQuery>> nested_interpreters;

    Block result_header;
};

}
