#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class IInterpreterUnionOrSelectQuery : public IInterpreter
{
public:
    IInterpreterUnionOrSelectQuery(const ASTPtr & query_ptr_, const Context & context_, const SelectQueryOptions & options_)
        : query_ptr(query_ptr_)
        , context(std::make_shared<Context>(context_))
        , options(options_)
        , max_streams(context->getSettingsRef().max_threads)
    {
    }

    virtual void buildQueryPlan(QueryPlan & query_plan) = 0;

    virtual void ignoreWithTotals() = 0;

    virtual ~IInterpreterUnionOrSelectQuery() override = default;

    Block getSampleBlock() { return result_header; }

    size_t getMaxStreams() const { return max_streams; }

protected:
    ASTPtr query_ptr;
    std::shared_ptr<Context> context;
    Block result_header;
    SelectQueryOptions options;
    size_t max_streams = 1;
};
}

