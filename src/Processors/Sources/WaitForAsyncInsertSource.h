#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

/// Source, that allow to wait until processing of
/// asynchronous insert for specified query_id will be finished.
class WaitForAsyncInsertSource : public ISource, WithContext
{
public:
    WaitForAsyncInsertSource(
        bool wait_, /// Whether the source should wait for query completion or not
        size_t timeout_ms_, /// The timeout for the wait
        ASTPtr query_, /// The query's AST
        ContextPtr query_context_, /// The query's context
        String && bytes_ /// Data extracted from the query as returned by pushCheckOnly
        )
        : ISource(Block())
        , WithContext(query_context_)
        , wait(wait_)
        , timeout_ms(timeout_ms_)
        , query(std::move(query_))
        , bytes(std::move(bytes_))
    {
    }

    String getName() const override { return "WaitForAsyncInsert"; }

protected:
    Chunk generate() override;

private:
    bool wait;
    size_t timeout_ms;
    ASTPtr query;
    String bytes;
};

}
