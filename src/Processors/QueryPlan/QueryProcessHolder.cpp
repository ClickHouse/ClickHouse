#include <Processors/QueryPlan/QueryProcessHolder.h>
#include <Interpreters/QueryProcess.h>
#include <Common/Exception.h>

namespace DB
{

QueryProcessHolder::QueryProcessHolder(const ASTPtr & ast_, bool internal_, const std::shared_ptr<Context> context_)
    : context(context_)
    , query_process(std::make_shared<QueryProcess>(ast_, internal_, *context))
{
}

void QueryProcessHolder::initialize()
{
    if (initialized)
        return;

    initialized = true;

    query_process->queryStart();
}

QueryProcessHolder::~QueryProcessHolder()
{
    /// FIXME:
    /// It can not initialized if multiple QueryProcessHolder was created, but
    /// only one will be used eventually, see QueryPlan::addQueryProcessHolder()
    /// for more comments.
    ///
    /// NOTE: replace with assert(initialized) one will be resolved
    if (!initialized)
        return;

    try
    {
        /// FIXME: pass correct arguments
        query_process->queryFinish(nullptr, nullptr, nullptr);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
