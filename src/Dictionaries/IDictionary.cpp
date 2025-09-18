#include <Common/CurrentThread.h>
#include <Dictionaries/IDictionary.h>
#include <Interpreters/Context.h>


namespace DB
{

std::pair<std::unique_ptr<CurrentThread::QueryScope>, ContextMutablePtr> IDictionary::createThreadGroupIfNeeded(ContextPtr context)
{
    auto thread_group = CurrentThread::getGroup();

    if (!thread_group)
    {
        ContextMutablePtr result_context = Context::createCopy(context);
        result_context->makeQueryContext();
        result_context->setCurrentQueryId("");
        return {std::make_unique<CurrentThread::QueryScope>(result_context), std::move(result_context)};
    }

    if (auto thread_group_thread_context = thread_group->query_context.lock())
    {
        ContextMutablePtr result_context = Context::createCopy(thread_group_thread_context);
        result_context->makeQueryContext();
        result_context->setCurrentQueryId("");
        return {nullptr, std::move(result_context)};
    }

    /// TODO(mstetsyuk): try and make it UNREACHABLE

    ContextMutablePtr result = Context::createCopy(context);
    result->makeQueryContext();
    result->setCurrentQueryId("");
    return {nullptr, std::move(result)};
}

}
