#include <Functions/GatherUtils/GatherUtils.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

}

namespace DB::GatherUtils
{

void sliceHas(IArraySource & first, IArraySource & second, ArraySearchType search_type, ColumnUInt8 & result)
{
    switch (search_type)
    {
        case ArraySearchType::All:
            sliceHasAll(first, second, result);
            break;
        case ArraySearchType::Any:
            sliceHasAny(first, second, result);
            break;
        case ArraySearchType::Substr:
            sliceHasSubstr(first, second, result);
            break;
        case ArraySearchType::StartsWith:
            sliceHasStartsWith(first, second, result);
            break;
        case ArraySearchType::EndsWith:
            sliceHasEndsWith(first, second, result);
            break;
    }
}

void checkQueryCancellation()
{
    if (CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
}

}
