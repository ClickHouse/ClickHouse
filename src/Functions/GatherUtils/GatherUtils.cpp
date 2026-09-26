#include <Functions/GatherUtils/GatherUtils.h>

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

}
