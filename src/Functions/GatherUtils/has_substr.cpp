#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

namespace
{

struct ArrayHasSubstrSelectArraySourcePair : public ArraySourcePairSelector<ArrayHasSubstrSelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void selectSourcePair(FirstSource && first, SecondSource && second, ColumnUInt8 & result)
    {
        arrayAllAny<ArraySearchType::Substr>(first, second, result);
    }
};

}

void sliceHasSubstr(IArraySource & first, IArraySource & second, ColumnUInt8 & result)
{
    ArrayHasSubstrSelectArraySourcePair::select(first, second, result);
}

}
