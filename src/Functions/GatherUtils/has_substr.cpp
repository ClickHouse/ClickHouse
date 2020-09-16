#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

struct ArrayHasSubstrSelectArraySourcePair : public ArraySourcePairSelector<ArrayHasSelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void selectSourcePair(FirstSource && first, SecondSource && second, ArraySearchType search_type, ColumnUInt8 & result)
    {
        arrayAllAny<ArraySearchType::Substr>(first, second, result);
    }
};


void sliceHasSubstr(IArraySource & first, IArraySource & second, ColumnUInt8 & result)
{
    ArrayHasSubstrSelectArraySourcePair::select(first, second, result);
}

}
