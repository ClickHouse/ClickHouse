#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

struct ArrayHasSelectArraySourcePair : public ArraySourcePairSelector<ArrayHasSelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void selectSourcePair(FirstSource && first, SecondSource && second, bool all, ColumnUInt8 & result)
    {
        if (all)
            arrayAllAny<true>(first, second, result);
        else
            arrayAllAny<false>(first, second, result);
    }
};

void sliceHas(IArraySource & first, IArraySource & second, bool all, ColumnUInt8 & result)
{
    ArrayHasSelectArraySourcePair::select(first, second, all, result);
}

}
