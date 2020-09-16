#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

struct ArrayHasAllSelectArraySourcePair : public ArraySourcePairSelector<ArrayHasSelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void selectSourcePair(FirstSource && first, SecondSource && second, ColumnUInt8 & result)
    {
        arrayAllAny<ArraySearchType::All>(first, second, result);
    }
};


void sliceHasAll(IArraySource & first, IArraySource & second, ColumnUInt8 & result)
{
    ArrayHasAllSelectArraySourcePair::select(first, second, result);
}

}
