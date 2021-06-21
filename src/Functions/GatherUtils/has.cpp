#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

struct ArrayHasSelectArraySourcePair : public ArraySourcePairSelector<ArrayHasSelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void selectSourcePair(FirstSource && first, SecondSource && second, ArraySearchType search_type, ColumnUInt8 & result)
    {
        switch (search_type)
        {
            case ArraySearchType::All:
                arrayAllAny<ArraySearchType::All>(first, second, result);
                break;
            case ArraySearchType::Any:
                arrayAllAny<ArraySearchType::Any>(first, second, result);
                break;
            case ArraySearchType::Substr:
                arrayAllAny<ArraySearchType::Substr>(first, second, result);
                break;

        }
    }
};


void sliceHas(IArraySource & first, IArraySource & second, ArraySearchType search_type, ColumnUInt8 & result)
{
    ArrayHasSelectArraySourcePair::select(first, second, search_type, result);
}

}
