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
    static void callFunction(FirstSource && first,
                             bool is_second_const, bool is_second_nullable, SecondSource && second,
                             ColumnUInt8 & result)
    {
        using SourceType = typename std::decay<SecondSource>::type;

        if (is_second_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;

            if (is_second_const)
                arrayAllAny<ArraySearchType::Substr>(first, static_cast<ConstSource<NullableSource> &>(second), result);
            else
                arrayAllAny<ArraySearchType::Substr>(first, static_cast<NullableSource &>(second), result);
        }
        else
        {
            if (is_second_const)
                arrayAllAny<ArraySearchType::Substr>(first, static_cast<ConstSource<SourceType> &>(second), result);
            else
                arrayAllAny<ArraySearchType::Substr>(first, second, result);
        }
    }

    template <typename FirstSource, typename SecondSource>
    static void selectSourcePair(bool is_first_const, bool is_first_nullable, FirstSource && first,
                                 bool is_second_const, bool is_second_nullable, SecondSource && second,
                                 ColumnUInt8 & result)
    {
        using SourceType = typename std::decay<FirstSource>::type;

        if (is_first_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;

            if (is_first_const)
                callFunction(static_cast<ConstSource<NullableSource> &>(first), is_second_const, is_second_nullable, second, result);
            else
                callFunction(static_cast<NullableSource &>(first), is_second_const, is_second_nullable, second, result);
        }
        else
        {
            if (is_first_const)
                callFunction(static_cast<ConstSource<SourceType> &>(first), is_second_const, is_second_nullable, second, result);
            else
                callFunction(first, is_second_const, is_second_nullable, second, result);
        }
    }
};

}

void sliceHasSubstr(IArraySource & first, IArraySource & second, ColumnUInt8 & result)
{
    ArrayHasSubstrSelectArraySourcePair::select(first, second, result);
}

}
