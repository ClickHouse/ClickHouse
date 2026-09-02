#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Selectors.h>
#include <Functions/GatherUtils/Algorithms.h>

namespace DB::GatherUtils
{

namespace
{

struct ArrayHasSubstrSelectArraySourcePair : public ArraySourcePairSelector<ArrayHasSubstrSelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void callFunction(FirstSource && first,
                             bool is_second_const, bool is_second_nullable, SecondSource && second,
                             UInt8 * result, size_t size)
    {
        using SourceType = typename std::decay_t<SecondSource>;

        if (is_second_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;

            if (is_second_const)
                arrayAllAny<ArraySearchType::Substr>(first, static_cast<ConstSource<NullableSource> &>(second), result, size);
            else
                arrayAllAny<ArraySearchType::Substr>(first, static_cast<NullableSource &>(second), result, size);
        }
        else
        {
            if (is_second_const)
                arrayAllAny<ArraySearchType::Substr>(first, static_cast<ConstSource<SourceType> &>(second), result, size);
            else
                arrayAllAny<ArraySearchType::Substr>(first, second, result, size);
        }
    }

    template <typename Source>
    static void selectSourcePair(bool is_first_const, bool is_first_nullable, Source && first,
                                 bool is_second_const, bool is_second_nullable, Source && second,
                                 UInt8 * result, size_t size)
    {
        using SourceType = typename std::decay_t<Source>;

        if (is_first_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;

            if (is_first_const)
                callFunction(static_cast<ConstSource<NullableSource> &>(first), is_second_const, is_second_nullable, second, result, size);
            else
                callFunction(static_cast<NullableSource &>(first), is_second_const, is_second_nullable, second, result, size);
        }
        else
        {
            if (is_first_const)
                callFunction(static_cast<ConstSource<SourceType> &>(first), is_second_const, is_second_nullable, second, result, size);
            else
                callFunction(first, is_second_const, is_second_nullable, second, result, size);
        }
    }
};

}

void sliceHasSubstr(IArraySource & first, IArraySource & second, ColumnUInt8 & result)
{
    ArrayHasSubstrSelectArraySourcePair::select(first, second, result.getData().data(), result.size());
}

}
