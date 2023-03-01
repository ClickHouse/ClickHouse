#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace GatherUtils
{

namespace
{

struct ArrayConcat : public ArraySourceSelector<ArrayConcat>
{
    using Sources = std::vector<std::unique_ptr<IArraySource>>;

    template <typename Source>
    static void selectSource(bool /*is_const*/, bool is_nullable, Source & source, const Sources & sources, ColumnArray & result)
    {
        using SourceType = typename std::decay<Source>::type;
        using Sink = typename SourceType::SinkType;

        if (is_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;
            using NullableSink = typename NullableSource::SinkType;

            NullableSink sink(result.getData(), result.getOffsets(), source.getColumnSize());
            concat<NullableSource, NullableSink>(sources, std::move(sink));
        }
        else
        {
            Sink sink(result.getData(), result.getOffsets(), source.getColumnSize());
            concat<SourceType, Sink>(sources, std::move(sink));
        }
    }
};

}

void concatInplace(const std::vector<std::unique_ptr<IArraySource>> & sources, ColumnArray & res)
{
    if (sources.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Concat function should get at least 1 ArraySource");

    ArrayConcat::select(*sources.front(), sources, res);
}

MutableColumnPtr concat(const std::vector<std::unique_ptr<IArraySource>> & sources)
{
    if (sources.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Concat function should get at least 1 ArraySource");

    auto res = ColumnArray::create(sources.front()->createValuesColumn());
    ArrayConcat::select(*sources.front(), sources, assert_cast<ColumnArray &>(*res));
    return res;
}

}

}

#endif
