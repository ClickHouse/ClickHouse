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
    static void selectSource(bool /*is_const*/, bool is_nullable, Source & source, const Sources & sources, ColumnArray::MutablePtr & result)
    {
        using SourceType = typename std::decay<Source>::type;
        using Sink = typename SourceType::SinkType;

        if (is_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;
            using NullableSink = typename NullableSource::SinkType;

            auto & nullable_source = static_cast<NullableSource &>(source);


            result = ColumnArray::create(nullable_source.createValuesColumn());
            NullableSink sink(result->getData(), result->getOffsets(), source.getColumnSize());

            concat<NullableSource, NullableSink>(sources, std::move(sink));
        }
        else
        {
            result = ColumnArray::create(source.createValuesColumn());
            Sink sink(result->getData(), result->getOffsets(), source.getColumnSize());

            concat<SourceType, Sink>(sources, std::move(sink));
        }
    }
};

}

ColumnArray::MutablePtr concat(const std::vector<std::unique_ptr<IArraySource>> & sources)
{
    if (sources.empty())
        throw Exception("Concat function should get at least 1 ArraySource", ErrorCodes::LOGICAL_ERROR);

    ColumnArray::MutablePtr res;
    ArrayConcat::select(*sources.front(), sources, res);
    return res;
}

}

}

#endif
