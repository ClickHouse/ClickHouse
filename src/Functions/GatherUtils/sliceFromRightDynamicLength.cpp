#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

namespace
{

struct Selector : public ArraySourceSelector<Selector>
{
    template <typename Source>
    static void selectSource(bool is_const, bool is_nullable, Source && source,
                             const IColumn & length_column, ColumnArray::MutablePtr & result)
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

            if (is_const)
                sliceFromRightDynamicLength(static_cast<ConstSource<NullableSource> &>(source), sink, length_column);
            else
                sliceFromRightDynamicLength(static_cast<NullableSource &>(source), sink, length_column);
        }
        else
        {
            result = ColumnArray::create(source.createValuesColumn());
            Sink sink(result->getData(), result->getOffsets(), source.getColumnSize());

            if (is_const)
                sliceFromRightDynamicLength(static_cast<ConstSource<SourceType> &>(source), sink, length_column);
            else
                sliceFromRightDynamicLength(source, sink, length_column);
        }
    }
};

}

ColumnArray::MutablePtr sliceFromRightDynamicLength(IArraySource & src, const IColumn & length_column)
{
    ColumnArray::MutablePtr res;
    Selector::select(src, length_column, res);
    return res;
}
}

#endif
