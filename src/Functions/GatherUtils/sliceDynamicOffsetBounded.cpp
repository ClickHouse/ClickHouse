#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

namespace
{

struct SliceDynamicOffsetBoundedSelectArraySource : public ArraySourceSelector<SliceDynamicOffsetBoundedSelectArraySource>
{
    template <typename Source>
    static void selectSource(bool is_const, bool is_nullable, Source && source,
                           const IColumn & offset_column, const IColumn & length_column, ColumnArray::MutablePtr & result)
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
                sliceDynamicOffsetBounded(static_cast<ConstSource<NullableSource> &>(source), sink, offset_column, length_column);
            else
                sliceDynamicOffsetBounded(static_cast<NullableSource &>(source), sink, offset_column, length_column);
        }
        else
        {
            result = ColumnArray::create(source.createValuesColumn());
            Sink sink(result->getData(), result->getOffsets(), source.getColumnSize());

            if (is_const)
                sliceDynamicOffsetBounded(static_cast<ConstSource<SourceType> &>(source), sink, offset_column, length_column);
            else
                sliceDynamicOffsetBounded(source, sink, offset_column, length_column);
        }
    }
};

}

ColumnArray::MutablePtr sliceDynamicOffsetBounded(IArraySource & src, const IColumn & offset_column, const IColumn & length_column)
{
    ColumnArray::MutablePtr res;
    SliceDynamicOffsetBoundedSelectArraySource::select(src, offset_column, length_column, res);
    return res;
}
}

#endif
