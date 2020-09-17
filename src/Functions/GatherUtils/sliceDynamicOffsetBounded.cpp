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
    static void selectImpl(Source && source, const IColumn & offset_column, const IColumn & length_column, ColumnArray::MutablePtr & result)
    {
        using SourceType = typename std::decay<Source>::type;
        using Sink = typename SourceType::SinkType;
        result = ColumnArray::create(source.createValuesColumn());
        Sink sink(result->getData(), result->getOffsets(), source.getColumnSize());
        sliceDynamicOffsetBounded(source, sink, offset_column, length_column);
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
