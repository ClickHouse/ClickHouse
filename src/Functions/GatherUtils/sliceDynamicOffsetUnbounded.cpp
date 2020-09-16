#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{
struct SliceDynamicOffsetUnboundedSelectArraySource : public ArraySourceSelector<SliceDynamicOffsetUnboundedSelectArraySource>
{
    template <typename Source>
    static void selectImpl(Source && source, const IColumn & offset_column, ColumnArray::MutablePtr & result)
    {
        using Sink = typename std::remove_cv<Source>::type::SinkType;
        result = ColumnArray::create(source.createValuesColumn());
        Sink sink(result->getData(), result->getOffsets(), source.getColumnSize());
        sliceDynamicOffsetUnbounded(source, sink, offset_column);
    }
};


ColumnArray::MutablePtr sliceDynamicOffsetUnbounded(IArraySource & src, const IColumn & offset_column)
{
    ColumnArray::MutablePtr res;
    SliceDynamicOffsetUnboundedSelectArraySource::select(src, offset_column, res);
    return res;
}
}

#endif
