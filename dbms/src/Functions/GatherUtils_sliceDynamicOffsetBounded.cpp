#include "GatherUtils.h"
#include "GatherUtils_selectors.h"

namespace DB
{
struct SliceDynamicOffsetBoundedSelectArraySource : public ArraySinkSourceSelector<SliceDynamicOffsetBoundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, IColumn & offset_column, IColumn & length_column)
    {
        sliceDynamicOffsetBounded(source, sink, offset_column, length_column);
    }
};

void sliceDynamicOffsetBounded(IArraySource & src, IArraySink & sink, IColumn & offset_column, IColumn & length_column)
{
    SliceDynamicOffsetBoundedSelectArraySource::select(src, sink, offset_column, length_column);
}
}
