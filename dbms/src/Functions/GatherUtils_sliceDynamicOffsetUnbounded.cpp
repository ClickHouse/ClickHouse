#include "GatherUtils.h"
#include "GatherUtils_selectors.h"

namespace DB
{
struct SliceDynamicOffsetUnboundedSelectArraySource : public ArraySinkSourceSelector<SliceDynamicOffsetUnboundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, IColumn & offset_column)
    {
        sliceDynamicOffsetUnbounded(source, sink, offset_column);
    }
};


void sliceDynamicOffsetUnbounded(IArraySource & src, IArraySink & sink, IColumn & offset_column)
{
    SliceDynamicOffsetUnboundedSelectArraySource::select(src, sink, offset_column);
}
}
