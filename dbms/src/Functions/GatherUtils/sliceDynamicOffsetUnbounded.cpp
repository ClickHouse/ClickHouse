#include <Functions/GatherUtils/Selectors.h>
#include <Functions/GatherUtils/Algorithms.h>

namespace DB::GatherUtils
{
struct SliceDynamicOffsetUnboundedSelectArraySource : public ArraySinkSourceSelector<SliceDynamicOffsetUnboundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, const IColumn & offset_column)
    {
        sliceDynamicOffsetUnbounded(source, sink, offset_column);
    }
};


void sliceDynamicOffsetUnbounded(IArraySource & src, IArraySink & sink, const IColumn & offset_column)
{
    SliceDynamicOffsetUnboundedSelectArraySource::select(src, sink, offset_column);
}
}
