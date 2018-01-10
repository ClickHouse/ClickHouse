#include <Functions/GatherUtils/Selectors.h>
#include <Functions/GatherUtils/Algorithms.h>

namespace DB::GatherUtils
{
struct SliceDynamicOffsetBoundedSelectArraySource : public ArraySinkSourceSelector<SliceDynamicOffsetBoundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, const IColumn & offset_column, const IColumn & length_column)
    {
        sliceDynamicOffsetBounded(source, sink, offset_column, length_column);
    }
};

void sliceDynamicOffsetBounded(IArraySource & src, IArraySink & sink, const IColumn & offset_column, const IColumn & length_column)
{
    SliceDynamicOffsetBoundedSelectArraySource::select(src, sink, offset_column, length_column);
}
}
