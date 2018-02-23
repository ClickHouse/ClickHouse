#include <Functions/GatherUtils/Selectors.h>
#include <Functions/GatherUtils/Algorithms.h>

namespace DB::GatherUtils
{
struct SliceFromRightConstantOffsetBoundedSelectArraySource
    : public ArraySinkSourceSelector<SliceFromRightConstantOffsetBoundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset, ssize_t & length)
    {
        sliceFromRightConstantOffsetBounded(source, sink, offset, length);
    }
};

void sliceFromRightConstantOffsetBounded(IArraySource & src, IArraySink & sink, size_t offset, ssize_t length)
{
    SliceFromRightConstantOffsetBoundedSelectArraySource::select(src, sink, offset, length);
}
}
