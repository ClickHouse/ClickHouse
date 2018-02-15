#include <Functions/GatherUtils/Selectors.h>
#include <Functions/GatherUtils/Algorithms.h>

namespace DB::GatherUtils
{
struct SliceFromLeftConstantOffsetUnboundedSelectArraySource
    : public ArraySinkSourceSelector<SliceFromLeftConstantOffsetUnboundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset)
    {
        sliceFromLeftConstantOffsetUnbounded(source, sink, offset);
    }
};

void sliceFromLeftConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset)
{
    SliceFromLeftConstantOffsetUnboundedSelectArraySource::select(src, sink, offset);
}
}
