#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{
struct SliceFromLeftConstantOffsetBoundedSelectArraySource
    : public ArraySinkSourceSelector<SliceFromLeftConstantOffsetBoundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset, ssize_t & length)
    {
        sliceFromLeftConstantOffsetBounded(source, sink, offset, length);
    }
};

void sliceFromLeftConstantOffsetBounded(IArraySource & src, IArraySink & sink, size_t offset, ssize_t length)
{
    SliceFromLeftConstantOffsetBoundedSelectArraySource::select(src, sink, offset, length);
}
}

#endif
