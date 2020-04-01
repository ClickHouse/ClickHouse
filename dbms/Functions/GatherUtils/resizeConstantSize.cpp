#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

struct ArrayResizeConstant : public ArrayAndValueSourceSelectorBySink<ArrayResizeConstant>
{
    template <typename ArraySource, typename ValueSource, typename Sink>
    static void selectArrayAndValueSourceBySink(
            ArraySource && array_source, ValueSource && value_source, Sink && sink, ssize_t size)
    {
        resizeConstantSize(array_source, value_source, sink, size);
    }
};


void resizeConstantSize(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, ssize_t size)
{
    ArrayResizeConstant::select(sink, array_source, value_source, size);
}
}

#endif
