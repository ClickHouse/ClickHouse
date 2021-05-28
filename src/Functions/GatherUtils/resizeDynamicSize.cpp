#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

struct ArrayResizeDynamic : public ArrayAndValueSourceSelectorBySink<ArrayResizeDynamic>
{
    template <typename ArraySource, typename ValueSource, typename Sink>
    static void selectArrayAndValueSourceBySink(
            ArraySource && array_source, ValueSource && value_source, Sink && sink, const IColumn & size_column)
    {
        resizeDynamicSize(array_source, value_source, sink, size_column);
    }
};


void resizeDynamicSize(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, const IColumn & size_column)
{
    ArrayResizeDynamic::select(sink, array_source, value_source, size_column);
}
}

#endif
