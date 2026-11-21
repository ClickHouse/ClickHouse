#ifndef __clang_analyzer__ // It's too hard to analyze.

#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Selectors.h>
#include <Functions/GatherUtils/Algorithms.h>

namespace DB::GatherUtils
{

namespace
{

struct ArrayResizeDynamic : public ArrayAndValueSourceSelectorBySink<ArrayResizeDynamic>
{
    template <typename ArraySource, typename ValueSource, typename Sink>
    static void selectArrayAndValueSourceBySink(
            ArraySource && array_source, ValueSource && value_source, Sink && sink, const IColumn & size_column,
            CancellationBudget & budget)
    {
        resizeDynamicSize(array_source, value_source, sink, size_column, budget);
    }
};

}

void resizeDynamicSize(
    IArraySource & array_source, IValueSource & value_source, IArraySink & sink, const IColumn & size_column, CancellationBudget & budget)
{
    ArrayResizeDynamic::select(sink, array_source, value_source, size_column, budget);
}
}

#endif
