#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

namespace
{

struct SliceFromRightConstantOffsetBoundedSelectArraySource
    : public ArraySourceSelector<SliceFromRightConstantOffsetBoundedSelectArraySource>
{
    template <typename Source>
    static void selectImpl(Source && source, size_t & offset, ssize_t & length, ColumnArray::MutablePtr & result)
    {
        using SourceType = typename std::decay<Source>::type;
        using Sink = typename SourceType::SinkType;
        result = ColumnArray::create(source.createValuesColumn());
        Sink sink(result->getData(), result->getOffsets(), source.getColumnSize());
        sliceFromRightConstantOffsetBounded(source, sink, offset, length);
    }
};

}

ColumnArray::MutablePtr sliceFromRightConstantOffsetBounded(IArraySource & src, size_t offset, ssize_t length)
{
    ColumnArray::MutablePtr res;
    SliceFromRightConstantOffsetBoundedSelectArraySource::select(src, offset, length, res);
    return res;
}
}

#endif
