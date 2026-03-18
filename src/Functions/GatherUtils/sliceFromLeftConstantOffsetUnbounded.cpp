#ifndef __clang_analyzer__ // It's too hard to analyze.

#include "GatherUtils.h"
#include "Selectors.h"
#include "Algorithms.h"

namespace DB::GatherUtils
{

namespace
{

struct SliceFromLeftConstantOffsetUnboundedSelectArraySource
    : public ArraySourceSelector<SliceFromLeftConstantOffsetUnboundedSelectArraySource>
{
    template <typename Source>
    static void selectSource(bool is_const, bool is_nullable, Source && source, size_t & offset, ColumnArray::MutablePtr & result)
    {
        using SourceType = typename std::decay<Source>::type;
        using Sink = typename SourceType::SinkType;

        if (is_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;
            using NullableSink = typename NullableSource::SinkType;

            auto & nullable_source = static_cast<NullableSource &>(source);

            result = ColumnArray::create(nullable_source.createValuesColumn());
            NullableSink sink(result->getData(), result->getOffsets(), source.getColumnSize());

            if (is_const)
                sliceFromLeftConstantOffsetUnbounded(static_cast<ConstSource<NullableSource> &>(source), sink, offset);
            else
                sliceFromLeftConstantOffsetUnbounded(static_cast<NullableSource &>(source), sink, offset);
        }
        else
        {
            result = ColumnArray::create(source.createValuesColumn());
            Sink sink(result->getData(), result->getOffsets(), source.getColumnSize());

            if (is_const)
                sliceFromLeftConstantOffsetUnbounded(static_cast<ConstSource<SourceType> &>(source), sink, offset);
            else
                sliceFromLeftConstantOffsetUnbounded(source, sink, offset);
        }
    }
};

}

ColumnArray::MutablePtr sliceFromLeftConstantOffsetUnbounded(IArraySource & src, size_t offset)
{
    ColumnArray::MutablePtr res;
    SliceFromLeftConstantOffsetUnboundedSelectArraySource::select(src, offset, res);
    return res;
}
}

#endif
