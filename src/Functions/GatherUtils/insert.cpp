#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Selectors.h>

namespace DB::GatherUtils
{

namespace
{

struct ArrayInsertConstant : public ArrayAndValueSourceSelectorBySink<ArrayInsertConstant>
{
    template <typename ArraySource, typename ValueSource, typename Sink, typename Position>
    static void selectArrayAndValueSourceBySink(ArraySource && array_source, ValueSource && value_source, Sink && sink, Position position)
    {
        insertConstantPositionImpl(array_source, value_source, sink, position);
    }
};

struct ArrayInsertDynamic : public ArrayAndValueSourceSelectorBySink<ArrayInsertDynamic>
{
    template <typename ArraySource, typename ValueSource, typename Sink>
    static void selectArrayAndValueSourceBySink(
        ArraySource && array_source, ValueSource && value_source, Sink && sink, const IColumn & position_column, bool position_is_unsigned)
    {
        insertDynamicPositionImpl(array_source, value_source, sink, position_column, position_is_unsigned);
    }
};

}

void insertConstantPosition(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, Int64 position)
{
    ArrayInsertConstant::select(sink, array_source, value_source, position);
}

void insertConstantPosition(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, UInt64 position)
{
    ArrayInsertConstant::select(sink, array_source, value_source, position);
}

void insertDynamicPosition(
    IArraySource & array_source, IValueSource & value_source, IArraySink & sink, const IColumn & position_column, bool position_is_unsigned)
{
    ArrayInsertDynamic::select(sink, array_source, value_source, position_column, position_is_unsigned);
}

}
