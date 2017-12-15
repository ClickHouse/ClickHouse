#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>


namespace DB
{

ColumnConst::ColumnConst(ColumnPtr data_, size_t s)
    : data(data_), s(s)
{
    /// Squash Const of Const.
    while (const ColumnConst * const_data = typeid_cast<const ColumnConst *>(data.get()))
        data = const_data->getDataColumnPtr();

    if (data->size() != 1)
        throw Exception("Incorrect size of nested column in constructor of ColumnConst: " + toString(data->size()) + ", must be 1.",
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
}

MutableColumnPtr ColumnConst::convertToFullColumn() const
{
    return data->replicate(Offsets_t(1, s));
}

}
