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
    while (ColumnConst * const_data = typeid_cast<ColumnConst *>(data.get()))
        data = const_data->getDataColumnPtr();

    if (data->size() != 1)
        throw Exception("Incorrect size of nested column in constructor of ColumnConst: " + toString(data->size()) + ", must be 1.",
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
}

bool ColumnConst::isNull() const
{
    const ColumnNullable * column_nullable = typeid_cast<const ColumnNullable *>(data.get());
    return column_nullable && column_nullable->isNullAt(0);
}

ColumnPtr ColumnConst::convertToFullColumn() const
{
    return data->replicate(Offsets_t(1, s));
}

}
