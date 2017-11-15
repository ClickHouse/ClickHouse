#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Core/FieldVisitors.h>
#include <Common/typeid_cast.h>


namespace DB
{

ColumnConst::ColumnConst(ColumnPtr data, size_t s)
    : data(data), s(s)
{
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



String ColumnConst::dump() const
{
    WriteBufferFromOwnString out;
    out << "ColumnConst, size: " << s << ", nested column: " << data->getName() << ", nested size: " << data->size();
    if (data->size())
        out << ", value: " << applyVisitor(FieldVisitorDump(), (*data)[0]);

    return out.str();
}

}
