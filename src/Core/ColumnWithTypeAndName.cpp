#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

ColumnWithTypeAndName::ColumnWithTypeAndName(const DataTypePtr & type_, const String & name_)
    : column(type_->createColumn()), type(type_), name(name_) {}

ColumnWithTypeAndName ColumnWithTypeAndName::cloneEmpty() const
{
    ColumnWithTypeAndName res;

    res.name = name;
    res.type = type;
    if (column)
        res.column = column->cloneEmpty();

    return res;
}


bool ColumnWithTypeAndName::operator==(const ColumnWithTypeAndName & other) const
{
    return name == other.name
        && ((!type && !other.type) || (type && other.type && type->equals(*other.type)))
        && ((!column && !other.column) || (column && other.column && column->getName() == other.column->getName()));
}


void ColumnWithTypeAndName::dumpNameAndType(WriteBuffer & out) const
{
    out << name;

    if (type)
        out << ' ' << type->getName();
    else
        out << " nullptr";
}

void ColumnWithTypeAndName::dumpStructure(WriteBuffer & out) const
{
    dumpNameAndType(out);

    if (column)
        out << ' ' << column->dumpStructure();
    else
        out << " nullptr";
}

String ColumnWithTypeAndName::dumpStructure() const
{
    WriteBufferFromOwnString out;
    dumpStructure(out);
    return out.str();
}

}
