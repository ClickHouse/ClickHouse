#include <Core/ColumnsWithTypeAndName.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ColumnWithTypeAndName ColumnWithTypeAndName::cloneEmpty() const
{
    ColumnWithTypeAndName res;

    res.name = name;
    res.type = type;
    if (column)
        res.column = column->cloneEmpty();

    return res;
}


bool ColumnWithTypeAndName::operator== (const ColumnWithTypeAndName & other) const
{
    return name == other.name
        && ((!type && !other.type) || (type && other.type && type->equals(*other.type)))
        && ((!column && !other.column) || (column && other.column && column->getName() == other.column->getName()));
}


String ColumnWithTypeAndName::prettyPrint() const
{
    WriteBufferFromOwnString out;
    writeString(name, out);
    if (type)
    {
        writeChar(' ', out);
        writeString(type->getName(), out);
    }
    if (column)
    {
        writeChar(' ', out);
        writeString(column->getName(), out);
    }
    return out.str();
}

}
