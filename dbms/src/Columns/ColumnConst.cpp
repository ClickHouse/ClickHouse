#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Columns/ColumnConst.h>
#include <Core/FieldVisitors.h>


namespace DB
{

String ColumnConst::dump() const
{
    String res;
    WriteBufferFromString out(res);

    {
        out << "ColumnConst, size: " << s << ", nested column: " << data->getName() << ", nested size: " << data->size();
        if (data->size())
            out << ", value: " << applyVisitor(FieldVisitorDump(), (*data)[0]);
    }

    return res;
}

}
