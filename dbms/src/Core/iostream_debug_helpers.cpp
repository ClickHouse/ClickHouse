#include "iostream_debug_helpers.h"

#include <iostream>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Common/COW.h>
#include <Common/FieldVisitors.h>

namespace DB
{
std::ostream & operator<<(std::ostream & stream, const IBlockInputStream & what)
{
    stream << "IBlockInputStream(name = " << what.getName() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Field & what)
{
    stream << applyVisitor(FieldVisitorDump(), what);
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const NameAndTypePair & what)
{
    stream << "NameAndTypePair(name = " << what.name << ", type = " << what.type << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IDataType & what)
{
    stream << "IDataType(name = " << what.getName() << ", default = " << what.getDefault() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IStorage & what)
{
    stream << "IStorage(name = " << what.getName() << ", tableName = " << what.getTableName() << ") {"
           << what.getColumns().getAllPhysical().toString() << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const TableStructureReadLock &)
{
    stream << "TableStructureReadLock()";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IFunctionBuilder & what)
{
    stream << "IFunction(name = " << what.getName() << ", variadic = " << what.isVariadic() << ", args = " << what.getNumberOfArguments()
           << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Block & what)
{
    stream << "Block("
           << "num_columns = " << what.columns() << "){" << what.dumpStructure() << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const ColumnWithTypeAndName & what)
{
    stream << "ColumnWithTypeAndName(name = " << what.name << ", type = " << what.type << ", column = ";
    return dumpValue(stream, what.column) << ")";
}

std::ostream & operator<<(std::ostream & stream, const IColumn & what)
{
    stream << "IColumn(" << what.dumpStructure() << ")";
    stream << "{";
    for (size_t i = 0; i < what.size(); ++i)
    {
        if (i)
            stream << ", ";
        stream << applyVisitor(FieldVisitorDump(), what[i]);
    }
    stream << "}";

    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Connection::Packet & what)
{
    stream << "Connection::Packet("
           << "type = " << what.type;
    // types description: Core/Protocol.h
    if (what.exception)
        stream << "exception = " << what.exception.get();
    // TODO: profile_info
    stream << ") {" << what.block << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IAST & what)
{
    stream << "IAST{";
    what.dumpTree(stream);
    stream << "}";
    return stream;
}

}
