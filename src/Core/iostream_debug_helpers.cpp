#include "iostream_debug_helpers.h"

#include <iostream>
#include <Client/Connection.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Common/COW.h>
#include <Common/FieldVisitors.h>

namespace DB
{

template <>
std::ostream & operator<< <Field>(std::ostream & stream, const Field & what)
{
    stream << applyVisitor(FieldVisitorDump(), what);
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IBlockInputStream & what)
{
    stream << "IBlockInputStream(name = " << what.getName() << ")";
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
    auto table_id = what.getStorageID();
    stream << "IStorage(name = " << what.getName() << ", tableName = " << table_id.table_name << ") {"
           << what.getInMemoryMetadataPtr()->getColumns().getAllPhysical().toString() << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const TableLockHolder &)
{
    stream << "TableStructureReadLock()";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IFunctionOverloadResolver & what)
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
    stream << "ColumnWithTypeAndName(name = " << what.name << ", type = " << *what.type << ", column = ";
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

std::ostream & operator<<(std::ostream & stream, const Packet & what)
{
    stream << "Packet("
           << "type = " << what.type;
    // types description: Core/Protocol.h
    if (what.exception)
        stream << "exception = " << what.exception.get();
    // TODO: profile_info
    stream << ") {" << what.block << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const ExpressionAction & what)
{
    stream << "ExpressionAction(" << what.toString() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const ExpressionActions & what)
{
    stream << "ExpressionActions(" << what.dumpActions() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const TreeRewriterResult & what)
{
    stream << "SyntaxAnalyzerResult{";
    stream << "storage=" << what.storage << "; ";
    if (!what.source_columns.empty())
    {
        stream << "source_columns=";
        dumpValue(stream, what.source_columns);
        stream << "; ";
    }
    if (!what.aliases.empty())
    {
        stream << "aliases=";
        dumpValue(stream, what.aliases);
        stream << "; ";
    }
    if (!what.array_join_result_to_source.empty())
    {
        stream << "array_join_result_to_source=";
        dumpValue(stream, what.array_join_result_to_source);
        stream << "; ";
    }
    if (!what.array_join_alias_to_name.empty())
    {
        stream << "array_join_alias_to_name=";
        dumpValue(stream, what.array_join_alias_to_name);
        stream << "; ";
    }
    if (!what.array_join_name_to_alias.empty())
    {
        stream << "array_join_name_to_alias=";
        dumpValue(stream, what.array_join_name_to_alias);
        stream << "; ";
    }
    stream << "rewrite_subqueries=" << what.rewrite_subqueries << "; ";
    stream << "}";

    return stream;
}

}
