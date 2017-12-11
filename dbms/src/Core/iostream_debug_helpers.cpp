#include "iostream_debug_helpers.h"

#include <iostream>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Storages/IStorage.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/IAST.h>

std::ostream & operator<<(std::ostream & stream, const DB::IBlockInputStream & what)
{
    stream << "IBlockInputStream(id = " << what.getID() << ", name = " << what.getName() << ")";
    //what.dumpTree(stream); // todo: set const
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::Field & what)
{
    stream << "Field(type = " << what.getTypeName() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::NameAndTypePair & what)
{
    stream << "NameAndTypePair(name = " << what.name << ", type = " << what.type << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::IDataType & what)
{
    stream << "IDataType(name = " << what.getName() << ", default = " << what.getDefault() << ", isNullable = " << what.isNullable()
           << ", isNumeric = " << what.isNumeric() << ", behavesAsNumber = " << what.behavesAsNumber() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::IStorage & what)
{
    stream << "IStorage(name = " << what.getName() << ", tableName = " << what.getTableName() << ") {"
           << what.getColumnsList().toString()
           << "}";
    // isRemote supportsSampling supportsFinal supportsPrewhere supportsParallelReplicas
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::TableStructureReadLock &)
{
    stream << "TableStructureReadLock()";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::IFunction & what)
{
    stream << "IFunction(name = " << what.getName() << ", variadic = " << what.isVariadic() << ", args = " << what.getNumberOfArguments()
           << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::Block & what)
{
    stream << "Block("
           << "size = " << what.getColumns().size()
           << "){" << what.dumpStructure() << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::ColumnWithTypeAndName & what)
{
    stream << "ColumnWithTypeAndName(name = " << what.name << ", type = " << what.type << ", column = " << what.column << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::IColumn & what)
{
    stream << "IColumn(name = " << what.getName()
           // TODO: maybe many flags here
           << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::Connection::Packet & what)
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

std::ostream & operator<<(std::ostream & stream, const DB::SubqueryForSet & what)
{
    stream << "SubqueryForSet(source = " << what.source
    << ", source_sample = " <<  what.source_sample
    // TODO: << ", set = " <<  what.set << ", join = " <<  what.join
    << ", table = " << what.table
    << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::IAST & what)
{
    stream << "IAST("
           << "query_string = " << what.query_string
           <<"){";
    what.dumpTree(stream);
    stream << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const DB::ExpressionAnalyzer & what)
{
    stream << "ExpressionAnalyzer{"
           << "hasAggregation="<<what.hasAggregation()
           << ", RequiredColumns=" << what.getRequiredColumns()
           << ", SubqueriesForSet=" << what.getSubqueriesForSets()
           << ", ExternalTables=" << what.getExternalTables()
           // TODO
           << "}";
    return stream;
}

