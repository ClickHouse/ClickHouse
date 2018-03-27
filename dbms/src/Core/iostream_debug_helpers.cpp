#include "iostream_debug_helpers.h"

#include <iostream>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Common/COWPtr.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Storages/IStorage.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/IAST.h>

namespace DB
{

std::ostream & operator<<(std::ostream & stream, const IBlockInputStream & what)
{
    stream << "IBlockInputStream(name = " << what.getName() << ")";
    //what.dumpTree(stream); // todo: set const
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Field & what)
{
    stream << "Field(type = " << what.getTypeName() << ")";
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
           << what.getColumns().getAllPhysical().toString()
           << "}";
    // isRemote supportsSampling supportsFinal supportsPrewhere
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const TableStructureReadLock &)
{
    stream << "TableStructureReadLock()";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IFunctionBuilder & what)
{
    stream << "IFunction(name = " << what.getName() << ", variadic = " << what.isVariadic() << ", args = "
           << what.getNumberOfArguments() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Block & what)
{
    stream << "Block("
           << "num_columns = " << what.columns()
           << "){" << what.dumpStructure() << "}";
    return stream;
}


template <typename T>
std::ostream & printCOWPtr(std::ostream & stream, const typename COWPtr<T>::Ptr & what)
{
    stream << "COWPtr::Ptr(" << what.get();
    if (what)
        stream << ", use_count = " << what->use_count();
    stream << ") {";
    if (what)
        stream << *what;
    else
        stream << "nullptr";
    stream << "}";
    return stream;
}


std::ostream & operator<<(std::ostream & stream, const ColumnWithTypeAndName & what)
{
    stream << "ColumnWithTypeAndName(name = " << what.name << ", type = " << what.type << ", column = ";
    return printCOWPtr<IColumn>(stream, what.column) << ")";
}

std::ostream & operator<<(std::ostream & stream, const IColumn & what)
{
    stream << "IColumn(" << what.dumpStructure() << ")";
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

std::ostream & operator<<(std::ostream & stream, const SubqueryForSet & what)
{
    stream << "SubqueryForSet(source = " << what.source
    // TODO: << ", set = " <<  what.set << ", join = " <<  what.join
    << ", table = " << what.table
    << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IAST & what)
{
    stream << "IAST{";
    what.dumpTree(stream);
    stream << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const ExpressionAnalyzer & what)
{
    stream << "ExpressionAnalyzer{"
           << "hasAggregation=" << what.hasAggregation()
           << ", SubqueriesForSet=" << what.getSubqueriesForSets()
           << ", ExternalTables=" << what.getExternalTables()
           << "}";
    return stream;
}

}
