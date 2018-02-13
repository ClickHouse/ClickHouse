#pragma once
#include <iostream>

#include <Client/Connection.h>
#include <Common/PODArray.h>

namespace DB
{

class IBlockInputStream;
std::ostream & operator<<(std::ostream & stream, const IBlockInputStream & what);

class Field;
std::ostream & operator<<(std::ostream & stream, const Field & what);

struct NameAndTypePair;
std::ostream & operator<<(std::ostream & stream, const NameAndTypePair & what);

class IDataType;
std::ostream & operator<<(std::ostream & stream, const IDataType & what);

class IStorage;
std::ostream & operator<<(std::ostream & stream, const IStorage & what);

class TableStructureReadLock;
std::ostream & operator<<(std::ostream & stream, const TableStructureReadLock & what);

class IFunctionBase;
std::ostream & operator<<(std::ostream & stream, const IFunctionBase & what);

class Block;
std::ostream & operator<<(std::ostream & stream, const Block & what);

struct ColumnWithTypeAndName;
std::ostream & operator<<(std::ostream & stream, const ColumnWithTypeAndName & what);

class IColumn;
std::ostream & operator<<(std::ostream & stream, const IColumn & what);


struct SubqueryForSet;
std::ostream & operator<<(std::ostream & stream, const SubqueryForSet & what);

class IAST;
std::ostream & operator<<(std::ostream & stream, const IAST & what);

class ExpressionAnalyzer;
std::ostream & operator<<(std::ostream & stream, const ExpressionAnalyzer & what);

std::ostream & operator<<(std::ostream & stream, const Connection::Packet & what);

template <typename T, size_t INITIAL_SIZE, typename TAllocator, size_t pad_right_>
std::ostream & operator<<(std::ostream & stream, const PODArray<T, INITIAL_SIZE, TAllocator, pad_right_> & what)
{
    stream << "PODArray(size = " << what.size() << ", capacity = " << what.capacity() << ")";
    dumpContainer(stream, what);
    return stream;
};

}

/// some operator<< should be declared before operator<<(... std::shared_ptr<>)
#include <common/iostream_debug_helpers.h>
