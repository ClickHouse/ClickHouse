#pragma once
#include <iostream>

#include <Client/Connection.h>


namespace DB
{

// Used to disable implicit casting for certain overloaded types such as Field, which leads to
// overload resolution ambiguity.
template <typename T> struct Dumpable;
template <typename T>
std::ostream & operator<<(std::ostream & stream, const typename Dumpable<T>::Type & what);

class IBlockInputStream;
std::ostream & operator<<(std::ostream & stream, const IBlockInputStream & what);

class Field;
template <> struct Dumpable<Field> { using Type = Field; };

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

class IAST;
std::ostream & operator<<(std::ostream & stream, const IAST & what);

std::ostream & operator<<(std::ostream & stream, const Connection::Packet & what);

struct ExpressionAction;
std::ostream & operator<<(std::ostream & stream, const ExpressionAction & what);

class ExpressionActions;
std::ostream & operator<<(std::ostream & stream, const ExpressionActions & what);

struct SyntaxAnalyzerResult;
std::ostream & operator<<(std::ostream & stream, const SyntaxAnalyzerResult & what);
}

/// some operator<< should be declared before operator<<(... std::shared_ptr<>)
#include <common/iostream_debug_helpers.h>
