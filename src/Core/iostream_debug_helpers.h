#pragma once
#include <iostream>

namespace DB
{

// Use template to disable implicit casting for certain overloaded types such as Field, which leads
// to overload resolution ambiguity.
class Field;
template <typename T>
requires std::is_same_v<T, Field>
std::ostream & operator<<(std::ostream & stream, const T & what);

struct NameAndTypePair;
std::ostream & operator<<(std::ostream & stream, const NameAndTypePair & what);

class IDataType;
std::ostream & operator<<(std::ostream & stream, const IDataType & what);

class IStorage;
std::ostream & operator<<(std::ostream & stream, const IStorage & what);

class IFunctionOverloadResolver;
std::ostream & operator<<(std::ostream & stream, const IFunctionOverloadResolver & what);

class IFunctionBase;
std::ostream & operator<<(std::ostream & stream, const IFunctionBase & what);

class Block;
std::ostream & operator<<(std::ostream & stream, const Block & what);

struct ColumnWithTypeAndName;
std::ostream & operator<<(std::ostream & stream, const ColumnWithTypeAndName & what);

class IColumn;
std::ostream & operator<<(std::ostream & stream, const IColumn & what);

struct Packet;
std::ostream & operator<<(std::ostream & stream, const Packet & what);

class ExpressionActions;
std::ostream & operator<<(std::ostream & stream, const ExpressionActions & what);

struct TreeRewriterResult;
std::ostream & operator<<(std::ostream & stream, const TreeRewriterResult & what);
}

/// some operator<< should be declared before operator<<(... std::shared_ptr<>)
#include <base/iostream_debug_helpers.h>
