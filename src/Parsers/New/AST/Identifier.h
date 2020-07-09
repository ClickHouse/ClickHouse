#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class Identifier : public INode
{

};

class ColumnIdentifier : public Identifier
{

};

class TableIdentifier : public Identifier
{

};

class DatabaseIdentifier : public Identifier
{

};

}
