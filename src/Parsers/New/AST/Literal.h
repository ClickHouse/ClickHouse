#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class Literal : public INode
{

};

class NumberLiteral : public Literal
{

};

class StringLiteral : public Literal
{

};

}
