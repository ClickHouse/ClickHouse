#pragma once

#include <Core/Types.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/MySQL/CreateQueryVisitor.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{

String toCreateQuery(const MySQLTableStruct & table_struct, const Context & context);

}
