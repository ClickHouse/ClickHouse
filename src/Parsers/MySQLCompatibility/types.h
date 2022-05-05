#pragma once

#include <Parsers/IAST.h>
#include <Parsers/MySQLCompatibility/ParserOverlay/AST.h>

// MOO: for debug
#include <Common/logger_useful.h>
#include <Poco/Util/Application.h>
#include <IO/Operators.h>

namespace MySQLCompatibility
{

class IConversionTree;

using ConvPtr = std::shared_ptr<IConversionTree>;
using MySQLTree = MySQLParserOverlay::AST;
using MySQLPtr = MySQLParserOverlay::ASTPtr;
using CHPtr = DB::ASTPtr;
}
