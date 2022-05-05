#pragma once

#include <Parsers/IAST.h>
#include <Parsers/MySQLCompatibility/ParserOverlay/AST.h>

// MOO: for debug
#include <IO/Operators.h>
#include <Poco/Util/Application.h>
#include <Common/logger_useful.h>

namespace MySQLCompatibility
{

class IConversionTree;

using ConvPtr = std::shared_ptr<IConversionTree>;
using MySQLTree = MySQLParserOverlay::AST;
using MySQLPtr = MySQLParserOverlay::ASTPtr;
using CHPtr = DB::ASTPtr;
}
