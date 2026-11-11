#include <Core/SettingFieldASTFunction.h>

#include <Core/Defines.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

boost::intrusive_ptr<ASTFunction> parseASTFunction(const String & str)
{
    if (str.empty())
        return nullptr;
    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, str, /*max_query_size=*/0, /*max_parser_depth=*/DBMS_DEFAULT_MAX_PARSER_DEPTH, /*max_parser_backtracks=*/DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    auto * func = typeid_cast<ASTFunction *>(ast.get());
    if (!func)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a function, got: {}", str);
    return boost::intrusive_ptr<ASTFunction>(func);
}

}

SettingFieldASTFunction::SettingFieldASTFunction(const boost::intrusive_ptr<ASTFunction> & func)
    : value{func}
{
}

SettingFieldASTFunction::SettingFieldASTFunction(const String & str)
    : value{parseASTFunction(str)}
{
}

SettingFieldASTFunction::SettingFieldASTFunction(const Field & f)
    : SettingFieldASTFunction{f.safeGet<String>()}
{
}

SettingFieldASTFunction & SettingFieldASTFunction::operator =(const String & str)
{
    value = parseASTFunction(str);
    changed = true;
    return *this;
}

SettingFieldASTFunction & SettingFieldASTFunction::operator =(const Field & f)
{
    *this = f.safeGet<String>();
    return *this;
}

String SettingFieldASTFunction::toString() const
{
    return value ? value->formatWithSecretsOneLine() : String{};
}

void SettingFieldASTFunction::parseFromString(const String & str)
{
    *this = str;
}

void SettingFieldASTFunction::writeBinary(WriteBuffer & out) const
{
    writeStringBinary(toString(), out);
}

void SettingFieldASTFunction::readBinary(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    *this = str;
}

}
