#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTIdentifier.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTStatisticsDeclaration::clone() const
{
    auto res = make_intrusive<ASTStatisticsDeclaration>();

    res->set(res->columns, columns->clone());
    if (types)
        res->set(res->types, types->clone());

    return res;
}

std::vector<String> ASTStatisticsDeclaration::getColumnNames() const
{
    std::vector<String> result;
    result.reserve(columns->children.size());
    for (const ASTPtr & column_ast : columns->children)
    {
        result.push_back(column_ast->as<ASTIdentifier &>().name());
    }
    return result;

}

std::vector<String> ASTStatisticsDeclaration::getTypeNames() const
{
    chassert(types != nullptr);
    std::vector<String> result;
    result.reserve(types->children.size());
    for (const ASTPtr & column_ast : types->children)
    {
        result.push_back(column_ast->as<ASTFunction &>().name);
    }
    return result;

}

void ASTStatisticsDeclaration::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "StatisticsDeclaration");
    w.writeChild("columns", columns);
    w.writeChild("types", types);
}

void ASTStatisticsDeclaration::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    auto child = r.readChild("columns");
    if (!child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'columns' field in `StatisticsDeclaration` during AST JSON deserialization");
    set(columns, child);

    child = r.readChild("types");
    if (child)
        set(types, child);
}

void ASTStatisticsDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    columns->format(ostr, s, state, frame);
    if (types)
    {
        ostr << " TYPE ";
        types->format(ostr, s, state, frame);
    }
}

}
