#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

ASTPtr ASTCreateSQLFunctionQuery::clone() const
{
    auto res = make_intrusive<ASTCreateSQLFunctionQuery>(*this);
    res->children.clear();

    res->function_name = function_name->clone();
    res->children.push_back(res->function_name);

    res->function_core = function_core->clone();
    res->children.push_back(res->function_core);
    return res;
}

void ASTCreateSQLFunctionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    ostr << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "FUNCTION ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << backQuoteIfNeed(getFunctionName());

    formatOnCluster(ostr, settings);

    ostr << " AS ";
    function_core->format(ostr, settings, state, frame);
}

String ASTCreateSQLFunctionQuery::getFunctionName() const
{
    String name;
    bool is_ok = tryGetIdentifierNameInto(function_name, name);
    if (!is_ok)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected function name, got '{}'", function_name->formatForErrorMessage());
    return name;
}

void ASTCreateSQLFunctionQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "CreateSQLFunctionQuery");

    w.writeBool("or_replace", or_replace);
    w.writeBool("if_not_exists", if_not_exists);

    if (!cluster.empty())
        w.writeString("cluster", cluster);

    w.writeChild("function_name", function_name);
    w.writeChild("function_core", function_core);
}

void ASTCreateSQLFunctionQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    or_replace = r.getBool("or_replace");
    if_not_exists = r.getBool("if_not_exists");
    cluster = r.getString("cluster");

    children.clear();

    function_name = r.readChildOfType<ASTIdentifier>("function_name");
    if (!function_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'function_name' for `CreateSQLFunctionQuery` during AST JSON deserialization");
    children.push_back(function_name);

    function_core = r.readChild("function_core");
    if (!function_core)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'function_core' for `CreateSQLFunctionQuery` during AST JSON deserialization");
    children.push_back(function_core);
}


}
