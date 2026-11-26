#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <utility>
#include <vector>


namespace DB
{

/** AST node for:
  *
  *     CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] func_name [ON CLUSTER cluster]
  *         ARGUMENTS (a UInt8, b String)
  *         RETURNS UInt64
  *         ENGINE = DriverName(key1 = 'value1', key2 = 42)
  *         AS '...code body...'
  *
  * Also serves the `ATTACH FUNCTION ...` form used when the same query is persisted on
  * disk in the user_defined SQL objects directory.
  */
class ASTCreateFunctionWithDriverQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool or_replace = false;
    bool if_not_exists = false;
    bool is_attach = false;

    /// The function name as an identifier AST.
    ASTPtr function_name_ast;

    /// Optional: ExpressionList of `ASTNameTypePair` (named-and-typed arguments).
    /// May be empty (driver decides the signature).
    ASTPtr arguments_ast;

    /// Optional: return type, parsed via `ParserDataType`.
    ASTPtr return_type_ast;

    /// Driver name from `ENGINE = DriverName(...)`.
    String engine_name;

    /// Engine arguments are name/value pairs, with the value being a literal AST.
    /// We keep them in a vector to preserve user-specified order when re-formatting.
    std::vector<std::pair<String, ASTPtr>> engine_arguments;

    /// The body the user provided after AS - free text passed to driver's stdin.
    String source_code;

    String getID(char delim) const override;

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTCreateFunctionWithDriverQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

    String getFunctionName() const;
};

}
