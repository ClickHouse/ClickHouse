#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>

#include <unordered_set>

namespace DB
{

class ASTQueryParameter;
class ASTSelectQuery;
class ASTSetQuery;
class Field;
class SettingsChanges;

/// Whether `ast` contains a table expression that calls `name` as a function, i.e. an invocation of a
/// parameterized common table expression: `WITH t AS (SELECT {v:String}) SELECT * FROM t(v = 'x')`. Such a
/// CTE body is a template - its placeholders are supplied by the invocation, not by the query parameters -
/// so `ReplaceQueryParameterVisitor` leaves them in place and `QueryTreeBuilder` keeps the body as an AST
/// for `QueryAnalyzer` to expand. A CTE that is never invoked is not a template and keeps failing on an
/// unset parameter as it always did, so both places have to agree on this predicate.
bool isParameterizedCTEInvoked(const IAST & ast, const String & name);

/// Visit substitutions in a query, replace ASTQueryParameter with ASTLiteral.
/// Rebuild ASTIdentifiers if some parts are ASTQueryParameter.
class ReplaceQueryParameterVisitor
{
public:
    explicit ReplaceQueryParameterVisitor(const NameToNameMap & parameters)
        : query_parameters(parameters)
    {}

    void visit(ASTPtr & ast);

    /// Resolve query parameters used as setting values (e.g. `max_threads = {threads:UInt64}`)
    /// directly in a SettingsChanges list. Used when settings from a query are applied to a
    /// context outside of the full AST traversal (e.g. on the client side).
    void visitSettingsChanges(SettingsChanges & changes);

    size_t getNumberOfReplacedParameters() const { return num_replaced_parameters; }

private:
    const NameToNameMap & query_parameters;
    size_t num_replaced_parameters = 0;

    /// Bodies of parameterized common table expressions, which must keep their `ASTQueryParameter`
    /// placeholders until the CTE invocation supplies the values. Filled while descending into a
    /// `SELECT` that both declares and invokes such a CTE, see `visitSelectQuery`.
    std::unordered_set<const IAST *> preserved_subtrees;

    const String & getParamValue(const String & name);
    void resolveParameterizedAlias(ASTPtr & ast);
    void visitIdentifier(ASTPtr & ast);
    void visitQueryParameter(ASTPtr & ast);
    void visitSetQuery(ASTSetQuery & set_query);
    void visitSelectQuery(ASTPtr & ast, const ASTSelectQuery & select_query);
    void visitChildren(ASTPtr & ast);

    /// Resolve a query parameter (name + declared type) into a concrete Field value.
    /// Shared by visitQueryParameter (which wraps it into an AST literal) and visitSettingsChanges
    /// (which stores it directly as a setting value).
    Field resolveParameterValueAsField(const String & name, const String & type_name);
};

/// Resolve query parameters used as setting values in a SettingsChanges list, in place.
/// Convenience wrapper around ReplaceQueryParameterVisitor::visitSettingsChanges.
void replaceQueryParametersInSettingsChanges(SettingsChanges & changes, const NameToNameMap & parameters);

}
