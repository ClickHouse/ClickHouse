#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Formats/FormatSettings.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTQueryParameter;
class ASTSetQuery;
class Field;
class SettingsChanges;

/// Visit substitutions in a query, replace ASTQueryParameter with ASTLiteral.
/// Rebuild ASTIdentifiers if some parts are ASTQueryParameter.
class ReplaceQueryParameterVisitor
{
public:
    /// Format settings affect how parameter values are parsed, e.g. `date_time_input_format`
    /// for a `{param:DateTime}` parameter. Pass the settings obtained from the query context
    /// (see `getFormatSettings`) so that the corresponding settings of the user are respected.
    ReplaceQueryParameterVisitor(const NameToNameMap & parameters, const FormatSettings & format_settings_)
        : query_parameters(parameters)
        , format_settings(format_settings_)
    {}

    void visit(ASTPtr & ast);

    /// Resolve query parameters used as setting values (e.g. `max_threads = {threads:UInt64}`)
    /// directly in a SettingsChanges list. Used when settings from a query are applied to a
    /// context outside of the full AST traversal (e.g. on the client side).
    void visitSettingsChanges(SettingsChanges & changes);

    size_t getNumberOfReplacedParameters() const { return num_replaced_parameters; }

private:
    const NameToNameMap & query_parameters;
    FormatSettings format_settings;
    size_t num_replaced_parameters = 0;

    const String & getParamValue(const String & name);
    void resolveParameterizedAlias(ASTPtr & ast);
    void visitIdentifier(ASTPtr & ast);
    void visitQueryParameter(ASTPtr & ast);
    void visitSetQuery(ASTSetQuery & set_query);
    void visitChildren(ASTPtr & ast);

    /// Resolve a query parameter (name + declared type) into a concrete Field value.
    /// Shared by visitQueryParameter (which wraps it into an AST literal) and visitSettingsChanges
    /// (which stores it directly as a setting value).
    Field resolveParameterValueAsField(const String & name, const String & type_name);
};

/// Resolve query parameters used as setting values in a SettingsChanges list, in place.
/// Convenience wrapper around ReplaceQueryParameterVisitor::visitSettingsChanges.
void replaceQueryParametersInSettingsChanges(SettingsChanges & changes, const NameToNameMap & parameters, const FormatSettings & format_settings);

}
