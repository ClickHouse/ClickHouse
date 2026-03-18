#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/SelectUnionMode.h>

namespace DB
{
/** Single SELECT query or multiple SELECT queries with UNION
 * or UNION or UNION DISTINCT
  */
class ASTSelectWithUnionQuery : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "SelectWithUnionQuery"; }

    ASTPtr clone() const override;

    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    QueryKind getQueryKind() const override { return QueryKind::Select; }

    SelectUnionMode union_mode;

    SelectUnionModes list_of_modes;

    bool is_normalized = false;

    ASTPtr list_of_selects;

    SelectUnionModesSet set_of_modes;

    /// Consider any mode other than ALL as non-default.
    bool hasNonDefaultUnionMode() const;

    bool hasQueryParameters() const;

private:
    /// This variable is optional as we want to set it on the first call to hasQueryParameters
    /// and return the same variable on future calls to hasQueryParameters
    /// its mutable as we set it in const function
    mutable std::optional<bool> has_query_parameters;

};

}
