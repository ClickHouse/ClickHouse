#pragma once

#include <Parsers/ASTQueryWithOutput.h>


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
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    enum class Mode
    {
        Unspecified,
        ALL,
        DISTINCT
    };

    using Modes = std::vector<Mode>;

    Modes union_modes;

    ASTPtr list_of_selects;

    /// we need flatten_nodes to help build nested_interpreter
    ASTPtr flatten_nodes_list;
};

}
