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

    using UnionModes = std::vector<Mode>;
    using UnionModesSet = std::unordered_set<Mode>;

    Mode union_mode;

    UnionModes list_of_modes;

    bool is_normalized = false;

    ASTPtr list_of_selects;

    UnionModesSet set_of_modes;

    /// Consider any mode other than ALL as non-default.
    bool hasNonDefaultUnionMode() const;
};

}
