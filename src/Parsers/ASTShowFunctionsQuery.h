#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTShowFunctionsQuery : public ASTQueryWithOutput
{
public:
    bool case_insensitive_like = false;
    String like;

    String getID(char) const override { return "ShowFunctions"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

    /// The `like` pattern and its `case_insensitive_like` modifier are plain members, not part of
    /// `children`, and `getID` is a constant. Fold them into the hash so the rewrite-rule matcher
    /// (which treats an equal tree hash as semantic equality) does not let a rule template for one
    /// `SHOW FUNCTIONS LIKE ...` over-match an unrelated one.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
