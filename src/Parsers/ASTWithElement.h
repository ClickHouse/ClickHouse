#pragma once

#include <Core/IdentifierName.h>
#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{
/** subquery in with statement
  */
class ASTWithElement : public IAST
{
public:
    String name;
    ASTPtr subquery;
    ASTPtr aliases;

    bool is_materialized = false; /// WITH t AS MATERIALIZED (subquery)
    /// Quoting of the CTE name as written in the query. Next to `is_materialized`, so it fits in the padding.
    IdentifierPartQuote name_quote = IdentifierPartQuote::Unquoted;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "WithElement"; }

    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
