#pragma once

#include <Core/IdentifierName.h>
#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

class ASTInterpolateElement : public IAST
{
public:
    String column;
    /// Quoting of the target column as written in the query.
    IdentifierPartQuote column_quote = IdentifierPartQuote::Unquoted;
    ASTPtr expr;

    String getID(char delim) const override { return String("InterpolateElement") + delim + "(column " + column + ")"; }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    /// `expr` is a separate pointer to a node that `children` holds too, so a visitor that replaces the child has to repair it.
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(nullptr, &expr);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
