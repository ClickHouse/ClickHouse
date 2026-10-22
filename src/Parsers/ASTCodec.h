#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

class ASTExpressionList;

/// `CODEC(Delta(4), ZSTD(3))`: a chain of `ASTNameWithParameters`.
class ASTCodec : public IAST
{
public:
    ASTCodec() = default;
    explicit ASTCodec(ASTs elements_);

    /// `ASTExpressionList` of `ASTNameWithParameters`, never empty.
    ASTExpressionList & elements() const;

    String getID(char) const override { return "Codec"; }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
