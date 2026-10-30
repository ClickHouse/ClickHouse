#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/** CODEC name (codec, ...) in the column list of CREATE TABLE.
  * `name` is a column or a subcolumn of a column, written as a dotted name.
  */
class ASTCodecDeclaration : public IAST
{
public:
    ASTCodecDeclaration() = default;
    ASTCodecDeclaration(const String & name_, ASTPtr codec_);

    String name;

    String getID(char) const override { return "CodecDeclaration"; }

    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    /// The `CODEC(...)` function node, the same as `ASTColumnDeclaration::getCodec` returns.
    ASTPtr getCodec() const;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
