#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// A name with optional parameters that is not a function call: a codec (`LZ4`, `ZSTD(3)`) or a
/// statistics type (`tdigest`).
class ASTNameWithParameters : public IAST
{
public:
    String name;

    /// Before 26.11 it was parsed as ASTFunction, hence it may have some garbage like IGNORE NULLS/RESPECT NULLS/OVER/TRIM
    bool legacy_syntax = false;

    ASTNameWithParameters() = default;
    explicit ASTNameWithParameters(String name_, ASTs parameters_ = {});

    /// `ASTExpressionList`, or null when there are no parameters.
    ASTPtr getParameters() const { return children.empty() ? nullptr : children[0]; }

    String getID(char delim) const override { return "NameWithParameters" + (delim + name); }
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/// Throws when a name in `ast` came from the legacy function syntax. Called for a new definition only:
/// a stored one must keep loading whatever an earlier version accepted.
void rejectLegacyNameSyntax(const IAST & ast);

}
