#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON
{
class Object;
}

namespace DB
{

/// Single rewrite or format action belonging to `EXPLAIN TEXT`.
class ASTExplainTextAction final : public IAST
{
public:
    enum class Kind : UInt8
    {
        Oneline,
        Multiline,
        ModifyLimit,
        ModifyOffset,
        Page,
        ModifyFormat,
    };

    explicit ASTExplainTextAction(Kind kind_);

    Kind getKind() const { return kind; }
    bool hasOperand() const;
    ASTPtr getOperand() const;
    void setOperand(ASTPtr operand);

    static String toString(Kind kind);
    static Kind fromString(const String & value);

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    void validateShape() const;

protected:
    void formatImpl(
        WriteBuffer & ostr,
        const FormatSettings & settings,
        FormatState & state,
        FormatStateStacked frame) const override;

private:
    Kind kind;
};

}
