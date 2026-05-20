#pragma once

#include <Parsers/IAST.h>

#include <Core/Field.h>

#include <optional>

namespace DB
{

/// Streaming query settings attached to a table expression:
///   FROM t STREAM [CURSOR '{...}']
///                 [WATERMARK FOR <col> AS <expr>]
///
struct ASTStreamSettings : public IAST
{
public:
    struct WatermarkSettings
    {
        String column;
        ASTPtr expression;
    };

    std::optional<Map> cursor;
    std::optional<WatermarkSettings> watermark;

    String getID(char) const override { return "ASTStreamSettings"; }
    ASTPtr clone() const override;
    bool hasTweaks() const;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
