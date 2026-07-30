#pragma once

#include <Parsers/IAST.h>

#include <Core/Field.h>

#include <optional>

namespace DB
{

/// Streaming query settings attached to a table expression:
///   FROM t STREAM [CURSOR '{...}']
///                 [WATERMARK FOR <col> AS <expr> [IDLE TIMEOUT INTERVAL N SECOND]]
///
struct ASTStreamSettings : public IAST
{
    struct WatermarkSettings
    {
        String column;
        ASTPtr expression;
        UInt64 idle_timeout_ms = 0;
    };

    std::optional<Map> cursor;
    std::optional<WatermarkSettings> watermark;

public:
    String getID(char) const override { return "ASTStreamSettings"; }
    ASTPtr clone() const override;
    bool hasTweaks() const;

protected:
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
