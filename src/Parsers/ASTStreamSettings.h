#pragma once

#include <Parsers/IAST.h>

#include <Core/Field.h>

#include <optional>

namespace DB
{

/// Streaming query settings attached to a table expression:
///   FROM t STREAM [CURSOR '{...}']
///
class ASTStreamSettings : public IAST
{
public:
    struct StreamSettings
    {
        std::optional<Map> cursor_tree;
    };

    StreamSettings settings;

    ASTStreamSettings() = default;
    explicit ASTStreamSettings(StreamSettings settings_);

    String getID(char) const override { return "ASTStreamSettings"; }

    ASTPtr clone() const override { return make_intrusive<ASTStreamSettings>(*this); }

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
