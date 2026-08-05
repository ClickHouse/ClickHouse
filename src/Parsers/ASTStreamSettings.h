#pragma once

#include <Parsers/IAST.h>

#include <Core/Streaming/Settings.h>

namespace DB
{

/// Streaming query settings attached to a table expression:
///   FROM t STREAM [CURSOR '{...}']
///                 [WATERMARK FOR <col> AS <expr> [IDLE TIMEOUT INTERVAL N SECOND]]
///
struct ASTStreamSettings : public IAST
{
    /// If true, read only the first snapshot and then finish (do not subscribe for updates).
    bool bounded = false;
    CursorTreeNodePtr cursor;
    WatermarkSettingsPtr watermark;

public:
    String getID(char) const override { return "ASTStreamSettings"; }
    ASTPtr clone() const override;
    bool hasTweaks() const;

    void setBounded(bool bounded_);
    void setCursor(CursorTreeNodePtr cursor_);
    void setWatermark(WatermarkSettingsPtr watermark_);

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
};

}
