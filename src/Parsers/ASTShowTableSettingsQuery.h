#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Query SHOW TABLE SETTINGS
class ASTShowTableSettingsQuery : public ASTQueryWithOutput
{
public:
    String database;
    String table;

    /// `LIKE ''` is distinct from omitting the filter altogether.
    String like;
    bool has_like = false;
    bool not_like = false;
    bool case_insensitive_like = false;

    /// SHOW CHANGED TABLE SETTINGS - only the settings something other than the default set.
    bool changed = false;

    String getID(char) const override { return "ShowTableSettings"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
