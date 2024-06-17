#pragma once

#include <Core/Names.h>
#include <Parsers/IAST.h>
#include <Common/SettingsChanges.h>

namespace DB
{

constexpr char QUERY_PARAMETER_NAME_PREFIX[] = "param_";

/** SET query
  */
class ASTSetQuery : public IAST
{
public:
    bool is_standalone = true; /// If false, this AST is a part of another query, such as SELECT.

    /// To support overriding certain settings in a **subquery**, we add a ASTSetQuery with Settings to all subqueries, containing
    /// the list of all settings that affect them (specifically or globally to the whole query).
    /// We use `print_in_format` to avoid printing these nodes when they were left unchanged from the parent copy
    /// See more: https://github.com/ClickHouse/ClickHouse/issues/38895
    bool print_in_format = true;

    SettingsChanges changes;
    /// settings that will be reset to default value
    std::vector<String> default_settings;
    NameToNameVector query_parameters;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Set"; }

    ASTPtr clone() const override { return std::make_shared<ASTSetQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    QueryKind getQueryKind() const override { return QueryKind::Set; }

    void appendColumnName(WriteBuffer & ostr) const override;
    void appendColumnNameWithoutAlias(WriteBuffer & ostr) const override { appendColumnName(ostr); }
};

}
