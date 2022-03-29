#pragma once

#include <Core/Names.h>
#include <Parsers/IAST.h>
#include <Common/SettingsChanges.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

/** SET query
  */
class ASTSetQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    enum SetKind
    {
        Session, /// set works on current session context;
        Global, /// set works on global context level;
    };
    bool is_standalone = true; /// If false, this AST is a part of another query, such as SELECT.

    /// To support overriding certain settings in a **subquery**, we add a ASTSetQuery with Settings to all subqueries, containing
    /// the list of all settings that affect them (specifically or globally to the whole query).
    /// We use `print_in_format` to avoid printing these nodes when they were left unchanged from the parent copy
    /// See more: https://github.com/ClickHouse/ClickHouse/issues/38895
    bool print_in_format = true;

    SettingsChanges changes;
    NameToNameMap query_parameters;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Set"; }

    SetKind getKind() const { return kind; }

    void setSetKind(SetKind set_kind)
    {
        kind = set_kind;
    }

    ASTPtr clone() const override { return std::make_shared<ASTSetQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
       return removeOnCluster<ASTSetQuery>(clone());
    }

private:
    SetKind kind;

    static String toString (SetKind kind)
    {
        switch (kind)
        {
            case Session: return "SET";
            case Global: return "SET GLOBAL";
        }

        __builtin_unreachable();
    }
};

}
