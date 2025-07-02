#pragma once

#include <Parsers/IAST.h>
#include <IO/Operators.h>
#include "Parsers/IAST_fwd.h"


namespace DB
{

/** Query with output options
  * (supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix).
  */
class ASTQueryWithOutput : public IAST
{
public:
    ASTPtr out_file;
    bool is_into_outfile_with_stdout = false;
    bool is_outfile_append = false;
    bool is_outfile_truncate = false;
    ASTPtr format_ast;
    ASTPtr settings_ast;
    ASTPtr compression;
    ASTPtr compression_level;

    /// Remove 'FORMAT <fmt> and INTO OUTFILE <file>' if exists
    static bool resetOutputASTIfExist(IAST & ast);

    bool hasOutputOptions() const;

protected:
    /// NOTE: call this helper at the end of the clone() method of descendant class.
    void cloneOutputOptions(ASTQueryWithOutput & cloned) const;

    /// Format only the query part of the AST (without output options).
    virtual void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const final;
};


/** Helper template for simple queries like SHOW PROCESSLIST.
  */
template <typename ASTIDAndQueryNames>
class ASTQueryWithOutputImpl : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return ASTIDAndQueryNames::ID; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTQueryWithOutputImpl<ASTIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        ostr << (settings.hilite ? hilite_keyword : "")
            << ASTIDAndQueryNames::Query << (settings.hilite ? hilite_none : "");
    }
};

}
