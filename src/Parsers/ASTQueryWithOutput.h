#pragma once

#include <Parsers/IAST.h>
#include <IO/Operators.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/** Query with output options
  * (supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix).
  */
class ASTQueryWithOutput : public IAST
{
protected:
    struct ASTQueryWithOutputFlags
    {
        using ParentFlags = void;
        static constexpr UInt32 RESERVED_BITS = 3;

        UInt32 is_into_outfile_with_stdout : 1;
        UInt32 is_outfile_append : 1;
        UInt32 is_outfile_truncate : 1;
        UInt32 unused : 29;
    };
public:
    ASTPtr out_file;
    ASTPtr format_ast;
    ASTPtr settings_ast;
    ASTPtr compression;
    ASTPtr compression_level;

    /// Note that flags are initialized to zero (false) by default
    ASTQueryWithOutput() = default;

    bool isIntoOutfileWithStdout() const { return flags<ASTQueryWithOutputFlags>().is_into_outfile_with_stdout; }
    void setIsIntoOutfileWithStdout(bool value) { flags<ASTQueryWithOutputFlags>().is_into_outfile_with_stdout = value; }

    bool isOutfileAppend() const { return flags<ASTQueryWithOutputFlags>().is_outfile_append; }
    void setIsOutfileAppend(bool value) { flags<ASTQueryWithOutputFlags>().is_outfile_append = value; }

    bool isOutfileTruncate() const { return flags<ASTQueryWithOutputFlags>().is_outfile_truncate; }
    void setIsOutfileTruncate(bool value) { flags<ASTQueryWithOutputFlags>().is_outfile_truncate = value; }

    /// Remove 'FORMAT <fmt> and INTO OUTFILE <file>' if exists
    static bool resetOutputASTIfExist(IAST & ast);

    bool hasOutputOptions() const;

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
        auto res = make_intrusive<ASTQueryWithOutputImpl<ASTIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const override
    {
        ostr << ASTIDAndQueryNames::Query;
    }
};

}
