#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** Query with output options (supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] suffix).
  */
class ASTQueryWithOutput : public IAST
{
public:
    ASTPtr out_file;
    ASTPtr format;

    ASTQueryWithOutput() = default;
    explicit ASTQueryWithOutput(const StringRange range_) : IAST(range_) {}

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const final;

protected:
    /// NOTE: call this helper at the end of the clone() method of descendant class.
    void cloneOutputOptions(ASTQueryWithOutput & cloned) const;

    /// Format only the query part of the AST (without output options).
    virtual void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;
};


template <typename AstIDAndQueryNames>
class ASTQueryWithOutputImpl : public ASTQueryWithOutput
{
public:
    explicit ASTQueryWithOutputImpl() = default;
    explicit ASTQueryWithOutputImpl(StringRange range_) : ASTQueryWithOutput(range_) {}
    String getID() const override { return AstIDAndQueryNames::ID; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTQueryWithOutputImpl<AstIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
                      << AstIDAndQueryNames::Query << (settings.hilite ? hilite_none : "");
    }
};

}
