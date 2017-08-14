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

protected:
    /// NOTE: call this helper at the end of the clone() method of descendant class.
    void cloneOutputOptions(ASTQueryWithOutput & cloned) const;

    /// Format only the query part of the AST (without output options).
    virtual void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override final;
};


/// Declares the class-successor of ASTQueryWithOutput with implemented methods getID and clone.
#define DEFINE_AST_QUERY_WITH_OUTPUT(Name, ID, Query) \
class Name : public ASTQueryWithOutput \
{ \
public: \
    Name() {} \
    Name(StringRange range_) : ASTQueryWithOutput(range_) {} \
    String getID() const override { return ID; }; \
    \
    ASTPtr clone() const override \
    { \
        std::shared_ptr<Name> res = std::make_shared<Name>(*this); \
        res->children.clear(); \
        cloneOutputOptions(*res); \
        return res; \
    } \
\
protected: \
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override \
    { \
        settings.ostr << (settings.hilite ? hilite_keyword : "") << Query << (settings.hilite ? hilite_none : ""); \
    } \
};

}
