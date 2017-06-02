#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query specifying table name and, possibly, the database and the FORMAT section.
    */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    String database;
    String table;

    ASTQueryWithTableAndOutput() = default;
    ASTQueryWithTableAndOutput(const StringRange range_) : ASTQueryWithOutput(range_) {}

protected:
    void formatHelper(const FormatSettings & settings, FormatState & state, FormatStateStacked frame, const char * name) const
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << name << " " << (settings.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
    }
};


/// Declares the inheritance class of ASTQueryWithTableAndOutput with the implementation of methods getID and clone.
#define DEFINE_AST_QUERY_WITH_TABLE_AND_OUTPUT(Name, ID, Query) \
    class Name : public ASTQueryWithTableAndOutput \
    { \
    public: \
        Name() = default;                                                \
        Name(const StringRange range_) : ASTQueryWithTableAndOutput(range_) {} \
        String getID() const override { return ID"_" + database + "_" + table; }; \
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
            formatHelper(settings, state, frame, Query); \
        } \
    };
}
