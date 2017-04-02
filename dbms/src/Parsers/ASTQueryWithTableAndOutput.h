#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Запрос с указанием названия таблицы и, возможно, БД и секцией FORMAT.
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


/// Объявляет класс-наследник ASTQueryWithTableAndOutput с реализованными методами getID и clone.
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
