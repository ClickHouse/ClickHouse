#pragma once

#include <Parsers/IAST.h>


namespace DB
{


/** USE запрос
  */
class ASTUseQuery : public IAST
{
public:
    String database;

    ASTUseQuery() = default;
    ASTUseQuery(const StringRange range_) : IAST(range_) {}

    /** Получить текст, который идентифицирует этот элемент. */
    String getID() const override { return "UseQuery_" + database; };

    ASTPtr clone() const override { return std::make_shared<ASTUseQuery>(*this); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "USE " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(database);
        return;
    }
};

}
