#pragma once

#include <Parsers/IAST.h>


namespace DB
{


/** INSERT запрос
  */
class ASTInsertQuery : public IAST
{
public:
    String database;
    String table;
    ASTPtr columns;
    String format;
    ASTPtr select;
    /// Идентификатор запроса INSERT. Используется при репликации.
    String insert_id;
    /// Данные для вставки
    const char * data = nullptr;
    const char * end = nullptr;

    ASTInsertQuery() = default;
    ASTInsertQuery(const StringRange range_) : IAST(range_) {}

    /** Получить текст, который идентифицирует этот элемент. */
    String getID() const override { return "InsertQuery_" + database + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTInsertQuery>(*this);
        res->children.clear();

        if (columns)     { res->columns = columns->clone();     res->children.push_back(res->columns); }
        if (select)     { res->select = select->clone();     res->children.push_back(res->select); }

        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
