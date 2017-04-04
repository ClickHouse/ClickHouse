#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class Set;

/** Множество. В процессе вычисления, на множество заменяется выражение в секции IN
  *  - подзапрос или явное перечисление значений.
  */
class ASTSet : public IAST
{
public:
    std::shared_ptr<Set> set;
    String column_name;
    bool is_explicit = false;

    ASTSet(const String & column_name_) : column_name(column_name_) {}
    ASTSet(const StringRange range_, const String & column_name_) : IAST(range_), column_name(column_name_) {}
    String getID() const override { return "Set_" + getColumnName(); }
    ASTPtr clone() const override { return std::make_shared<ASTSet>(*this); }
    String getColumnName() const override { return column_name; }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        /** Подготовленное множество. В пользовательских запросах такого не бывает, но такое бывает после промежуточных преобразований запроса.
          * Выведем его не по-настоящему (это не будет корректным запросом, но покажет, что здесь было множество).
          */
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << "(...)"
            << (settings.hilite ? hilite_none : "");
    }
};

}
