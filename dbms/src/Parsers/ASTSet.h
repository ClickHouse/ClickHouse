#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class Set;

/** The set. During the calculation, the expression in the IN section is replaced by the set
  *  - a subquery or an explicit enumeration of values.
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
        /** Prepared set. In user requests, this does not happen, but this happens after the intermediate query transformation.
          * Output it for not real (this will not be a valid query, but it will show that there was a set).
          */
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << "(...)"
            << (settings.hilite ? hilite_none : "");
    }
};

}
