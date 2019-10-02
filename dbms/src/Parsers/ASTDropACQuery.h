#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** DROP ROLE [IF EXISTS] role [,...]
    DROP USER [IF EXISTS] user [,...]
  */
template <typename AttributesT>
class ASTDropAttributesQuery : public IAST
{
public:
    ASTs names;
    bool if_exists = false;

    String getID(char) const override { return "DROP " + AttribitesT::TYPE.name + " query"; }
    ASTPtr clone() const override { return std::make_shared<ASTDropAttributesQuery<AttributesT>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
                      << "DROP USER "
                      << (if_exists ? " IF EXISTS" : "")
                      << (settings.hilite ? hilite_none : "");

        for (size_t i = 0; i != user_names.size(); ++i)
        {
            settings.ostr << (i ? ", " : " ");
            user_names[i]->format(settings);
        }
    }
};
}
