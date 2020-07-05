#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user_name | CURRENT_USER} [,...] [WITH GRANT OPTION]
  * REVOKE access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user_name | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | CURRENT_USER} [,...]
  */
class ParserGrantQuery : public IParserBase
{
public:
    ParserGrantQuery & useAttachMode(bool attach_mode_ = true) { attach_mode = attach_mode_; return *this; }

protected:
    const char * getName() const override { return "GRANT or REVOKE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
