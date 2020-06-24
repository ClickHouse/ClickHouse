#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] name ON [database.]table
  *      [AS {PERMISSIVE | RESTRICTIVE}]
  *      [FOR {SELECT | INSERT | UPDATE | DELETE | ALL}]
  *      [USING condition]
  *      [WITH CHECK condition] [,...]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER [ROW] POLICY [IF EXISTS] name ON [database.]table
  *      [RENAME TO new_name]
  *      [AS {PERMISSIVE | RESTRICTIVE}]
  *      [FOR {SELECT | INSERT | UPDATE | DELETE | ALL}]
  *      [USING {condition | NONE}]
  *      [WITH CHECK {condition | NONE}] [,...]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ParserCreateRowPolicyQuery : public IParserBase
{
public:
    ParserCreateRowPolicyQuery & enableAttachMode(bool enable_) { attach_mode = enable_; return *this; }

protected:
    const char * getName() const override { return "CREATE ROW POLICY or ALTER ROW POLICY query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
