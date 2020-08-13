#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name
  *      [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
  *      [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
  *       {MAX {{QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number} [,...] |
  *        NO LIMITS | TRACKING ONLY} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER QUOTA [IF EXISTS] name
  *      [RENAME TO new_name]
  *      [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
  *      [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
  *       {MAX {{QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number} } [,...] |
  *        NO LIMITS | TRACKING ONLY} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ParserCreateQuotaQuery : public IParserBase
{
public:
    ParserCreateQuotaQuery & enableAttachMode(bool enable_) { attach_mode = enable_; return *this; }

protected:
    const char * getName() const override { return "CREATE QUOTA or ALTER QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
