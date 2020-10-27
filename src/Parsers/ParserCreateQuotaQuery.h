#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name
  *      [KEYED BY {none | user_name | ip_address | client_key | client_key, user_name | client_key, ip_address} | NOT KEYED]
  *      [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day}
  *       {MAX {{queries | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time} = number} [,...] |
  *        NO LIMITS | TRACKING ONLY} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER QUOTA [IF EXISTS] name
  *      [RENAME TO new_name]
  *      [KEYED BY {none | user_name | ip_address | client_key | client_key, user_name | client_key, ip_address} | NOT KEYED]
  *      [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day}
  *       {MAX {{queries | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time} = number} [,...] |
  *        NO LIMITS | TRACKING ONLY} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ParserCreateQuotaQuery : public IParserBase
{
public:
    void useAttachMode(bool attach_mode_ = true) { attach_mode = attach_mode_; }

protected:
    const char * getName() const override { return "CREATE QUOTA or ALTER QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
