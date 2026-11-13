#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE QUERY MASKING RULE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster] [IN access_storage]
  *      REGEXP 'regexp' [REPLACE 'replacement']
  *
  * ALTER QUERY MASKING RULE [IF EXISTS] name [ON CLUSTER cluster]
  *      [RENAME TO new_name] [REGEXP 'regexp'] [REPLACE 'replacement']
  */
class ParserCreateQueryMaskingRuleQuery : public IParserBase
{
public:
    void useAttachMode(bool attach_mode_ = true) { attach_mode = attach_mode_; }

    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const override { return "CREATE QUERY MASKING RULE or ALTER QUERY MASKING RULE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
