#pragma once

#include <Parsers/ASTClusterQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>


namespace DB
{
/** Query like this:
  * CLUSTER
  *     [ADD NODE SERVER:PORT SHARD REPLICA]
  *     [PAUSE NODE SERVER:PORT]
  *     [START NODE SERVER:PORT]
  *     [DROP NODE SERVER:PORT]
  *     [REPLACE NODE OLD_SERVER:OLD_PORT NEW_SERVER:NEW_PORT]
  */
class ParserClusterQuery : public IParserBase
{
public:
    const char * getName() const override { return "CLUSTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    static bool parseServerPort(Pos & pos, std::shared_ptr<ASTClusterQuery> & query, Expected & expected);
    static bool parseServerPort(Pos & pos, String & server, UInt16 & port, Expected & expected);

private:
    Poco::Logger * log;
};

}
