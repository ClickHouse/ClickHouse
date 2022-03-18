#pragma once

#include "IParserBase.h"

namespace DB
{

/** CREATE FUNCTION test AS x -> x || '1'
 *  !! '1'? `CREATE FUNCTION test AS 1` crashes
 *  CREATE [VOLATILE] FUNCTION name (args...) {
 *      body
 *  } USING interpreter
 *  !! permissions to use interpreters?
 */
class ParserCreateFunctionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE FUNCTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
