#pragma once

#include "IParserBase.h"

namespace DB
{

/// CREATE MODEL model
/// ALGORITHM 'xgboost
/// [OPTIONS (key1 = value1, key2 = value2)]
/// TARGET 'target'
/// FROM TABLE table_name /* TODO: FROM (tableFunction) */
class ParserCreateModelQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE MODEL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
