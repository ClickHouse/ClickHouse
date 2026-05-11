#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Parses both CREATE and DROP NAMED SCALAR forms:
///   CREATE [OR REPLACE] [SHARED] NAMED SCALAR [IF NOT EXISTS] name
///       [ON CLUSTER cluster] [REFRESH EVERY <N> <unit>] AS <select>
///   DROP [SHARED] NAMED SCALAR [IF EXISTS] name [ON CLUSTER cluster]
///
/// SHARED rejects ON CLUSTER (the Keeper coordinator broadcasts to all
/// replicas already). The REFRESH grammar is fixed at `EVERY <N> <unit>`
/// where unit is SECOND/MINUTE/HOUR/DAY (with optional plural).
class ParserNamedScalarDDLQuery : public IParserBase
{
protected:
    const char * getName() const override { return "NAMED SCALAR DDL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
