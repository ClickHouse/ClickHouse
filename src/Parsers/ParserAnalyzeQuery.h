#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** ANALYZE query: collecting table statistics.
 *
 *  ANALYZE [FULL|SAMPLE] TABLE table_name (column_name [, column_name]) [ON CLUSTER cluster_name] [ASYNC | SYNC]
 *  [SETTINGS key1 = value1[, key2 = value2, ...]];
 */
class ParserAnalyzeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "Analyze table"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
