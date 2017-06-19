#pragma once

#include <Parsers/IAST.h>
#include <unordered_map>
#include <vector>


namespace DB
{

class WriteBuffer;
struct CollectAliases;
struct CollectTables;


/** For every ARRAY JOIN, collect a map:
  *  result alias -> source
  *
  * There could be several variants:
  *
  * SELECT elem FROM t ARRAY JOIN array AS elem            elem -> array
  * SELECT n.elem FROM t ARRAY JOIN nested AS n            n -> nested
  * SELECT array FROM t ARRAY JOIN array                   array -> array
  * SELECT nested.elem FROM t ARRAY JOIN nested            nested -> nested
  * SELECT elem FROM t ARRAY JOIN [1, 2, 3] AS elem        elem -> [1, 2, 3]
  *
  * Does not analyze arrayJoin functions.
  */
struct AnalyzeArrayJoins
{
    void process(const ASTPtr & ast);

    struct SourceInfo
    {
        String column_name;
        ASTPtr node;
    };

    using ResultToSource = std::unordered_map<String, SourceInfo>;
    using ArrayJoins = std::vector<ResultToSource>;

    /// Debug output
    void dump(WriteBuffer & out) const;
};

}
