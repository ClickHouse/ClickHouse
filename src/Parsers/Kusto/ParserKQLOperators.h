#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class KQLOperators
{
public:
    String getExprFromToken(IParser::Pos pos);
protected:

    enum class WildcardsPos:uint8_t
    {
        none,
        left,
        right,
        both
    };

    enum class KQLOperatorValue : uint16_t
    {
        none,
        contains,
        not_contains,
        contains_cs,
        not_contains_cs,
        endswith,
        not_endswith,
        endswith_cs,
        not_endswith_cs,
        equal, //=~
        not_equal,//!~
        equal_cs, //=
        not_equal_cs,//!=
        has,
        not_has,
        has_all,
        has_any,
        has_cs,
        not_has_cs,
        hasprefix,
        not_hasprefix,
        hasprefix_cs,
        not_hasprefix_cs,
        hassuffix,
        not_hassuffix,
        hassuffix_cs,
        not_hassuffix_cs,
        in_cs,  //in
        not_in_cs, //!in
        in, //in~
        not_in ,//!in~
        matches_regex,
        startswith,
        not_startswith,
        startswith_cs,
        not_startswith_cs,
    };

    std::unordered_map <String,KQLOperatorValue> KQLOperator =
    {
        {"contains" , KQLOperatorValue::contains},
        {"!contains" , KQLOperatorValue::not_contains},
        {"contains_cs" , KQLOperatorValue::contains_cs},
        {"!contains_cs" , KQLOperatorValue::not_contains_cs},
        {"endswith" , KQLOperatorValue::endswith},
        {"!endswith" , KQLOperatorValue::not_endswith},
        {"endswith_cs" , KQLOperatorValue::endswith_cs},
        {"!endswith_cs" , KQLOperatorValue::not_endswith_cs},
        {"=~" , KQLOperatorValue::equal},
        {"!~" , KQLOperatorValue::not_equal},
        {"==" , KQLOperatorValue::equal_cs},
        {"!=" , KQLOperatorValue::not_equal_cs},
        {"has" , KQLOperatorValue::has},
        {"!has" , KQLOperatorValue::not_has},
        {"has_all" , KQLOperatorValue::has_all},
        {"has_any" , KQLOperatorValue::has_any},
        {"has_cs" , KQLOperatorValue::has_cs},
        {"!has_cs" , KQLOperatorValue::not_has_cs},
        {"hasprefix" , KQLOperatorValue::hasprefix},
        {"!hasprefix" , KQLOperatorValue::not_hasprefix},
        {"hasprefix_cs" , KQLOperatorValue::hasprefix_cs},
        {"!hasprefix" , KQLOperatorValue::not_hasprefix_cs},
        {"hassuffix" , KQLOperatorValue::hassuffix},
        {"!hassuffix" , KQLOperatorValue::not_hassuffix},
        {"hassuffix_cs" , KQLOperatorValue::hassuffix_cs},
        {"!hassuffix_cs" , KQLOperatorValue::not_hassuffix_cs},
        {"in" , KQLOperatorValue::in_cs},
        {"!in" , KQLOperatorValue::not_in_cs},
        {"in~" , KQLOperatorValue::in},
        {"!in~" , KQLOperatorValue::not_in},
        {"matches regex" , KQLOperatorValue::matches_regex},
        {"startswith" , KQLOperatorValue::startswith},
        {"!startswith" , KQLOperatorValue::not_startswith},
        {"startswith_cs" , KQLOperatorValue::startswith_cs},
        {"!startswith_cs" , KQLOperatorValue::not_startswith_cs},
    };
    static String genHaystackOpExpr(std::vector<String> &tokens,IParser::Pos &token_pos,String kql_op, String ch_op, WildcardsPos wildcards_pos);
};

}
