#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// 1. If a regular expression without alternatives starts with ^ or ends with an unescaped $, rewrite replaceRegexpAll
/// with replaceRegexpOne.
///
/// 2. If a replaceRegexpOne function has a regexp that matches entire haystack, and a replacement of nothing other than
/// \1 and some subpatterns in the regexp, or \0 and no subpatterns in the regexp, rewrite it with extract.
///
/// 3. If an extract function has a regexp with some subpatterns and the regexp starts with ^.* or ending with an
/// unescaped .*$, remove this prefix and/or suffix.
class RegexpFunctionRewritePass final : public IQueryTreePass
{
public:
    String getName() override { return "RegexpFunctionRewrite"; }

    String getDescription() override { return "Rewrite regexp related functions into more efficient forms."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

};

}
