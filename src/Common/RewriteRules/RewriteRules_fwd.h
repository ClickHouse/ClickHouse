#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace DB
{

class RewriteRuleObject;
using RewriteRuleObjectPtr = std::shared_ptr<const RewriteRuleObject>;
using MutableRewriteRuleObjectPtr = std::shared_ptr<RewriteRuleObject>;
/// Holds the loaded rules keyed by name. The order in this container is irrelevant: rules are
/// applied in the order they are listed in the `query_rules` setting, not in creation order
/// (see `docs/en/operations/query-rules.md`).
using RewriteRuleObjectsList = std::vector<std::pair<std::string, MutableRewriteRuleObjectPtr>>;
}
