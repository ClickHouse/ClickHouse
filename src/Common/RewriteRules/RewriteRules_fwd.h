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
/// Insertion-ordered container so that rules are applied in the order of their creation,
/// as documented in `docs/en/operations/query-rules.md`.
using RewriteRuleObjectsList = std::vector<std::pair<std::string, MutableRewriteRuleObjectPtr>>;

class RewriteRuleLog;
using RewriteRuleLogPtr = std::shared_ptr<const RewriteRuleLog>;
using MutableRewriteRuleLogPtr = std::shared_ptr<RewriteRuleLog>;
}
