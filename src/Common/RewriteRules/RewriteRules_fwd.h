#pragma once
#include <memory>
#include <map>

namespace DB
{

class RewriteRuleObject;
using RewriteRuleObjectPtr = std::shared_ptr<const RewriteRuleObject>;
using MutableRewriteRuleObjectPtr = std::shared_ptr<RewriteRuleObject>;
using RewriteRuleObjectsMap = std::map<std::string, MutableRewriteRuleObjectPtr>;

class RewriteRuleLog;
using RewriteRuleLogPtr = std::shared_ptr<const RewriteRuleLog>;
using MutableRewriteRuleLogPtr = std::shared_ptr<RewriteRuleLog>;
}
