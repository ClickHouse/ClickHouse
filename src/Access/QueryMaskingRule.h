#pragma once

#include <Access/IAccessEntity.h>


namespace DB
{

/** A query masking rule defined by CREATE QUERY MASKING RULE.
  * Like a rule from the `query_masking_rules` server setting, it replaces every match of `regexp`
  * with `replace` in logged queries, log messages and exception messages of the whole server.
  */
struct QueryMaskingRule : public IAccessEntity
{
    String regexp;
    String replace = "******";

    [[nodiscard]] bool equal(const IAccessEntity & other) const override;
    [[nodiscard]] std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<QueryMaskingRule>(); }
    static constexpr auto TYPE = AccessEntityType::QUERY_MASKING_RULE;
    [[nodiscard]] AccessEntityType getType() const override { return TYPE; }

    [[nodiscard]] bool isBackupAllowed() const override { return true; }
};

using QueryMaskingRulePtr = std::shared_ptr<const QueryMaskingRule>;

}
