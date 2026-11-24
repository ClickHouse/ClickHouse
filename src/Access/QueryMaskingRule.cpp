#include <Access/QueryMaskingRule.h>


namespace DB
{

bool QueryMaskingRule::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_rule = typeid_cast<const QueryMaskingRule &>(other);
    return (regexp == other_rule.regexp) && (replace == other_rule.replace);
}

}
