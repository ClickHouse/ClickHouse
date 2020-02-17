#pragma once

#include <Parsers/IAST.h>
#include <Access/Quota.h>


namespace DB
{
class ASTRoleList;


/** CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name
  *      [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
  *      [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
  *       {[SET] MAX {{QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = {number | ANY} } [,...] |
  *        [SET] TRACKING} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER QUOTA [IF EXISTS] name
  *      [RENAME TO new_name]
  *      [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
  *      [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
  *       {[SET] MAX {{QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = {number | ANY} } [,...] |
  *        [SET] TRACKING |
  *        UNSET TRACKING} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ASTCreateQuotaQuery : public IAST
{
public:
    bool alter = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    String name;
    String new_name;

    using KeyType = Quota::KeyType;
    std::optional<KeyType> key_type;

    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;
    static constexpr size_t MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;

    struct Limits
    {
        std::optional<ResourceAmount> max[MAX_RESOURCE_TYPE];
        bool unset_tracking = false;
        std::chrono::seconds duration = std::chrono::seconds::zero();
        bool randomize_interval = false;
    };
    std::vector<Limits> all_limits;

    std::shared_ptr<ASTRoleList> roles;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
