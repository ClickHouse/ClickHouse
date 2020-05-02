#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Access/Quota.h>


namespace DB
{
class ASTExtendedRoleSet;


/** CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name
  *      [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
  *      [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
  *       {MAX {{QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number} [,...] |
  *        NO LIMITS | TRACKING ONLY} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER QUOTA [IF EXISTS] name
  *      [RENAME TO new_name]
  *      [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
  *      [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
  *       {MAX {{QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number} [,...] |
  *        NO LIMITS | TRACKING ONLY} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ASTCreateQuotaQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    String name;
    String new_name;
    using KeyType = Quota::KeyType;
    std::optional<KeyType> key_type;

    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;
    static constexpr auto MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;

    struct Limits
    {
        std::optional<ResourceAmount> max[MAX_RESOURCE_TYPE];
        bool drop = false;
        std::chrono::seconds duration = std::chrono::seconds::zero();
        bool randomize_interval = false;
    };
    std::vector<Limits> all_limits;

    std::shared_ptr<ASTExtendedRoleSet> roles;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void replaceCurrentUserTagWithName(const String & current_user_name) const;
    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateQuotaQuery>(clone()); }
};
}
