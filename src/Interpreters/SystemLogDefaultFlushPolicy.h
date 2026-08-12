#pragma once

#include <Interpreters/SystemLogFlushPolicy.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Default implementation of `ISystemLogFlushPolicy`.
class DefaultSystemLogFlushPolicy : public ISystemLogFlushPolicy
{
public:
    explicit DefaultSystemLogFlushPolicy(const Poco::Util::AbstractConfiguration & config)
        : skip_alias_columns(config.getBool("default_system_log_flush_policy.skip_alias_columns", false)) {}

    bool isManualFlush(uint64_t /*to_flush_end*/) override { return false; }
    void prepareManualFlush(uint64_t /*target_index*/) override {}
    void afterFlush(const BlockIO & /*io*/, bool /*is_manual_flush*/, size_t /*flush_size*/) override {}
    void addInsertSettings(ContextMutablePtr & /*context*/) override {}
    bool shouldSkipAliasColumns() override { return skip_alias_columns; }

private:
    bool skip_alias_columns;
};

}
