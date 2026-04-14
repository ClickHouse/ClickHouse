#pragma once

#include <Interpreters/SystemLogFlushPolicy.h>

namespace DB
{

/// Default implementation of `ISystemLogFlushPolicy`.
class DefaultSystemLogFlushPolicy : public ISystemLogFlushPolicy
{
public:
    explicit DefaultSystemLogFlushPolicy(bool skip_alias_columns_ = false)
        : skip_alias_columns(skip_alias_columns_) {}

    bool isManualFlush(uint64_t /*to_flush_end*/) override { return false; }
    void prepareManualFlush(uint64_t /*target_index*/) override {}
    void afterFlush(const BlockIO & /*io*/, bool /*is_manual_flush*/, size_t /*flush_size*/) override {}
    void addInsertSettings(ContextMutablePtr & /*context*/) override {}
    bool shouldSkipAliasColumns() override { return skip_alias_columns; }

private:
    bool skip_alias_columns = false;
};

}
