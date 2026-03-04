#pragma once

#include <memory>

namespace Poco
{
    class Logger;
}
using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{
    struct Settings;
    class Context;
    using ContextMutablePtr = std::shared_ptr<Context>;

    bool isDeduplicationEnabledForInsert(const Settings & settings);
    bool isDeduplicationEnabledForInsert(bool is_async_insert, const Settings & settings);

    bool isDeduplicationEnabledForInsertSelect(bool select_query_sorted, const Settings & settings, LoggerPtr logger = nullptr);

    void overrideDeduplicationSetting(bool is_on, ContextMutablePtr context);
}
