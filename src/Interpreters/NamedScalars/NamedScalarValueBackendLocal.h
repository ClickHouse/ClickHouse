#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/NamedScalars/INamedScalarValueBackend.h>

#include <Common/Logger.h>

namespace DB
{

class NamedScalarValueBackendLocal final : public INamedScalarValueBackend
{
public:
    NamedScalarValueBackendLocal(String dir_path_, LoggerPtr log_);

    const String & directoryPath() const { return dir_path; }

    void initialize();
    void setGlobalContext(ContextPtr global_context_);

    std::optional<String> readValueBlob(const String & value_key) override;
    bool supportsValueWatches() const override { return false; }
    std::optional<String> readValueBlobAndWatch(const String & value_key, std::function<void()> on_change) override;
    void removeValue(const String & value_key) override;
    std::optional<NamedScalarRefreshLease> tryAcquireRefreshLease(
        const String & name,
        const String & value_key) override;

private:
    void writeValue(const String & value_key, const String & value_blob);

    String dir_path;
    LoggerPtr log;
    ContextPtr global_context;
};

}
