#include <Functions/TypeMismatchStrictness.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>


namespace DB
{

namespace Setting
{
extern const SettingsBool variant_throw_on_type_mismatch;
extern const SettingsBool dynamic_throw_on_type_mismatch;
}

namespace
{

thread_local std::optional<bool> strictness_override;

ContextPtr tryGetQueryContext()
{
    if (CurrentThread::isInitialized())
        return CurrentThread::tryGetQueryContext();
    return nullptr;
}

}

bool shouldThrowOnVariantTypeMismatch()
{
    if (strictness_override)
        return *strictness_override;
    if (auto query_context = tryGetQueryContext())
        return query_context->getSettingsRef()[Setting::variant_throw_on_type_mismatch];
    /// No query context: the strict behavior is the default of the setting.
    return true;
}

bool shouldThrowOnDynamicTypeMismatch()
{
    if (strictness_override)
        return *strictness_override;
    if (auto query_context = tryGetQueryContext())
        return query_context->getSettingsRef()[Setting::dynamic_throw_on_type_mismatch];
    /// No query context: the strict behavior is the default of the setting.
    return true;
}

TypeMismatchStrictnessOverride::TypeMismatchStrictnessOverride(bool throw_on_type_mismatch)
    : previous(strictness_override)
{
    strictness_override = throw_on_type_mismatch;
}

TypeMismatchStrictnessOverride::~TypeMismatchStrictnessOverride()
{
    strictness_override = previous;
}

}
