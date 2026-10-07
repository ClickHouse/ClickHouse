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

thread_local std::optional<bool> variant_strictness_override;
thread_local std::optional<bool> dynamic_strictness_override;

ContextPtr tryGetQueryContext()
{
    if (CurrentThread::isInitialized())
        return CurrentThread::tryGetQueryContext();
    return nullptr;
}

}

bool shouldThrowOnVariantTypeMismatch()
{
    if (variant_strictness_override)
        return *variant_strictness_override;
    if (auto query_context = tryGetQueryContext())
        return query_context->getSettingsRef()[Setting::variant_throw_on_type_mismatch];
    /// No query context: the strict behavior is the default of the setting.
    return true;
}

bool shouldThrowOnDynamicTypeMismatch()
{
    if (dynamic_strictness_override)
        return *dynamic_strictness_override;
    if (auto query_context = tryGetQueryContext())
        return query_context->getSettingsRef()[Setting::dynamic_throw_on_type_mismatch];
    /// No query context: the strict behavior is the default of the setting.
    return true;
}

TypeMismatchStrictnessOverride::TypeMismatchStrictnessOverride(bool variant_throw_on_type_mismatch, bool dynamic_throw_on_type_mismatch)
    : previous_variant(variant_strictness_override)
    , previous_dynamic(dynamic_strictness_override)
{
    variant_strictness_override = variant_throw_on_type_mismatch;
    dynamic_strictness_override = dynamic_throw_on_type_mismatch;
}

TypeMismatchStrictnessOverride::~TypeMismatchStrictnessOverride()
{
    variant_strictness_override = previous_variant;
    dynamic_strictness_override = previous_dynamic;
}

}
