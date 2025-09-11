#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/Settings.h>
#include <IO/S3Common.h>
#include <IO/S3Defines.h>
#include <IO/S3RequestSettings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/Throttler.h>
#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>

#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace ProfileEvents
{
    extern const Event S3GetRequestThrottlerCount;
    extern const Event S3GetRequestThrottlerSleepMicroseconds;
    extern const Event S3PutRequestThrottlerCount;
    extern const Event S3PutRequestThrottlerSleepMicroseconds;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 s3_max_get_burst;
    extern const SettingsUInt64 s3_max_get_rps;
    extern const SettingsUInt64 s3_max_put_burst;
    extern const SettingsUInt64 s3_max_put_rps;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_SETTING_VALUE;
}

#define REQUEST_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, max_single_read_retries, 4, "", 0) \
    DECLARE(UInt64, request_timeout_ms, S3::DEFAULT_REQUEST_TIMEOUT_MS, "", 0) \
    DECLARE(UInt64, list_object_keys_size, S3::DEFAULT_LIST_OBJECT_KEYS_SIZE, "", 0) \
    DECLARE(Bool, allow_native_copy, S3::DEFAULT_ALLOW_NATIVE_COPY, "", 0) \
    DECLARE(Bool, check_objects_after_upload, S3::DEFAULT_CHECK_OBJECTS_AFTER_UPLOAD, "", 0) \
    DECLARE(Bool, throw_on_zero_files_match, false, "", 0) \
    DECLARE(Bool, allow_multipart_copy, true, "", 0) \
    DECLARE(UInt64, max_single_operation_copy_size, S3::DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE, "", 0) \
    DECLARE(String, storage_class_name, "", "", 0) \
    DECLARE(UInt64, http_max_fields, 1000000, "", 0) \
    DECLARE(UInt64, http_max_field_name_size, 128 * 1024, "", 0) \
    DECLARE(UInt64, http_max_field_value_size, 128 * 1024, "", 0)

#define PART_UPLOAD_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, strict_upload_part_size, 0, "", 0) \
    DECLARE(UInt64, min_upload_part_size, S3::DEFAULT_MIN_UPLOAD_PART_SIZE, "", 0) \
    DECLARE(UInt64, max_upload_part_size, S3::DEFAULT_MAX_UPLOAD_PART_SIZE, "", 0) \
    DECLARE(UInt64, upload_part_size_multiply_factor, S3::DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_FACTOR, "", 0) \
    DECLARE(UInt64, upload_part_size_multiply_parts_count_threshold, S3::DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_PARTS_COUNT_THRESHOLD, "", 0) \
    DECLARE(UInt64, max_inflight_parts_for_one_file, S3::DEFAULT_MAX_INFLIGHT_PARTS_FOR_ONE_FILE, "", 0) \
    DECLARE(UInt64, max_part_number, S3::DEFAULT_MAX_PART_NUMBER, "", 0) \
    DECLARE(UInt64, max_single_part_upload_size, S3::DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE, "", 0) \
    DECLARE(UInt64, max_unexpected_write_error_retries, 4, "", 0)

#define REQUEST_SETTINGS_LIST(M, ALIAS) \
    REQUEST_SETTINGS(M, ALIAS) \
    PART_UPLOAD_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(S3RequestSettingsTraits, REQUEST_SETTINGS_LIST)
IMPLEMENT_SETTINGS_TRAITS(S3RequestSettingsTraits, REQUEST_SETTINGS_LIST)

struct S3RequestSettingsImpl : public BaseSettings<S3RequestSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) S3RequestSettings##TYPE NAME = &S3RequestSettingsImpl ::NAME;

namespace S3RequestSetting
{
REQUEST_SETTINGS_LIST(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

namespace S3
{

namespace
{
bool setValueFromConfig(
    const Poco::Util::AbstractConfiguration & config, const std::string & path, typename S3RequestSettingsImpl::SettingFieldRef & field)
{
    if (!config.has(path))
        return false;

    auto which = field.getValue().getType();
    if (isInt64OrUInt64FieldType(which))
        field.setValue(config.getUInt64(path));
    else if (which == Field::Types::String)
        field.setValue(config.getString(path));
    else if (which == Field::Types::Bool)
        field.setValue(config.getBool(path));
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type: {}", field.getTypeName());

    return true;
}
}

S3RequestSettings::S3RequestSettings() : impl(std::make_unique<S3RequestSettingsImpl>())
{
}

S3RequestSettings::S3RequestSettings(const S3RequestSettings & settings)
    : get_request_throttler(settings.get_request_throttler)
    , put_request_throttler(settings.put_request_throttler)
    , proxy_resolver(settings.proxy_resolver)
    , impl(std::make_unique<S3RequestSettingsImpl>(*settings.impl))
{
}

S3RequestSettings::S3RequestSettings(S3RequestSettings && settings) noexcept
    : get_request_throttler(std::move(settings.get_request_throttler))
    , put_request_throttler(std::move(settings.put_request_throttler))
    , proxy_resolver(std::move(settings.proxy_resolver))
    , impl(std::make_unique<S3RequestSettingsImpl>(std::move(*settings.impl)))
{
}

S3RequestSettings::S3RequestSettings(
    const Poco::Util::AbstractConfiguration & config,
    const DB::Settings & settings,
    const std::string & config_prefix,
    const std::string & setting_name_prefix,
    bool validate_settings)
    : S3RequestSettings()
{
    for (auto & field : impl->allMutable())
    {
        auto path = fmt::format("{}.{}{}", config_prefix, setting_name_prefix, field.getName());

        bool updated = setValueFromConfig(config, path, field);
        if (!updated)
        {
            auto setting_name = "s3_" + field.getName();
            if (settings.has(setting_name) && settings.isChanged(setting_name))
                field.setValue(settings.get(setting_name));
        }
    }
    finishInit(settings, validate_settings);
}

S3RequestSettings::S3RequestSettings(const NamedCollection & collection, const DB::Settings & settings, bool validate_settings)
    : S3RequestSettings()
{
    auto values = impl->allMutable();
    for (auto & field : values)
    {
        const auto path = field.getName();
        if (collection.has(path))
        {
            auto which = field.getValue().getType();
            if (isInt64OrUInt64FieldType(which))
                field.setValue(collection.get<UInt64>(path));
            else if (which == Field::Types::String)
                field.setValue(collection.get<String>(path));
            else if (which == Field::Types::Bool)
                field.setValue(collection.get<bool>(path));
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type: {}", field.getTypeName());
        }
    }
    finishInit(settings, validate_settings);
}

S3RequestSettings::~S3RequestSettings() = default;

S3REQUEST_SETTINGS_SUPPORTED_TYPES(S3RequestSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

S3RequestSettings & S3RequestSettings::operator=(S3RequestSettings && settings) noexcept
{
    get_request_throttler = std::move(settings.get_request_throttler);
    put_request_throttler = std::move(settings.put_request_throttler);
    proxy_resolver = std::move(settings.proxy_resolver);
    *impl = std::move(*settings.impl);

    return *this;
}

void S3RequestSettings::updateFromSettings(const DB::Settings & settings, bool if_changed, bool validate_settings)
{
    for (auto & field : impl->allMutable())
    {
        const auto setting_name = "s3_" + field.getName();
        if (settings.has(setting_name) && (!if_changed || settings.isChanged(setting_name)))
        {
            impl->set(field.getName(), settings.get(setting_name));
        }
    }

    normalizeSettings();
    if (validate_settings)
        validateUploadSettings();
}

void S3RequestSettings::updateIfChanged(const S3RequestSettings & settings)
{
    for (auto & setting : settings.impl->all())
    {
        if (setting.isValueChanged())
            impl->set(setting.getName(), setting.getValue());
    }
}

void S3RequestSettings::validateUploadSettings()
{
    if (!impl->max_part_number)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting max_part_number cannot be zero");

    if (!impl->strict_upload_part_size)
    {
        if (!impl->min_upload_part_size)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Setting min_upload_part_size ({}) cannot be zero",
                ReadableSize(impl->min_upload_part_size));

        if (impl->max_upload_part_size < impl->min_upload_part_size)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Setting max_upload_part_size ({}) can't be less than setting min_upload_part_size ({})",
                ReadableSize(impl->max_upload_part_size), ReadableSize(impl->min_upload_part_size));

        if (!impl->upload_part_size_multiply_factor)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Setting upload_part_size_multiply_factor cannot be zero");

        if (!impl->upload_part_size_multiply_parts_count_threshold)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Setting upload_part_size_multiply_parts_count_threshold cannot be zero");

        size_t maybe_overflow;
        if (common::mulOverflow(impl->max_upload_part_size.value, impl->upload_part_size_multiply_factor.value, maybe_overflow))
            throw Exception(
                            ErrorCodes::INVALID_SETTING_VALUE,
                            "Setting upload_part_size_multiply_factor is too big ({}). "
                            "Multiplication to max_upload_part_size ({}) will cause integer overflow",
                            impl->upload_part_size_multiply_factor.value, ReadableSize(impl->max_upload_part_size));
    }

    std::unordered_set<String> storage_class_names {"STANDARD", "INTELLIGENT_TIERING"};
    if (!impl->storage_class_name.value.empty() && !storage_class_names.contains(impl->storage_class_name))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Setting storage_class has invalid value {} which only supports STANDARD and INTELLIGENT_TIERING",
            impl->storage_class_name.value);

    /// TODO: it's possible to set too small limits.
    /// We can check that max possible object size is not too small.
}

void S3RequestSettings::finishInit(const DB::Settings & settings, bool validate_settings)
{
    normalizeSettings();
    if (validate_settings)
        validateUploadSettings();

    /// NOTE: it would be better to reuse old throttlers
    /// to avoid losing token bucket state on every config reload,
    /// which could lead to exceeding limit for short time.
    /// But it is good enough unless very high `burst` values are used.
    if (UInt64 max_get_rps = impl->isChanged("max_get_rps") ? impl->get("max_get_rps").safeGet<UInt64>() : settings[Setting::s3_max_get_rps])
    {
        size_t default_max_get_burst
            = settings[Setting::s3_max_get_burst] ? settings[Setting::s3_max_get_burst] : (Throttler::default_burst_seconds * max_get_rps);

        size_t max_get_burst = impl->isChanged("max_get_burst") ? impl->get("max_get_burst").safeGet<UInt64>() : default_max_get_burst;
        get_request_throttler = std::make_shared<Throttler>(max_get_rps, max_get_burst, ProfileEvents::S3GetRequestThrottlerCount, ProfileEvents::S3GetRequestThrottlerSleepMicroseconds);
    }
    if (UInt64 max_put_rps = impl->isChanged("max_put_rps") ? impl->get("max_put_rps").safeGet<UInt64>() : settings[Setting::s3_max_put_rps])
    {
        size_t default_max_put_burst
            = settings[Setting::s3_max_put_burst] ? settings[Setting::s3_max_put_burst] : (Throttler::default_burst_seconds * max_put_rps);
        size_t max_put_burst = impl->isChanged("max_put_burst") ? impl->get("max_put_burst").safeGet<UInt64>() : default_max_put_burst;
        put_request_throttler = std::make_shared<Throttler>(max_put_rps, max_put_burst, ProfileEvents::S3PutRequestThrottlerCount, ProfileEvents::S3PutRequestThrottlerSleepMicroseconds);
    }
}

void S3RequestSettings::normalizeSettings()
{
    if (!impl->storage_class_name.value.empty() && impl->storage_class_name.changed)
        impl->storage_class_name = Poco::toUpperInPlace(impl->storage_class_name.value);
}

}

}
