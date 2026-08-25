#include <Core/BaseSettings.h>
#include <Core/FormatFactorySettings.h>
#include <Core/SettingsEnums.h>

namespace DB
{

#define FORMAT_FACTORY_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, ArrowCompression) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, CapnProtoEnumComparingMode) \
    M(CLASS_NAME, Char) \
    M(CLASS_NAME, DateTimeInputFormat) \
    M(CLASS_NAME, DateTimeOutputFormat) \
    M(CLASS_NAME, DateTimeOverflowBehavior) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, EscapingRule) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, IdentifierQuotingRule) \
    M(CLASS_NAME, IdentifierQuotingStyle) \
    M(CLASS_NAME, InputFormatColumnMatchingCaseSensitivity) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, IntervalOutputFormat) \
    M(CLASS_NAME, MsgPackUUIDRepresentation) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, ORCCompression) \
    M(CLASS_NAME, ParquetCompression) \
    M(CLASS_NAME, ParquetVersion) \
    M(CLASS_NAME, SchemaInferenceMode) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI)

/*
 * User-specified file format settings for File and URL engines.
 */
DECLARE_SETTINGS_TRAITS(FormatFactorySettingsTraits, LIST_OF_ALL_FORMAT_SETTINGS, FORMAT_FACTORY_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(FormatFactorySettingsTraits, LIST_OF_ALL_FORMAT_SETTINGS, FormatFactorySettings, FormatFactorySetting)
}
