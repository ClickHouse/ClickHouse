#pragma once

#include <Core/SettingsEnums_fwd.h>
#include <Core/SettingsFields.h>
#include <QueryPipeline/SizeLimits.h>
#include <Formats/FormatSettings.h>


namespace DB
{

DECLARE_SETTING_ENUM(LoadBalancing)

DECLARE_SETTING_ENUM(JoinStrictness)

DECLARE_SETTING_ENUM(JoinAlgorithm)

DECLARE_SETTING_ENUM(TotalsMode)

/// The settings keeps OverflowMode which cannot be OverflowMode::ANY.
DECLARE_SETTING_ENUM(OverflowMode)

/// The settings keeps OverflowMode which can be OverflowMode::ANY.
DECLARE_SETTING_ENUM_WITH_RENAME(OverflowModeGroupBy, OverflowMode)

DECLARE_SETTING_ENUM(DistributedProductMode)

DECLARE_SETTING_ENUM_WITH_RENAME(DateTimeInputFormat, FormatSettings::DateTimeInputFormat)

DECLARE_SETTING_ENUM_WITH_RENAME(DateTimeOutputFormat, FormatSettings::DateTimeOutputFormat)

DECLARE_SETTING_ENUM(LogsLevel)

DECLARE_SETTING_ENUM_WITH_RENAME(LogQueriesType, QueryLogElementType)

DECLARE_SETTING_ENUM(DefaultDatabaseEngine)

DECLARE_SETTING_MULTI_ENUM(MySQLDataTypesSupport)

DECLARE_SETTING_ENUM(UnionMode)

DECLARE_SETTING_ENUM(DistributedDDLOutputMode)

DECLARE_SETTING_ENUM(HandleKafkaErrorMode)

DECLARE_SETTING_ENUM(ShortCircuitFunctionEvaluation)

DECLARE_SETTING_ENUM_WITH_RENAME(EnumComparingMode, FormatSettings::EnumComparingMode)

DECLARE_SETTING_ENUM_WITH_RENAME(EscapingRule, FormatSettings::EscapingRule)

}
