#include <Parsers/getTimeSeriesSettingVersion.h>

#include <Common/FieldVisitorConvertToNumber.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>


namespace DB
{

UInt64 getTimeSeriesSettingVersion(const ASTCreateQuery & query)
{
    if (!query.storage || !query.storage->settings)
        return TimeSeriesVersion::LATEST;

    const auto * value = query.storage->settings->changes.tryGet("version");
    if (!value)
        return TimeSeriesVersion::LATEST;

    /// The same conversion as in `SettingFieldUInt64`, so that every value the setting accepts (e.g. a string literal) is recognized here too.
    if (value->getType() == Field::Types::String)
        return parseWithSizeSuffix<UInt64>(value->safeGet<String>());
    return applyVisitor(FieldVisitorConvertToNumber<UInt64>(), *value);
}

}
