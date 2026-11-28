#include <Interpreters/isRegisteredInFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Backups/BackupFactory.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Databases/DatabaseFactory.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Disks/DiskFactory.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/StringUtils.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

bool containsIgnoringCase(const auto & names, const String & name)
{
    return std::ranges::any_of(names, [&](const auto & registered) { return equalsCaseInsensitive(registered, name); });
}

}

bool isRegisteredInFactory(FactoryRestrictions::Kind kind, const String & name)
{
    using Kind = FactoryRestrictions::Kind;
    switch (kind)
    {
        case Kind::Formats:
        {
            const auto & formats = FormatFactory::instance().getAllFormats();
            return std::ranges::any_of(formats, [&](const auto & format) { return equalsCaseInsensitive(format.first, name); });
        }
        case Kind::TableEngines:
            return containsIgnoringCase(StorageFactory::instance().getAllRegisteredNames(), name);
        case Kind::TableFunctions:
            return containsIgnoringCase(TableFunctionFactory::instance().getAllRegisteredNames(), name);
        case Kind::DatabaseEngines:
            return containsIgnoringCase(DatabaseFactory::instance().getAllRegisteredNames(), name);
        case Kind::DictionarySources:
            return containsIgnoringCase(DictionarySourceFactory::instance().getAllRegisteredNames(), name);
        case Kind::DictionaryLayouts:
            return containsIgnoringCase(DictionaryFactory::instance().getAllRegisteredNames(), name);
        case Kind::Disks:
            return containsIgnoringCase(DiskFactory::instance().getAllRegisteredNames(), name);
        case Kind::BackupEngines:
            return containsIgnoringCase(BackupFactory::instance().getAllRegisteredNames(), name);
        case Kind::Functions:
            return containsIgnoringCase(FunctionFactory::instance().getAllRegisteredNames(), name);
        case Kind::AggregateFunctions:
            return containsIgnoringCase(AggregateFunctionFactory::instance().getAllRegisteredNames(), name);
        case Kind::AggregateFunctionCombinators:
        {
            const auto & combinators = AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators();
            return std::ranges::any_of(combinators, [&](const auto & combinator) { return equalsCaseInsensitive(combinator.name, name); });
        }
        case Kind::DataTypes:
            return containsIgnoringCase(DataTypeFactory::instance().getAllRegisteredNames(), name);
        case Kind::CompressionCodecs:
            return containsIgnoringCase(CompressionCodecFactory::instance().getAllRegisteredNames(), name);
        case Kind::SkipIndexes:
            return containsIgnoringCase(MergeTreeIndexFactory::instance().getAllRegisteredNames(), name);
        case Kind::Statistics:
            return containsIgnoringCase(MergeTreeStatisticsFactory::instance().getAllRegisteredNames(), name);
        case Kind::MAX:
            break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected factory kind {}", static_cast<int>(kind));
}

}
