#include <Storages/Statistics/StatisticsJSONPaths.h>

#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeObject.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <ranges>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

SingleStatisticsDescription makeDescription(StatisticsType type)
{
    return SingleStatisticsDescription(type, make_intrusive<ASTIdentifier>(statisticsTypeToString(type)), false);
}

}

StatisticsJSONPaths::StatisticsJSONPaths(
    const SingleStatisticsDescription & description,
    const DataTypePtr & data_type_,
    ColumnStatisticsDescription::StatisticsTypeDescMap path_types_)
    : IStatistics(description)
    , data_type(data_type_)
    , path_types(std::move(path_types_))
{
}

DataTypePtr StatisticsJSONPaths::getPathType(const String & path) const
{
    const auto & object_type = assert_cast<const DataTypeObject &>(*data_type);
    const auto & typed_paths = object_type.getTypedPaths();
    if (auto it = typed_paths.find(path); it != typed_paths.end())
        return it->second;
    return object_type.getDynamicType();
}

void StatisticsJSONPaths::build(const ColumnPtr & column)
{
    chassert(serialized_path_statistics.empty());
    const auto full_column = column->convertToFullColumnIfConst();
    const auto & object_column = assert_cast<const ColumnObject &>(*full_column);
    rows += object_column.size();

    auto build_path = [&](const String & path, const ColumnPtr & path_column)
    {
        auto it = path_statistics.find(path);
        if (it == path_statistics.end())
        {
            auto path_stats = createPathStatistics(getPathType(path));
            if (!path_stats)
                return;
            it = path_statistics.emplace(path, std::move(path_stats)).first;
        }
        it->second->build(path_column);
    };

    for (const auto & [path, path_column] : object_column.getTypedPaths())
        build_path(path, path_column);
    for (const auto & [path, path_column] : object_column.getDynamicPaths())
        build_path(path, path_column);

    /// The values of each path in shared data, in a column of the rows that have the path.
    const auto dynamic_type = assert_cast<const DataTypeObject &>(*data_type).getDynamicType();
    std::unordered_map<std::string_view, MutableColumnPtr> shared_data_paths;
    const auto [shared_data_path_names, shared_data_values] = object_column.getSharedDataPathsAndValues();
    for (size_t i = 0; i < shared_data_path_names->size(); ++i)
    {
        auto & path_column = shared_data_paths[shared_data_path_names->getDataAt(i)];
        if (!path_column)
            path_column = dynamic_type->createColumn();
        ColumnObject::deserializeValueFromSharedData(shared_data_values, i, *path_column);
    }
    for (auto & [path, path_column] : shared_data_paths)
        build_path(String(path), std::move(path_column));

    addNullRowsToPaths();
}

void StatisticsJSONPaths::merge(const StatisticsPtr & other_stats)
{
    chassert(serialized_path_statistics.empty());
    const auto * other = typeid_cast<const StatisticsJSONPaths *>(other_stats.get());
    if (!other)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot merge JSONPaths statistics with a different type");

    rows += other->rows;

    /// `other` may be in the statistics cache, so its path statistics are copied, not shared.
    auto merge_path = [&](const String & path, const ColumnStatisticsPtr & other_path_stats)
    {
        auto it = path_statistics.find(path);
        if (it == path_statistics.end())
            it = path_statistics.emplace(path, other_path_stats->cloneEmpty()).first;
        it->second->merge(other_path_stats);
    };
    for (const auto & [path, other_path_stats] : other->path_statistics)
        merge_path(path, other_path_stats);
    for (const auto & [path, data] : other->serialized_path_statistics)
        if (auto other_path_stats = other->deserializePathStatistics(path, data))
            merge_path(path, other_path_stats);

    addNullRowsToPaths();
}

void StatisticsJSONPaths::addNullRowsToPaths()
{
    for (const auto & [_, path_stats] : path_statistics)
        if (path_stats->getNumRows() < rows)
            path_stats->addNullRows(rows - path_stats->getNumRows());
}

void StatisticsJSONPaths::serialize(WriteBuffer & buf)
{
    /// Layout: UInt64 path_types_mask, UInt64 rows, VarUInt number of paths, then for each path its name and its
    /// statistics in the layout of `ColumnStatistics::serialize`.
    UInt64 path_types_mask = 0;
    for (const auto & [type, _] : path_types)
        path_types_mask |= 1ULL << static_cast<UInt8>(type);
    writeIntBinary(path_types_mask, buf);
    writeIntBinary(rows, buf);

    writeVarUInt(path_statistics.size() + serialized_path_statistics.size(), buf);
    for (const auto & [path, path_stats] : path_statistics)
    {
        writeStringBinary(path, buf);
        WriteBufferFromOwnString path_buf;
        path_stats->serialize(path_buf);
        writeStringBinary(path_buf.str(), buf);
    }
    for (const auto & [path, data] : serialized_path_statistics)
    {
        writeStringBinary(path, buf);
        writeStringBinary(data, buf);
    }
}

void StatisticsJSONPaths::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    UInt64 path_types_mask = 0;
    readIntBinary(path_types_mask, buf);
    /// The types this server does not know are ignored, like in `stat_types_mask`.
    const auto & factory = MergeTreeStatisticsFactory::instance();
    for (size_t i = 0; i < static_cast<size_t>(StatisticsType::Max); ++i)
    {
        auto type = static_cast<StatisticsType>(i);
        if ((path_types_mask & (1ULL << i)) && factory.isKnownType(type))
            path_types.emplace(type, makeDescription(type));
    }
    readIntBinary(rows, buf);

    UInt64 num_paths = 0;
    readVarUInt(num_paths, buf);
    for (UInt64 i = 0; i < num_paths; ++i)
    {
        String path;
        String data;
        readStringBinary(path, buf);
        readStringBinary(data, buf);
        serialized_path_statistics.emplace(std::move(path), std::move(data));
    }
}

bool StatisticsJSONPaths::isCompatibleWith(const IStatistics & other) const
{
    const auto * other_paths = typeid_cast<const StatisticsJSONPaths *>(&other);
    return other_paths && std::ranges::equal(path_types | std::views::keys, other_paths->path_types | std::views::keys);
}

void StatisticsJSONPaths::copyStructureFrom(const IStatistics & other)
{
    path_types = assert_cast<const StatisticsJSONPaths &>(other).path_types;
}

String StatisticsJSONPaths::getNameForLogs() const
{
    return "JSONPaths : " + std::to_string(path_statistics.size() + serialized_path_statistics.size());
}

size_t StatisticsJSONPaths::allocatedBytes() const
{
    /// A node of `std::map` holds the value and three pointers and a color.
    static constexpr size_t map_node_overhead = 4 * sizeof(void *);

    size_t res = sizeof(*this);
    for (const auto & [type, desc] : path_types)
        res += sizeof(ColumnStatisticsDescription::StatisticsTypeDescMap::value_type) + map_node_overhead + desc.allocatedBytes();
    for (const auto & [path, path_stats] : path_statistics)
        res += sizeof(PathStatistics::value_type) + map_node_overhead + path.capacity() + path_stats->allocatedBytes();
    for (const auto & [path, data] : serialized_path_statistics)
        res += sizeof(decltype(serialized_path_statistics)::value_type) + map_node_overhead + path.capacity() + data.capacity();
    return res;
}

std::shared_ptr<StatisticsJSONPaths> StatisticsJSONPaths::cloneWithPaths(const NameSet & paths) const
{
    auto result = std::make_shared<StatisticsJSONPaths>(stat, data_type, path_types);
    result->rows = rows;
    for (const auto & path : paths)
    {
        if (auto it = path_statistics.find(path); it != path_statistics.end())
            result->path_statistics.emplace(*it);
        else if (auto serialized_it = serialized_path_statistics.find(path); serialized_it != serialized_path_statistics.end())
            if (auto path_stats = deserializePathStatistics(path, serialized_it->second))
                result->path_statistics.emplace(path, std::move(path_stats));
    }
    return result;
}

ColumnStatisticsPtr StatisticsJSONPaths::createPathStatistics(const DataTypePtr & path_type) const
{
    if (isObject(path_type))
        return nullptr;

    const auto & factory = MergeTreeStatisticsFactory::instance();
    ColumnStatisticsDescription path_desc;
    path_desc.data_type = path_type;
    path_desc.types_to_desc = path_types;
    path_desc = factory.cloneWithSupportedStatistics(path_desc, path_type);
    return path_desc.empty() ? nullptr : factory.get(path_desc);
}

ColumnStatisticsPtr StatisticsJSONPaths::deserializePathStatistics(const String & path, const String & data) const
{
    ReadBufferFromString buf(data);
    return ColumnStatistics::deserialize(buf, getPathType(path));
}

StatisticsPtr createJSONPathsStatistics(const ColumnStatisticsDescription & stats_desc)
{
    if (!stats_desc.data_type || !isObject(stats_desc.data_type) || !stats_desc.hasExplicitStatistics())
        return nullptr;

    ColumnStatisticsDescription::StatisticsTypeDescMap path_types;
    for (const auto & [type, desc] : stats_desc.types_to_desc)
        if (!desc.is_implicit)
            path_types.emplace(type, desc);
    /// A path is absent from part of the rows, and only `basic` counts them.
    path_types.emplace(StatisticsType::Basic, makeDescription(StatisticsType::Basic));

    return std::make_shared<StatisticsJSONPaths>(makeDescription(StatisticsType::JSONPaths), stats_desc.data_type, std::move(path_types));
}

bool jsonPathsStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    return isObject(data_type);
}

StatisticsPtr jsonPathsStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsJSONPaths>(description, data_type, ColumnStatisticsDescription::StatisticsTypeDescMap{});
}

}
