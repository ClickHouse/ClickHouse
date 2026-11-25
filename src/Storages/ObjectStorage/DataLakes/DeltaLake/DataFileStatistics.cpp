#include <Storages/ObjectStorage/DataLakes/DeltaLake/DataFileStatistics.h>

#if USE_DELTA_KERNEL_RS

#include <Common/DateLUT.h>
#include <Common/assert_cast.h>
#include <Common/intExp10.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#if USE_SIMDJSON
#include <Common/JSONParsers/SimdJSONParser.h>
#endif

#include <unordered_set>

namespace DeltaLake
{

namespace
{

bool hasStatisticsType(const DB::DataTypePtr & type)
{
    switch (DB::removeNullable(type)->getTypeId())
    {
        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
        case DB::TypeIndex::Date32:
        case DB::TypeIndex::DateTime64:
        case DB::TypeIndex::String:
            return true;
        default:
            return false;
    }
}

#if USE_SIMDJSON

using Element = DB::SimdJSONParser::Element;

std::optional<double> getNumber(const Element & element)
{
    if (element.isDouble())
        return element.getDouble();
    if (element.isInt64())
        return static_cast<double>(element.getInt64());
    if (element.isUInt64())
        return static_cast<double>(element.getUInt64());
    return {};
}

std::optional<DB::Field> parseValue(const Element & element, const DB::IDataType & type, bool is_max)
{
    switch (type.getTypeId())
    {
        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
            if (element.isInt64())
                return DB::Field(element.getInt64());
            return {};
        case DB::TypeIndex::Float32:
            if (auto number = getNumber(element))
                return DB::Field(static_cast<Float64>(static_cast<Float32>(*number)));
            return {};
        case DB::TypeIndex::Float64:
            if (auto number = getNumber(element))
                return DB::Field(*number);
            return {};
        case DB::TypeIndex::Date32:
        {
            if (!element.isString())
                return {};
            auto text = element.getString();
            DB::ReadBufferFromMemory buf(text.data(), text.size());
            ExtendedDayNum day;
            if (!DB::tryReadDateText(day, buf, DateLUT::instance("UTC"), nullptr, false) || !buf.eof())
                return {};
            return DB::Field(static_cast<Int64>(day.toUnderType()));
        }
        case DB::TypeIndex::DateTime64:
        {
            if (!element.isString())
                return {};
            auto text = element.getString();
            DB::ReadBufferFromMemory buf(text.data(), text.size());
            const auto scale = assert_cast<const DB::DataTypeDateTime64 &>(type).getScale();
            const auto & utc = DateLUT::instance("UTC");
            DB::DateTime64 value;
            if (!DB::tryParseDateTime64BestEffort(value, scale, buf, utc, utc) || !buf.eof())
                return {};
            if (is_max && scale > 3)
                value += static_cast<Int64>(intExp10(static_cast<int>(scale) - 3)) - 1;
            return DB::DecimalField<DB::DateTime64>(value, scale);
        }
        case DB::TypeIndex::String:
            if (element.isString())
                return DB::Field(String(element.getString()));
            return {};
        default:
            return {};
    }
}

#endif

}

StatisticsColumns getStatisticsColumns(
    const DB::NamesAndTypesList & table_schema,
    const DB::Names & partition_columns,
    const std::unordered_set<String> & binary_columns,
    const DB::NameToNameMap & physical_names_map)
{
    const std::unordered_set<std::string_view> partition_columns_set(partition_columns.begin(), partition_columns.end());
    StatisticsColumns result;
    for (const auto & column : table_schema)
    {
        if (partition_columns_set.contains(column.name) || binary_columns.contains(column.name) || !hasStatisticsType(column.type))
            continue;
        auto physical_name = physical_names_map.empty()
            ? column.name
            : getPhysicalName(appendToLogicalPath({}, column.name), physical_names_map);
        result.push_back({column, std::move(physical_name)});
    }
    return result;
}

size_t DataFileStatistics::allocatedBytes() const
{
    size_t bytes = columns.capacity() * sizeof(ColumnStatistics);
    for (const auto & column : columns)
    {
        if (column.min && column.min->getType() == DB::Field::Types::String)
            bytes += column.min->safeGet<String>().capacity();
        if (column.max && column.max->getType() == DB::Field::Types::String)
            bytes += column.max->safeGet<String>().capacity();
    }
    return bytes;
}

#if USE_SIMDJSON

struct DataFileStatisticsParser::Impl
{
    DB::SimdJSONParser parser;
};

#else

struct DataFileStatisticsParser::Impl
{
};

#endif

DataFileStatisticsParser::DataFileStatisticsParser(const StatisticsColumns & columns_)
    : columns(columns_)
    , impl(std::make_unique<Impl>())
{
}

DataFileStatisticsParser::~DataFileStatisticsParser() = default;

DataFileStatistics DataFileStatisticsParser::parse(std::string_view json)
{
    DataFileStatistics result;
#if USE_SIMDJSON
    Element root;
    if (!impl->parser.parse(json, root) || !root.isObject())
        return result;
    const auto object = root.getObject();

    Element num_records;
    if (object.find("numRecords", num_records))
    {
        if (num_records.isInt64() && num_records.getInt64() >= 0)
            result.num_records = num_records.getInt64();
        else if (num_records.isUInt64())
            result.num_records = num_records.getUInt64();
    }

    auto find_object = [&](std::string_view key) -> std::optional<DB::SimdJSONParser::Object>
    {
        Element element;
        if (object.find(key, element) && element.isObject())
            return element.getObject();
        return {};
    };
    const auto min_values = find_object("minValues");
    const auto max_values = find_object("maxValues");
    const auto null_counts = find_object("nullCount");
    if (!min_values && !max_values && !null_counts)
        return result;

    result.columns.resize(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & name = columns[i].physical_name;
        const auto type = DB::removeNullable(columns[i].column.type);
        auto & column = result.columns[i];
        Element element;
        if (min_values && min_values->find(name, element))
            column.min = parseValue(element, *type, /* is_max */ false);
        if (max_values && max_values->find(name, element))
            column.max = parseValue(element, *type, /* is_max */ true);
        if (null_counts && null_counts->find(name, element))
        {
            if (element.isInt64() && element.getInt64() >= 0)
                column.null_count = element.getInt64();
            else if (element.isUInt64())
                column.null_count = element.getUInt64();
        }
    }
#else
    (void)json;
#endif
    return result;
}

StatisticsPruner::StatisticsPruner(const DB::ActionsDAG & filter_dag, const StatisticsColumns & columns, DB::ContextPtr context)
{
    std::unordered_set<std::string_view> filter_inputs;
    for (const auto * input : filter_dag.getInputs())
        filter_inputs.insert(input->result_name);

    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & column = columns[i].column;
        if (!filter_inputs.contains(column.name))
            continue;

        auto expression = std::make_shared<DB::ExpressionActions>(DB::ActionsDAG({column}), DB::ExpressionActionsSettings(context));
        DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag.getOutputs().front(), context, /* boolean_context */ true);
        DB::KeyCondition condition(inverted_dag, context, {column.name}, expression);
        DB::DataTypes data_types{column.type};
        if (DB::KeyCondition::typeMayHideNaN(column.type))
            condition.relaxAtomsOverNaNHidingColumns(data_types);
        if (condition.alwaysUnknownOrTrue())
            continue;
        conditions.push_back({i, std::move(data_types), std::move(condition)});
    }
}

bool StatisticsPruner::canBePruned(const DataFileStatistics & statistics) const
{
    for (const auto & [column_index, data_types, condition] : conditions)
    {
        if (column_index >= statistics.columns.size())
            continue;
        const auto & column = statistics.columns[column_index];

        bool may_be_true = true;
        if (column.min && column.max)
        {
            const DB::FieldRef left(*column.min);
            const DB::FieldRef right(*column.max);
            may_be_true = condition.mayBeTrueInRange(1, &left, &right, data_types);
        }
        else if (column.null_count && statistics.num_records && *column.null_count == *statistics.num_records)
        {
            may_be_true = false;
        }

        const bool may_have_nulls = data_types.front()->isNullable() && (!column.null_count || *column.null_count > 0);
        if (!may_be_true && may_have_nulls)
        {
            const DB::FieldRef null_point(DB::POSITIVE_INFINITY);
            may_be_true = condition.mayBeTrueInRange(1, &null_point, &null_point, data_types);
        }

        if (!may_be_true)
            return true;
    }
    return false;
}

}

#endif
