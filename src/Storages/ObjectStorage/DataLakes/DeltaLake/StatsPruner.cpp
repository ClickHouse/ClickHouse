#include <Storages/ObjectStorage/DataLakes/DeltaLake/StatsPruner.h>

#if USE_DELTA_KERNEL_RS

#include <Common/DateLUT.h>
#include <Common/FieldAccurateComparison.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DeltaLake
{

namespace
{

bool isPrunableType(const DB::DataTypePtr & type)
{
    const DB::WhichDataType which(DB::removeNullable(type));
    return which.isNativeInteger() || which.isFloat() || which.isDate() || which.isDate32() || which.isDateTime64()
        || which.isString();
}

boost::intrusive_ptr<DB::IAST> createKeyAST(const DB::Names & columns)
{
    auto key_ast = DB::make_intrusive<DB::ASTFunction>();
    key_ast->name = "tuple";
    key_ast->arguments = DB::make_intrusive<DB::ASTExpressionList>();
    key_ast->children.push_back(key_ast->arguments);
    for (const auto & column : columns)
        key_ast->arguments->children.emplace_back(DB::make_intrusive<DB::ASTIdentifier>(column));
    return key_ast;
}

std::optional<DB::Field> parseStatValue(const Poco::Dynamic::Var & value, const DB::DataTypePtr & type, bool is_max)
{
    if (value.isEmpty())
        return std::nullopt;

    const DB::WhichDataType which(type);
    if (which.isString())
    {
        if (!value.isString())
            return std::nullopt;
        return DB::Field(value.extract<std::string>());
    }

    if (which.isDateTime64())
    {
        if (!value.isString())
            return std::nullopt;
        const auto & datetime_type = assert_cast<const DB::DataTypeDateTime64 &>(*type);
        const auto text = value.extract<std::string>();
        DB::ReadBufferFromString buf(text);
        DB::DateTime64 result;
        if (!DB::tryParseDateTime64BestEffort(
                result, datetime_type.getScale(), buf, datetime_type.getTimeZone(), DateLUT::instance("UTC"))
            || !buf.eof())
            return std::nullopt;
        if (is_max && datetime_type.getScale() > 3)
            result.value += DB::DecimalUtils::scaleMultiplier<DB::DateTime64::NativeType>(datetime_type.getScale() - 3) - 1;
        return DB::Field(DB::DecimalField<DB::DateTime64>(result, datetime_type.getScale()));
    }

    if (which.isDate() || which.isDate32())
    {
        if (!value.isString())
            return std::nullopt;
        const auto text = value.extract<std::string>();
        DB::ReadBufferFromString buf(text);
        auto column = type->createColumn();
        if (!type->getDefaultSerialization()->tryDeserializeWholeText(*column, buf, DB::FormatSettings{}))
            return std::nullopt;
        return (*column)[0];
    }

    if (!value.isNumeric())
        return std::nullopt;

    DB::Field field;
    if (value.isInteger())
        field = value.isSigned() ? DB::Field(value.convert<Int64>()) : DB::Field(value.convert<UInt64>());
    else
        field = DB::Field(value.convert<Float64>());

    auto converted = DB::convertFieldToType(field, *type);
    if (converted.isNull())
        return std::nullopt;
    return converted;
}

std::optional<Int64> getInteger(const Poco::JSON::Object::Ptr & object, const std::string & name)
{
    if (!object || !object->has(name))
        return std::nullopt;
    const auto value = object->get(name);
    if (!value.isInteger())
        return std::nullopt;
    return value.convert<Int64>();
}

Poco::Dynamic::Var getValue(const Poco::JSON::Object::Ptr & object, const std::string & name)
{
    if (!object || !object->has(name))
        return {};
    return object->get(name);
}

}

StatsPruner::StatsPruner(
    const DB::ActionsDAG & filter_dag,
    const DB::NamesAndTypesList & table_schema,
    const DB::Names & partition_columns,
    const DB::NameToNameMap & physical_names_map,
    DB::ContextPtr context)
{
    const auto required_columns = filter_dag.getRequiredColumnsNames();
    const std::unordered_set<std::string> partition_column_set(partition_columns.begin(), partition_columns.end());

    DB::Names key_columns;
    DB::NamesAndTypesList key_names_and_types;
    for (const auto & column : table_schema)
    {
        if (partition_column_set.contains(column.name) || !isPrunableType(column.type)
            || std::find(required_columns.begin(), required_columns.end(), column.name) == required_columns.end())
            continue;

        key_columns.push_back(column.name);
        key_names_and_types.emplace_back(column.name, DB::makeNullable(DB::removeNullable(column.type)));
        stat_types.push_back(DB::removeNullable(column.type));
        physical_names.push_back(
            physical_names_map.empty() ? column.name : getPhysicalName(appendToLogicalPath({}, column.name), physical_names_map));
    }

    if (key_columns.empty())
        return;

    key = DB::KeyDescription::getKeyFromAST(createKeyAST(key_columns), DB::ColumnsDescription(key_names_and_types), {}, context);

    DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag.getOutputs().front(), context, /* boolean_context */ true);
    key_condition.emplace(inverted_dag, context, key.column_names, key.expression);
    key_condition->relaxAtomsOverNaNHidingColumns(key.data_types);
    if (key_condition->alwaysUnknownOrTrue())
        key_condition.reset();
}

bool StatsPruner::canBePruned(std::string_view stats_json) const
{
    if (!key_condition)
        return false;

    Poco::JSON::Parser parser;
    const auto stats = parser.parse(std::string(stats_json)).extract<Poco::JSON::Object::Ptr>();
    if (!stats)
        return false;

    const auto num_records = getInteger(stats, "numRecords");
    const auto min_values = stats->getObject("minValues");
    const auto max_values = stats->getObject("maxValues");
    const auto null_counts = stats->getObject("nullCount");

    DB::Hyperrectangle hyperrectangle;
    hyperrectangle.reserve(physical_names.size());
    for (size_t i = 0; i < physical_names.size(); ++i)
    {
        const auto & name = physical_names[i];
        const auto null_count = getInteger(null_counts, name);
        if (null_count && num_records && *num_records > 0 && *null_count == *num_records)
        {
            hyperrectangle.emplace_back(DB::POSITIVE_INFINITY);
            continue;
        }

        const auto min = parseStatValue(getValue(min_values, name), stat_types[i], /* is_max */ false);
        const auto max = parseStatValue(getValue(max_values, name), stat_types[i], /* is_max */ true);
        if (!min || !max || DB::accurateLess(*max, *min))
        {
            hyperrectangle.emplace_back(DB::Range::createWholeUniverse());
            continue;
        }

        if (!null_count || *null_count > 0)
            hyperrectangle.emplace_back(*min, true, DB::POSITIVE_INFINITY, true);
        else
            hyperrectangle.emplace_back(*min, true, *max, true);
    }

    return !key_condition->checkInHyperrectangle(hyperrectangle, key.data_types).can_be_true;
}

}

#endif
