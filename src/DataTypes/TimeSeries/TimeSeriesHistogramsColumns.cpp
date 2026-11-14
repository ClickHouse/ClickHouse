#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>

#include <Columns/IColumn.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/TimeSeries/TimeSeriesColumnNames.h>
#include <base/Decimal.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>


namespace DB
{

const std::vector<TimeSeriesHistogramsCodecDeclaration> & TimeSeriesHistogramsColumns::getCodecDeclarations()
{
    static const std::vector<TimeSeriesHistogramsCodecDeclaration> declarations = []
    {
        std::vector<TimeSeriesHistogramsCodecDeclaration> result;
        result.push_back({TimeSeriesColumnNames::Timestamp, getTimestampCodec()});
        for (const auto column : getAll())
        {
            if (isPairs(column))
            {
                result.push_back({fmt::format("{}.{}", getName(column), getPairIndexName()), getPairIndexCodec()});
                result.push_back({fmt::format("{}.{}", getName(column), getPairCountName()), getPairCountCodec(column)});
            }
            else
            {
                chassert(!getDefinition(column).codec.empty());
                result.push_back({String{getName(column)}, getDefinition(column).codec});
            }
        }
        return result;
    }();
    return declarations;
}

const DataTypePtr & TimeSeriesHistogramsColumns::getDataType(TimeSeriesHistogramsColumn column)
{
    static const auto data_types = []
    {
        std::array<DataTypePtr, getAll().size()> result;
        for (auto payload_column : getAll())
            result[static_cast<size_t>(payload_column)] = DataTypeFactory::instance().get(String{getType(payload_column)});
        return result;
    }();
    return data_types[static_cast<size_t>(column)];
}

const IColumn & TimeSeriesHistogramsColumns::getEmptyColumn(TimeSeriesHistogramsColumn column)
{
    static const auto columns = []
    {
        std::array<ColumnPtr, getAll().size()> result;
        for (auto payload_column : getAll())
            result[static_cast<size_t>(payload_column)] = getDataType(payload_column)->createColumn();
        return result;
    }();
    return *columns[static_cast<size_t>(column)];
}

DataTypePtr TimeSeriesHistogramsColumns::makeHistogramsArrayType(const DataTypePtr & timestamp_type)
{
    DataTypes element_types{timestamp_type};
    Strings element_names{TimeSeriesColumnNames::Timestamp};
    for (auto column : getAll())
    {
        element_types.push_back(getDataType(column));
        element_names.emplace_back(getName(column));
    }
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(std::move(element_types), std::move(element_names)));
}

DataTypePtr TimeSeriesHistogramsColumns::tryGetTimestampType(const DataTypePtr & type)
{
    /// A type wrapped in `SimpleAggregateFunction` is the nested type with a custom name, so no unwrapping is needed.
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
    if (!tuple_type || (tuple_type->getElements().size() != getAll().size() + 1))
        return nullptr;

    const auto & timestamp_type = tuple_type->getElements()[getTimestampPosition()];
    if (!makeHistogramsArrayType(timestamp_type)->equals(*std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(tuple_type->getElements(), tuple_type->getElementNames()))))
        return nullptr;
    return timestamp_type;
}

String TimeSeriesHistogramsColumns::getOuterColumnName(std::string_view element_name)
{
    return fmt::format("{}.{}", TimeSeriesColumnNames::Histograms, element_name);
}

std::string_view TimeSeriesHistogramsColumns::getInnerColumnName(std::string_view outer_column_name)
{
    const std::string_view prefix = TimeSeriesColumnNames::Histograms;
    if ((outer_column_name.size() <= prefix.size() + 1) || !outer_column_name.starts_with(prefix) || (outer_column_name[prefix.size()] != '.'))
        return {};
    return outer_column_name.substr(prefix.size() + 1);
}

NameAndTypePair TimeSeriesHistogramsColumns::getOuterTimestampColumn(const DataTypePtr & timestamp_type)
{
    return {getOuterColumnName(TimeSeriesColumnNames::Timestamp), std::make_shared<DataTypeArray>(timestamp_type)};
}

const NamesAndTypesList & TimeSeriesHistogramsColumns::getOuterPayloadColumns()
{
    static const NamesAndTypesList columns = []
    {
        NamesAndTypesList result;
        for (auto column : getAll())
            result.emplace_back(getOuterColumnName(getName(column)), std::make_shared<DataTypeArray>(getDataType(column)));
        return result;
    }();
    return columns;
}

namespace
{
    /// The list of the codecs without the keyword: `CODEC(Delta, T64, ZSTD(3))` -> `Delta, T64, ZSTD(3)`.
    std::string_view getCodecList(std::string_view codec)
    {
        static constexpr std::string_view prefix = "CODEC(";
        chassert(codec.starts_with(prefix) && codec.ends_with(')'));
        return codec.substr(prefix.size(), codec.size() - prefix.size() - 1);
    }

    /// The list of the codecs of a declaration as an inner table prints it: `Delta` with the byte size of the values of the subcolumn,
    /// e.g. `Delta(8), T64, ZSTD(3)` for `DateTime64(3)` timestamps. Only the timestamps and the bucket indexes have `Delta`.
    String getCodecListForInnerTable(const TimeSeriesHistogramsCodecDeclaration & declaration)
    {
        static constexpr std::string_view delta = "Delta";
        const auto codec_list = getCodecList(declaration.codec);
        if (!codec_list.starts_with(delta))
            return String{codec_list};
        const size_t byte_size = (declaration.subcolumn == TimeSeriesColumnNames::Timestamp) ? sizeof(DateTime64) : sizeof(Int32);
        return fmt::format("{}({}){}", delta, byte_size, codec_list.substr(delta.size()));
    }
}

String TimeSeriesHistogramsColumns::describeArrayTypeForDocs(std::string_view timestamp_type_text)
{
    String elements = fmt::format("{} {}", TimeSeriesColumnNames::Timestamp, timestamp_type_text);
    for (const auto column : getAll())
        elements += fmt::format(", {} {}", getName(column), getType(column));
    return fmt::format("Array(Tuple({}))", elements);
}

String TimeSeriesHistogramsColumns::describeOuterColumnsForDocs(std::string_view timestamp_type_text, std::string_view timestamp_description)
{
    String result = fmt::format(
        "| `{}` | `Array({})` by default | {} |\n", getOuterColumnName(TimeSeriesColumnNames::Timestamp), timestamp_type_text, timestamp_description);
    for (const auto column : getAll())
        result += fmt::format("| `{}` | `Array({})` | {} |\n", getOuterColumnName(getName(column)), getType(column), getDescription(column));
    return result;
}

String TimeSeriesHistogramsColumns::describeOuterColumnsForCreateExample(std::string_view timestamp_type_text)
{
    String result = fmt::format("    `{}` Array({}),\n", getOuterColumnName(TimeSeriesColumnNames::Timestamp), timestamp_type_text);
    for (const auto column : getAll())
        result += fmt::format("    `{}` Array({}),\n", getOuterColumnName(getName(column)), getType(column));
    return result;
}

String TimeSeriesHistogramsColumns::describeInnerCodecsForDocs()
{
    /// The groups are in the order of the first declaration with the codec, so the timestamps come first.
    std::vector<std::pair<std::string_view, std::vector<std::string_view>>> groups;
    for (const auto & declaration : getCodecDeclarations())
    {
        const auto it = std::ranges::find_if(groups, [&](const auto & group) { return group.first == declaration.codec; });
        if (it == groups.end())
            groups.emplace_back(declaration.codec, std::vector<std::string_view>{declaration.subcolumn});
        else
            it->second.push_back(declaration.subcolumn);
    }

    String result;
    for (const auto & [codec, subcolumns] : groups)
    {
        if (!result.empty())
            result += "; ";
        result += fmt::format("`{}` for `{}`", getCodecList(codec), fmt::join(subcolumns, "`, `"));
    }
    return result;
}

String TimeSeriesHistogramsColumns::describeInnerCodecsForCreateExample(bool for_inner_table)
{
    String result;
    for (const auto & declaration : getCodecDeclarations())
    {
        if (!result.empty())
            result += ",\n";
        result += fmt::format("    CODEC `{}` ({})",
            getOuterColumnName(declaration.subcolumn),
            for_inner_table ? getCodecListForInnerTable(declaration) : String{getCodecList(declaration.codec)});
    }
    return result;
}

}
