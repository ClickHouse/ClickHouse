#include <optional>
#include <type_traits>
#include <Processors/Formats/Impl/PrometheusTextOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <base/defines.h>

namespace DB
{

constexpr auto FORMAT_NAME = "Prometheus";

static bool isDataTypeString(const DataTypePtr & type)
{
    return WhichDataType(type).isStringOrFixedString();
}

template <typename ResType, typename Pred>
static void getColumnPos(const Block & header, const String & col_name, Pred pred, ResType & res)
{
    static_assert(std::is_same_v<ResType, size_t> || std::is_same_v<ResType, std::optional<size_t>>, "Illegal ResType");

    constexpr bool is_optional = std::is_same_v<ResType, std::optional<size_t>>;

    if (header.has(col_name))
    {
        res = header.getPositionByName(col_name);
        const auto & col = header.getByName(col_name);
        if (!pred(is_optional ? removeNullable(col.type) : col.type))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type '{}' of column '{}' for output format '{}'",
                col.type->getName(), col_name, FORMAT_NAME);
        }
    }
    else
    {
        if constexpr (is_optional)
            res = std::nullopt;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' is required for output format '{}'", col_name, FORMAT_NAME);
    }
}

PrometheusTextOutputFormat::PrometheusTextOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    const RowOutputFormatParams & params_,
    const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, params_), format_settings(format_settings_)
{
    const Block & header = getPort(PortKind::Main).getHeader();

    getColumnPos(header, "name", isDataTypeString, pos.name);
    getColumnPos(header, "value", isNumber<DataTypePtr>, pos.value);

    getColumnPos(header, "help", isDataTypeString,pos.help);
    getColumnPos(header, "type", isDataTypeString, pos.type);
}

void PrometheusTextOutputFormat::write(const Columns & columns, size_t row_num)
{
    if (pos.help.has_value() && !columns[*pos.help]->isNullAt(row_num))
    {
        writeCString("# HELP ", out);
        serializations[pos.name]->serializeText(*columns[pos.name], row_num, out, format_settings);
        writeChar(' ', out);
        serializations[*pos.help]->serializeText(*columns[*pos.help], row_num, out, format_settings);
        writeChar('\n', out);
    }

    if (pos.type.has_value() && !columns[*pos.type]->isNullAt(row_num))
    {
        writeCString("# TYPE ", out);
        serializations[pos.name]->serializeText(*columns[pos.name], row_num, out, format_settings);
        writeChar(' ', out);
        serializations[*pos.type]->serializeText(*columns[*pos.type], row_num, out, format_settings);
        /// TODO(vdimir): Check if type is 'counter', 'gauge', 'histogram', 'summary', or 'untyped'
        writeChar('\n', out);
    }

    serializations[pos.name]->serializeText(*columns[pos.name], row_num, out, format_settings);
    writeChar(' ', out);
    serializations[pos.value]->serializeText(*columns[pos.value], row_num, out, format_settings);

    writeChar('\n', out);
    writeChar('\n', out);
}

void registerOutputFormatPrometheus(FormatFactory & factory)
{
    factory.registerOutputFormat(FORMAT_NAME, [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<PrometheusTextOutputFormat>(buf, sample, params, settings);
    });
}

}
