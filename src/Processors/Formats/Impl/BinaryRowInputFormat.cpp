#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/BinaryRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SKIP_UNKNOWN_FIELD;
}

template <bool with_defaults>
BinaryRowInputFormat<with_defaults>::BinaryRowInputFormat(ReadBuffer & in_, const Block & header, IRowInputFormat::Params params_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes<BinaryFormatReader<with_defaults>>(
        header,
        in_,
        params_,
        true,
        with_names_,
        with_types_,
        format_settings_,
        std::make_unique<BinaryFormatReader<with_defaults>>(in_, format_settings_))
{
}

template <bool with_defaults>
BinaryFormatReader<with_defaults>::BinaryFormatReader(ReadBuffer & in_, const FormatSettings & format_settings_) : FormatWithNamesAndTypesReader(in_, format_settings_)
{
}

template <bool with_defaults>
std::vector<String> BinaryFormatReader<with_defaults>::readHeaderRow()
{
    std::vector<String> fields;
    String field;
    for (size_t i = 0; i < read_columns; ++i)
    {
        readStringBinary(field, *in);
        fields.push_back(field);
    }
    return fields;
}

template <bool with_defaults>
std::vector<String> BinaryFormatReader<with_defaults>::readNames()
{
    readVarUInt(read_columns, *in);
    return readHeaderRow();
}

template <bool with_defaults>
std::vector<String> BinaryFormatReader<with_defaults>::readTypes()
{
    read_data_types.reserve(read_columns);
    Names type_names;
    if (format_settings.binary.decode_types_in_binary_format)
    {
        type_names.reserve(read_columns);
        for (size_t i = 0; i < read_columns; ++i)
        {
            read_data_types.push_back(decodeDataType(*in));
            type_names.push_back(read_data_types.back()->getName());
        }
    }
    else
    {
        type_names = readHeaderRow();
        for (const auto & type_name : type_names)
            read_data_types.push_back(DataTypeFactory::instance().get(type_name));
    }

    return type_names;
}

template <bool with_defaults>
bool BinaryFormatReader<with_defaults>::readField(IColumn & column, const DataTypePtr & /*type*/, const SerializationPtr & serialization, bool /*is_last_file_column*/, const String & /*column_name*/)
{
    if constexpr (with_defaults)
    {
        UInt8 is_default;
        readBinary(is_default, *in);
        if (is_default)
        {
            column.insertDefault();
            return false;
        }
    }
    serialization->deserializeBinary(column, *in, format_settings);
    return true;
}

template <bool with_defaults>
void BinaryFormatReader<with_defaults>::skipHeaderRow()
{
    String tmp;
    for (size_t i = 0; i < read_columns; ++i)
        readStringBinary(tmp, *in);
}

template <bool with_defaults>
void BinaryFormatReader<with_defaults>::skipNames()
{
    readVarUInt(read_columns, *in);
    skipHeaderRow();
}

template <bool with_defaults>
void BinaryFormatReader<with_defaults>::skipTypes()
{
    if (read_columns == 0)
    {
        /// It's possible only when with_names = false and with_types = true
        readVarUInt(read_columns, *in);
    }

    skipHeaderRow();
}

template <bool with_defaults>
void BinaryFormatReader<with_defaults>::skipField(size_t file_column)
{
    if (file_column >= read_data_types.size())
        throw Exception(ErrorCodes::CANNOT_SKIP_UNKNOWN_FIELD,
                        "Cannot skip unknown field in RowBinaryWithNames format, because it's type is unknown");
    Field field;
    read_data_types[file_column]->getDefaultSerialization()->deserializeBinary(field, *in, format_settings);
}

BinaryWithNamesAndTypesSchemaReader::BinaryWithNamesAndTypesSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : FormatWithNamesAndTypesSchemaReader(in_, format_settings_, true, true, &reader), reader(in_, format_settings_)
{
}

void registerInputFormatRowBinary(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerInputFormat(format_name, [with_names, with_types](
            ReadBuffer & buf,
            const Block & sample,
            const IRowInputFormat::Params & params,
            const FormatSettings & settings)
        {
            return std::make_shared<BinaryRowInputFormat<false>>(buf, sample, params, with_names, with_types, settings);
        });
    };

    registerWithNamesAndTypes("RowBinary", register_func);
    factory.registerFileExtension("bin", "RowBinary");

    factory.registerInputFormat("RowBinaryWithDefaults", [](
         ReadBuffer & buf,
         const Block & sample,
         const IRowInputFormat::Params & params,
         const FormatSettings & settings)
    {
        return std::make_shared<BinaryRowInputFormat<true>>(buf, sample, params, false, false, settings);
    });
}

void registerRowBinaryWithNamesAndTypesSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("RowBinaryWithNamesAndTypes", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_shared<BinaryWithNamesAndTypesSchemaReader>(buf, settings);
    });


}


}
