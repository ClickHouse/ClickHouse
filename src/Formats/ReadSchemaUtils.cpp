#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int BAD_ARGUMENTS;
}

ColumnsDescription readSchemaFromFormat(
    const String & format_name,
    const std::optional<FormatSettings> & format_settings,
    ReadBufferCreator read_buffer_creator,
    ContextPtr context,
    std::unique_ptr<ReadBuffer> & buf_out)
{
    NamesAndTypesList names_and_types;
    if (FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format_name))
    {
        auto external_schema_reader = FormatFactory::instance().getExternalSchemaReader(format_name, context, format_settings);
        try
        {
            names_and_types = external_schema_reader->readSchema();
        }
        catch (const DB::Exception & e)
        {
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot extract table structure from {} format file. Error: {}", format_name, e.message());
        }
    }
    else if (FormatFactory::instance().checkIfFormatHasSchemaReader(format_name))
    {
        buf_out = read_buffer_creator();
        if (buf_out->eof())
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot extract table structure from {} format file, file is empty", format_name);

        auto schema_reader = FormatFactory::instance().getSchemaReader(format_name, *buf_out, context, format_settings);
        try
        {
            names_and_types = schema_reader->readSchema();
        }
        catch (const DB::Exception & e)
        {
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot extract table structure from {} format file. Error: {}", format_name, e.message());
        }
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} file format doesn't support schema inference", format_name);

    return ColumnsDescription(names_and_types);
}

ColumnsDescription readSchemaFromFormat(const String & format_name, const std::optional<FormatSettings> & format_settings, ReadBufferCreator read_buffer_creator, ContextPtr context)
{
    std::unique_ptr<ReadBuffer> buf_out;
    return readSchemaFromFormat(format_name, format_settings, read_buffer_creator, context, buf_out);
}

DataTypePtr generalizeDataType(DataTypePtr type)
{
    WhichDataType which(type);

    if (which.isNothing())
        return nullptr;

    if (which.isNullable())
    {
        const auto * nullable_type = assert_cast<const DataTypeNullable *>(type.get());
        return generalizeDataType(nullable_type->getNestedType());
    }

    if (isNumber(type))
        return makeNullable(std::make_shared<DataTypeFloat64>());

    if (which.isArray())
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
        auto nested_type = generalizeDataType(array_type->getNestedType());
        return nested_type ? std::make_shared<DataTypeArray>(nested_type) : nullptr;
    }

    if (which.isTuple())
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        DataTypes nested_types;
        for (const auto & element : tuple_type->getElements())
        {
            auto nested_type = generalizeDataType(element);
            if (!nested_type)
                return nullptr;
            nested_types.push_back(nested_type);
        }
        return std::make_shared<DataTypeTuple>(std::move(nested_types));
    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        auto key_type = removeNullable(generalizeDataType(map_type->getKeyType()));
        auto value_type = generalizeDataType(map_type->getValueType());
        return key_type && value_type ? std::make_shared<DataTypeMap>(key_type, value_type) : nullptr;
    }

    if (which.isLowCarnality())
    {
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(type.get());
        auto nested_type = generalizeDataType(lc_type->getDictionaryType());
        return nested_type ? std::make_shared<DataTypeLowCardinality>(nested_type) : nullptr;
    }

    return makeNullable(type);
}

}
