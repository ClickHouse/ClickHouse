#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/ReadSchemaUtils.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Common/assert_cast.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int BAD_ARGUMENTS;
}

static std::optional<NamesAndTypesList> getOrderedColumnsList(
    const NamesAndTypesList & columns_list, const Names & columns_order_hint)
{
    if (columns_list.size() != columns_order_hint.size())
        return {};

    std::unordered_map<String, DataTypePtr> available_columns;
    for (const auto & [name, type] : columns_list)
        available_columns.emplace(name, type);

    NamesAndTypesList res;
    for (const auto & name : columns_order_hint)
    {
        auto it = available_columns.find(name);
        if (it == available_columns.end())
            return {};

        res.emplace_back(name, it->second);
    }
    return res;
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

        /// If we have "INSERT SELECT" query then try to order
        /// columns as they are ordered in table schema for formats
        /// without strict column order (like JSON and TSKV).
        /// It will allow to execute simple data loading with query
        /// "INSERT INTO table SELECT * FROM ..."
        const auto & insertion_table = context->getInsertionTable();
        if (!schema_reader->hasStrictOrderOfColumns() && !insertion_table.empty())
        {
            auto storage = DatabaseCatalog::instance().getTable(insertion_table, context);
            auto metadata = storage->getInMemoryMetadataPtr();
            auto names_in_storage = metadata->getColumns().getNamesOfPhysical();
            auto ordered_list = getOrderedColumnsList(names_and_types, names_in_storage);
            if (ordered_list)
                names_and_types = *ordered_list;
        }
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} file format doesn't support schema inference. You must specify the structure manually", format_name);

    return ColumnsDescription(names_and_types);
}

ColumnsDescription readSchemaFromFormat(const String & format_name, const std::optional<FormatSettings> & format_settings, ReadBufferCreator read_buffer_creator, ContextPtr context)
{
    std::unique_ptr<ReadBuffer> buf_out;
    return readSchemaFromFormat(format_name, format_settings, read_buffer_creator, context, buf_out);
}

DataTypePtr makeNullableRecursivelyAndCheckForNothing(DataTypePtr type)
{
    if (!type)
        return nullptr;

    WhichDataType which(type);

    if (which.isNothing())
        return nullptr;

    if (which.isNullable())
    {
        const auto * nullable_type = assert_cast<const DataTypeNullable *>(type.get());
        return makeNullableRecursivelyAndCheckForNothing(nullable_type->getNestedType());
    }

    if (which.isArray())
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
        auto nested_type = makeNullableRecursivelyAndCheckForNothing(array_type->getNestedType());
        return nested_type ? std::make_shared<DataTypeArray>(nested_type) : nullptr;
    }

    if (which.isTuple())
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        DataTypes nested_types;
        for (const auto & element : tuple_type->getElements())
        {
            auto nested_type = makeNullableRecursivelyAndCheckForNothing(element);
            if (!nested_type)
                return nullptr;
            nested_types.push_back(nested_type);
        }
        return std::make_shared<DataTypeTuple>(std::move(nested_types));
    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        auto key_type = makeNullableRecursivelyAndCheckForNothing(map_type->getKeyType());
        auto value_type = makeNullableRecursivelyAndCheckForNothing(map_type->getValueType());
        return key_type && value_type ? std::make_shared<DataTypeMap>(removeNullable(key_type), value_type) : nullptr;
    }

    if (which.isLowCarnality())
    {
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(type.get());
        auto nested_type = makeNullableRecursivelyAndCheckForNothing(lc_type->getDictionaryType());
        return nested_type ? std::make_shared<DataTypeLowCardinality>(nested_type) : nullptr;
    }

    return makeNullable(type);
}

NamesAndTypesList getNamesAndRecursivelyNullableTypes(const Block & header)
{
    NamesAndTypesList result;
    for (auto & [name, type] : header.getNamesAndTypesList())
        result.emplace_back(name, makeNullableRecursivelyAndCheckForNothing(type));
    return result;
}

}
