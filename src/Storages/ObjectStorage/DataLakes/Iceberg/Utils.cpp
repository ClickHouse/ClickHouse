
#include <typeinfo>
#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#    include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
extern const int BAD_TYPE_OF_FIELD;
}

namespace Iceberg
{

using namespace DB;


MutableColumns parseAvro(avro::DataFileReaderBase & file_reader, const Block & header, const FormatSettings & settings)
{
    auto deserializer = std::make_unique<DB::AvroDeserializer>(header, file_reader.dataSchema(), true, true, settings);
    MutableColumns columns = header.cloneEmptyColumns();

    file_reader.init();
    RowReadExtension ext;
    while (file_reader.hasMore())
    {
        file_reader.decr();
        deserializer->deserializeRow(columns, file_reader.decoder(), ext);
    }

    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[0]->size() != columns[i]->size())
        {
            throw Exception(DB::ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "All columns should have the same size");
        }
    }
    return columns;
}

std::tuple<NameToIndex, NameToDataType, DB::Block> getColumnsAndTypesFromAvroByNames(
    avro::NodePtr root_node, const std::vector<String> & names, const std::vector<avro::Type> & expected_types)
{
    NameToIndex name_to_index;
    NameToDataType name_to_data_type;

    std::unordered_map<String, std::optional<size_t>> initial_index_by_name;
    for (const auto & name : names)
    {
        initial_index_by_name.insert({name, std::nullopt});
    }

    size_t leaves_num = root_node->leaves();
    for (size_t i = 0; i < leaves_num; ++i)
    {
        const auto & name = root_node->nameAt(static_cast<int>(i));

        if (initial_index_by_name.find(name) != initial_index_by_name.end())
            initial_index_by_name[name] = i;
    }


    size_t current_new_index = 0;
    ColumnsWithTypeAndName columns_to_add = {};
    for (size_t i = 0; i < names.size(); ++i)
    {
        const auto & name = names[i];
        if (initial_index_by_name.at(name).has_value())
        {
            name_to_index.insert({name, current_new_index++});
            const auto node = root_node->leafAt(static_cast<int>(initial_index_by_name.at(name).value()));
            const size_t initial_index = initial_index_by_name.at(name).value();
            if (node->type() != expected_types.at(i))
            {
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "The parsed column from Avro file of `{}` field should be {} type, got {}",
                    name,
                    magic_enum::enum_name(expected_types[initial_index]),
                    magic_enum::enum_name(node->type()));
            }
            name_to_data_type.insert({name, AvroSchemaReader::avroNodeToDataType(node)});
            columns_to_add.push_back(ColumnWithTypeAndName{name_to_data_type.at(name)->createColumn(), name_to_data_type.at(name), name});
        }
    }

    return std::make_tuple(name_to_index, name_to_data_type, Block{columns_to_add});
}

void checkColumnType(const DB::ColumnPtr & column, DB::TypeIndex expected_type_index)
{
    if (column->getDataType() != expected_type_index)
        throw Exception(
            ErrorCodes::BAD_TYPE_OF_FIELD,
            "The parsed column from Avro file should be {} type, got {}",
            magic_enum::enum_name(expected_type_index),
            column->getFamilyName());
}
}


#endif
