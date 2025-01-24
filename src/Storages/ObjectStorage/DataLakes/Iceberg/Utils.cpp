
#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#    include <boost/bimap.hpp>


namespace DB::ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
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
    // NameToAvroNode name_to_node;
    NameToDataType name_to_data_type;

    std::unordered_map<String, std::optional<size_t>> initial_index_by_name;
    for (const auto & name : names)
    {
        initial_index_by_name.insert({name, std::nullopt});
    }

    size_t leaves_num = root_node->leaves();
    for (size_t i = 0; i < leaves_num; ++i)
    {
        const auto & name = root_node->leafAt(i)->name();
        if (initial_index_by_name.find(name) != initial_index_by_name.end())
            initial_index_by_name[name] = i;
    }


    size_t current_new_index = 0;
    ColumnsWithTypeAndName columns_to_add = {};
    for (const auto & name : names)
    {
        if (initial_index_by_name.at(name).has_value())
        {
            name_to_index.insert({name, current_new_index++});
            const auto node = root_node->leafAt(initial_index_by_name.at(name).value());
            const size_t initial_index = initial_index_by_name.at(name).value();
            if (node->type() != expected_types.at(initial_index))
            {
                throw Exception(
                    DB::ErrorCodes::ILLEGAL_COLUMN,
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


// Block manifest_file_header = [&]()
// {
//     if (index_by_name.at("sequence_number").has_value())
//     {
//         avro::NodePtr sequence_number_node = root_node->leafAt(index_by_name.at("sequence_number").value());
//         auto sequence_number_col_data_type = AvroSchemaReader::avroNodeToDataType(sequence_number_node);
//         return Block{
//             {status_col_data_type->createColumn(), status_col_data_type, "status"},
//             {data_col_data_type->createColumn(), data_col_data_type, "data_file"},
//             {sequence_number_col_data_type->createColumn(), sequence_number_col_data_type, "sequence_number"}};
//     }
//     else
//     {
//         return Block{
//             {status_col_data_type->createColumn(), status_col_data_type, "status"},
//             {data_col_data_type->createColumn(), data_col_data_type, "data_file"}};
//     }
// }();


// avro::NodePtr status_node = root_node->leafAt(index_by_name.at("status").value());
// if (status_node->type() != avro::Type::AVRO_INT)
// {
//     throw Exception(
//         DB::ErrorCodes::ILLEGAL_COLUMN,
//         "The parsed column from Avro file of `status` field should be Int type, got {}",
//         magic_enum::enum_name(status_node->type()));
// }

// avro::NodePtr data_file_node = root_node->leafAt(index_by_name.at("data_file").value());
// if (data_file_node->type() != avro::Type::AVRO_RECORD)
// {
//     throw Exception(
//         DB::ErrorCodes::ILLEGAL_COLUMN,
//         "The parsed column from Avro file of `data_file` field should be Tuple type, got {}",
//         magic_enum::enum_name(data_file_node->type()));
// }

// auto status_col_data_type = AvroSchemaReader::avroNodeToDataType(status_node);
// auto data_col_data_type = AvroSchemaReader::avroNodeToDataType(data_file_node);

// Block manifest_file_header = [&]()
// {
//     if (index_by_name.at("sequence_number").has_value())
//     {
//         avro::NodePtr sequence_number_node = root_node->leafAt(index_by_name.at("sequence_number").value());
//         auto sequence_number_col_data_type = AvroSchemaReader::avroNodeToDataType(sequence_number_node);
//         return Block{
//             {status_col_data_type->createColumn(), status_col_data_type, "status"},
//             {data_col_data_type->createColumn(), data_col_data_type, "data_file"},
//             {sequence_number_col_data_type->createColumn(), sequence_number_col_data_type, "sequence_number"}};
//     }
//     else
//     {
//         return Block{
//             {status_col_data_type->createColumn(), status_col_data_type, "status"},
//             {data_col_data_type->createColumn(), data_col_data_type, "data_file"}};
//     }
// }();

// auto columns = parseAvro(*manifest_file_reader_, manifest_file_header, format_settings);
// }
// }

// std::tuple<std::, DB::MutableColumns> getColumnsAndTypesFromAvroByNames(
//     avro::NodePtr root_node, const std::unordered_map<String, avro::Type> & expected_names_and_types, const DB::FormatSettings & settings)
// {
//     std::unordered_map<String, std::optional<size_t>> index_by_name;
//     for (const auto & [name, type] : expected_names_and_types)
//     {
//         index_by_name.emplace(name, std::nullopt);
//     }

//     size_t leaves_num = root_node->leaves();
//     for (size_t i = 0; i < leaves_num; ++i)
//     {
//         const auto & name = root_node->leafAt(i)->name();
//         if (index_by_name.find(name) != index_by_name.end())
//             index_by_name[name] = i;
//     }

//     for (const auto & [name, type] : expected_names_and_types)
//     {
//     }

//     avro::NodePtr status_node = root_node->leafAt(index_by_name.at("status").value());
//     if (status_node->type() != avro::Type::AVRO_INT)
//     {
//         throw Exception(
//             DB::ErrorCodes::ILLEGAL_COLUMN,
//             "The parsed column from Avro file of `status` field should be Int type, got {}",
//             magic_enum::enum_name(status_node->type()));
//     }

//     avro::NodePtr data_file_node = root_node->leafAt(index_by_name.at("data_file").value());
//     if (data_file_node->type() != avro::Type::AVRO_RECORD)
//     {
//         throw Exception(
//             DB::ErrorCodes::ILLEGAL_COLUMN,
//             "The parsed column from Avro file of `data_file` field should be Tuple type, got {}",
//             magic_enum::enum_name(data_file_node->type()));
//     }

//     auto status_col_data_type = AvroSchemaReader::avroNodeToDataType(status_node);
//     auto data_col_data_type = AvroSchemaReader::avroNodeToDataType(data_file_node);

//     Block manifest_file_header = [&]()
//     {
//         if (index_by_name.at("sequence_number").has_value())
//         {
//             avro::NodePtr sequence_number_node = root_node->leafAt(index_by_name.at("sequence_number").value());
//             auto sequence_number_col_data_type = AvroSchemaReader::avroNodeToDataType(sequence_number_node);
//             return Block{
//                 {status_col_data_type->createColumn(), status_col_data_type, "status"},
//                 {data_col_data_type->createColumn(), data_col_data_type, "data_file"},
//                 {sequence_number_col_data_type->createColumn(), sequence_number_col_data_type, "sequence_number"}};
//         }
//         else
//         {
//             return Block{
//                 {status_col_data_type->createColumn(), status_col_data_type, "status"},
//                 {data_col_data_type->createColumn(), data_col_data_type, "data_file"}};
//         }
//     }();

//     auto columns = parseAvro(*manifest_file_reader_, manifest_file_header, format_settings);
// }
}


#endif
