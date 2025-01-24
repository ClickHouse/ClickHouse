#pragma once

#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>

namespace Iceberg
{

using NameToIndex = std::unordered_map<String, size_t>;
using NameToDataType = std::unordered_map<String, DB::DataTypePtr>;

DB::MutableColumns parseAvro(avro::DataFileReaderBase & file_reader, const DB::Block & header, const DB::FormatSettings & settings);

std::tuple<NameToIndex, NameToDataType, DB::Block> getColumnsAndTypesFromAvroByNames(
    avro::NodePtr root_node, const std::vector<String> & names, const std::vector<avro::Type> & expected_types);
}

void checkColumnType(const DB::ColumnPtr & column, DB::TypeIndex expected_type_index);

#endif
