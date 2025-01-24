#pragma once

#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>

#    include <boost/bimap.hpp>


namespace Iceberg
{

using NameToIndex = boost::bimap<String, size_t>;
using NameToAvroNode = std::unordered_map<String, avro::NodePtr>;
using NameToDataType = std::unordered_map<String, DB::DataTypePtr>;

DB::MutableColumns parseAvro(avro::DataFileReaderBase & file_reader, const DB::Block & header, const DB::FormatSettings & settings);

std::tuple<NameToIndex, NameToDataType, DB::Block> getColumnsAndTypesFromAvroByNames(
    avro::NodePtr root_node, const std::vector<String> & names, const std::vector<avro::Type> & expected_types);
}

#endif
