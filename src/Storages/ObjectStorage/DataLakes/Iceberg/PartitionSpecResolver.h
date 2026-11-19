#pragma once

#include "config.h"

#if USE_AVRO

#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

#include <Poco/JSON/Object.h>

#include <map>

namespace DB::Iceberg
{

class PartitionSpecResolver
{
public:
    struct ResolvedPartitionSpec
    {
        Poco::JSON::Object::Ptr spec;
        std::vector<String> partition_columns;
        DataTypes partition_types;
    };

    PartitionSpecResolver(Poco::JSON::Object::Ptr metadata_object_, IcebergSchemaProcessorPtr schema_processor_, ContextPtr context_);

    const ResolvedPartitionSpec & resolve(Int32 spec_id, Int32 files_schema_id);

private:
    Poco::JSON::Object::Ptr metadata_object;
    IcebergSchemaProcessorPtr schema_processor;
    ContextPtr context;
    std::map<std::pair<Int32, Int32>, ResolvedPartitionSpec> resolved_specs;
};

}

#endif
