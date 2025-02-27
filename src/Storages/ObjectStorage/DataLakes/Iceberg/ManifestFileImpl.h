#pragma once

#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h>

#include "SchemaProcessor.h"

namespace Iceberg
{

/**
 * Manifest file has the following format: '/iceberg_data/db/table_name/metadata/c87bfec7-d36c-4075-ad04-600b6b0f2020-m0.avro'
 *
 * `manifest file` is different in format version V1 and V2 and has the following contents:
 *                        v1     v2
 * status                 req    req
 * snapshot_id            req    opt
 * sequence_number               opt
 * file_sequence_number          opt
 * data_file              req    req
 * Example format version V1:
 * ┌─status─┬─────────snapshot_id─┬─data_file───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 2819310504515118887 │ ('/iceberg_data/db/table_name/data/00000-1-3edca534-15a0-4f74-8a28-4733e0bf1270-00001.parquet','PARQUET',(),100,1070,67108864,[(1,233),(2,210)],[(1,100),(2,100)],[(1,0),(2,0)],[],[(1,'\0'),(2,'0')],[(1,'c'),(2,'99')],NULL,[4],0) │
 * └────────┴─────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 * Example format version V2:
 * ┌─status─┬─────────snapshot_id─┬─sequence_number─┬─file_sequence_number─┬─data_file───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 5887006101709926452 │            ᴺᵁᴸᴸ │                 ᴺᵁᴸᴸ │ (0,'/iceberg_data/db/table_name/data/00000-1-c8045c90-8799-4eac-b957-79a0484e223c-00001.parquet','PARQUET',(),100,1070,[(1,233),(2,210)],[(1,100),(2,100)],[(1,0),(2,0)],[],[(1,'\0'),(2,'0')],[(1,'c'),(2,'99')],NULL,[4],[],0) │
 * └────────┴─────────────────────┴─────────────────┴──────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 * In case of partitioned data we'll have extra directory partition=value:
 * ─status─┬─────────snapshot_id─┬─data_file──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=0/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00001.parquet','PARQUET',(0),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0\0'),(2,'1')],[(1,'\0\0\0\0\0\0\0\0'),(2,'1')],NULL,[4],0) │
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=1/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00002.parquet','PARQUET',(1),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0'),(2,'2')],[(1,'\0\0\0\0\0\0\0'),(2,'2')],NULL,[4],0) │
 * │      1 │ 2252246380142525104 │ ('/iceberg_data/db/table_name/data/a=2/00000-1-c9535a00-2f4f-405c-bcfa-6d4f9f477235-00003.parquet','PARQUET',(2),1,631,67108864,[(1,46),(2,48)],[(1,1),(2,1)],[(1,0),(2,0)],[],[(1,'\0\0\0\0\0\0\0'),(2,'3')],[(1,'\0\0\0\0\0\0\0'),(2,'3')],NULL,[4],0) │
 * └────────┴─────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 */
struct ManifestFileContentImpl
{
public:
    explicit ManifestFileContentImpl(
        std::unique_ptr<avro::DataFileReaderBase> manifest_file_reader_,
        Int32 format_version_,
        const String & common_path,
        const DB::FormatSettings & format_settings,
        Int32 schema_id_,
        const DB::IcebergSchemaProcessor & schema_processor,
        Int64 inherited_sequence_number);

    Int32 schema_id;


    // Size - number of supported partition columns
    std::vector<PartitionColumnInfo> partition_column_infos;

    // Size - number of files
    std::vector<ManifestFileEntry> files;
};

}

#endif
