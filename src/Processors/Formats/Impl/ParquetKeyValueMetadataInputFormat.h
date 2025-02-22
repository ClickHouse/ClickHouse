#pragma once
#include "config.h"
#if USE_PARQUET

#    include <Formats/FormatSettings.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <parquet/metadata.h>

namespace DB
{

/* Special format that always returns one row containing all key-value metadata pairs from a Parquet file
 * (see https://github.com/apache/parquet-format/blob/94b9d631aef332c78b8f1482fb032743a9c3c407/src/main/thrift/parquet.thrift#L1263).
 * The result row has the following structure:
 * key_value_metadata - a Map(String, String) containing all custom key-value metadata pairs from the file
 *
 * Note: If a Parquet file contains no key-value metadata entries, this format will return one row
 * with an empty map.
 * */

class ParquetKeyValueMetadataInputFormat : public IInputFormat
{
public:
    ParquetKeyValueMetadataInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "ParquetKeyValueMetadataInputFormat"; }

    void resetParser() override;

private:
    Chunk read() override;

    void onCancel() noexcept override { is_stopped = 1; }

    void fillKeyValueMetadata(const std::shared_ptr<parquet::FileMetaData> & metadata, MutableColumnPtr & column);

    const FormatSettings format_settings;
    bool done = false;
    std::atomic<int> is_stopped{0};
};

class ParquetKeyValueMetadataSchemaReader : public ISchemaReader
{
public:
    explicit ParquetKeyValueMetadataSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;
};

}

#endif
