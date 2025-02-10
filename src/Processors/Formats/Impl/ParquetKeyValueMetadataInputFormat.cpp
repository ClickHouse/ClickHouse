#include "ParquetKeyValueMetadataInputFormat.h"

#if USE_PARQUET

#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnTuple.h>
#    include <Core/NamesAndTypes.h>
#    include <DataTypes/DataTypeMap.h>
#    include <Formats/FormatFactory.h>
#    include <Processors/Formats/Impl/Parquet/ParquetMetadataReader.h>
#    include <arrow/api.h>

namespace DB
{

static NamesAndTypesList getHeaderForParquetKeyValueMetadata()
{
    NamesAndTypesList names_and_types{
        {"key_value_metadata", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())}};
    return names_and_types;
}

ParquetKeyValueMetadataInputFormat::ParquetKeyValueMetadataInputFormat(
    ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_)
    , format_settings(format_settings_)
{
}

Chunk ParquetKeyValueMetadataInputFormat::read()
{
    Chunk res;
    if (done)
        return res;

    auto metadata = getFileMetadata(*in, format_settings, is_stopped);
    auto names_and_types = getHeaderForParquetKeyValueMetadata();
    auto types = names_and_types.getTypes();

    auto column = types[0]->createColumn();
    fillKeyValueMetadata(metadata, column);
    res.addColumn(std::move(column));
    done = true;
    return res;
}

void ParquetKeyValueMetadataInputFormat::fillKeyValueMetadata(
    const std::shared_ptr<parquet::FileMetaData> & metadata, MutableColumnPtr & column)
{
    auto & column_map = assert_cast<ColumnMap &>(*column);
    auto & key_column = column_map.getNestedData().getColumn(0);
    auto & value_column = column_map.getNestedData().getColumn(1);
    auto key_value_metadata = metadata->key_value_metadata();
    if (!key_value_metadata)
        return;

    for (int i = 0; i < key_value_metadata->size(); ++i)
    {
        key_column.insert(key_value_metadata->key(i));
        value_column.insert(key_value_metadata->value(i));
    }

    column_map.getNestedColumn().getOffsets().push_back(key_column.size());
}

void ParquetKeyValueMetadataInputFormat::resetParser()
{
    IInputFormat::resetParser();
    done = false;
}

ParquetKeyValueMetadataSchemaReader::ParquetKeyValueMetadataSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList ParquetKeyValueMetadataSchemaReader::readSchema()
{
    return getHeaderForParquetKeyValueMetadata();
}

void registerInputFormatParquetKeyValueMetadata(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "ParquetKeyValueMetadata",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings &,
           bool /* is_remote_fs */,
           size_t /* max_download_threads */,
           size_t /* max_parsing_threads */) { return std::make_shared<ParquetKeyValueMetadataInputFormat>(buf, sample, settings); });
    factory.markFormatSupportsSubsetOfColumns("ParquetKeyValueMetadata");
}

void registerParquetKeyValueMetadataSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "ParquetKeyValueMetadata",
        [](ReadBuffer & buf, const FormatSettings &) { return std::make_shared<ParquetKeyValueMetadataSchemaReader>(buf); });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatParquetKeyValueMetadata(FormatFactory &)
{
}

void registerParquetKeyValueMetadataSchemaReader(FormatFactory &)
{
}
}

#endif
