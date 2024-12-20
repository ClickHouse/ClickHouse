#include <gtest/gtest.h>

#include <config.h>
#if USE_PARQUET

#    include <Columns/ColumnsNumber.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <IO/WriteBufferFromFile.h>
#    include <Processors/Executors/CompletedPipelineExecutor.h>
#    include <Processors/Executors/PipelineExecutor.h>
#    include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#    include <Processors/ISource.h>
#    include <QueryPipeline/QueryPipelineBuilder.h>

#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <arrow/io/file.h>
#    include <parquet/file_reader.h>
#    include <parquet/page_index.h>
#    include <parquet/thrift_internal.h>

using namespace DB;
namespace
{
template <class T>
std::shared_ptr<ISource> multiColumnsSource(const std::vector<DataTypePtr> & type, const std::vector<std::vector<T>> & values, size_t times)
{
    Block header;
    for (size_t i = 0; i < type.size(); ++i)
    {
        header.insert(ColumnWithTypeAndName(type[i], "x" + std::to_string(i)));
    }


    Chunks chunks;

    for (size_t i = 0; i < times; ++i)
    {
        Chunk chunk;
        Columns columns;
        for (size_t j = 0; j < type.size(); ++j)
        {
            auto column = type[j]->createColumn();
            for (const auto& n : values[j])
            {
                if constexpr (std::is_same_v<T, UInt64>)
                    if (n == 0)
                        column->insertDefault();
                    else
                        column->insert(n);
                else
                    column->insert(n);
            }
            columns.push_back(std::move(column));
        }
        chunk = Chunk(Columns{std::move(columns)}, values[0].size());
        chunks.push_back(std::move(chunk));
    }
    return std::make_shared<SourceFromChunks>(header, std::move(chunks));
}

void validatePageIndex(
    String path,
    std::optional<std::function<void(std::vector<bool>)>> validate_null_pages = std::nullopt,
    std::optional<std::function<void(std::vector<int64_t>)>> validate_null_counts = std::nullopt)
{
    std::shared_ptr<::arrow::io::RandomAccessFile> source;
    PARQUET_ASSIGN_OR_THROW(source, ::arrow::io::ReadableFile::Open(path))
    auto reader = parquet::ParquetFileReader::OpenFile(path);
    auto metadata = reader->metadata();
    auto properties = parquet::default_reader_properties();
    parquet::ThriftDeserializer deserializer(properties);
    for (int i = 0; i < metadata->num_row_groups(); ++i)
    {
        auto row_group = metadata->RowGroup(i);
        std::vector<int64_t> column_index_offsets;
        std::vector<int64_t> column_index_lengths;
        std::vector<int64_t> offset_index_offsets;
        std::vector<int64_t> offset_index_lengths;
        for (int j = 0; j < row_group->num_columns(); j++)
        {
            auto column_chunk = row_group->ColumnChunk(j);
            auto column_index = column_chunk->GetColumnIndexLocation();
            auto offset_index = column_chunk->GetOffsetIndexLocation();
            ASSERT_TRUE(column_index.has_value());
            ASSERT_TRUE(offset_index.has_value());
            ASSERT_GT(column_index.value().offset, 0);
            ASSERT_GT(column_index.value().length, 0);
            ASSERT_GT(offset_index.value().offset, 0);
            ASSERT_GT(offset_index.value().length, 0);
            column_index_offsets.push_back(column_index.value().offset);
            column_index_lengths.push_back(column_index.value().length);
            offset_index_offsets.push_back(offset_index.value().offset);
            offset_index_lengths.push_back(offset_index.value().length);
        }
        for (int k = 0; k < row_group->num_columns(); k++)
        {
            auto page_index_reader = reader->GetPageIndexReader();
            ASSERT_TRUE(page_index_reader != nullptr);
            PARQUET_ASSIGN_OR_THROW(auto column_index_buffer, source->ReadAt(column_index_offsets[k], column_index_lengths[k]))
            PARQUET_ASSIGN_OR_THROW(auto offset_index_buffer, source->ReadAt(offset_index_offsets[k], offset_index_lengths[k]))
            const auto *column_descr = metadata->schema()->Column(k);
            std::unique_ptr<parquet::ColumnIndex> column_index = parquet::ColumnIndex::Make(
                *column_descr, column_index_buffer->data(), static_cast<uint32_t>(column_index_buffer->size()), properties);
            std::unique_ptr<parquet::OffsetIndex> offset_index
                = parquet::OffsetIndex::Make(offset_index_buffer->data(), static_cast<uint32_t>(offset_index_buffer->size()), properties);
            // validate null pages
            if (validate_null_pages.has_value())
            {
                validate_null_pages.value()(column_index->null_pages());
            }
            size_t num_pages = offset_index->page_locations().size();
            // validate null counts
            if (column_index->has_null_counts())
            {
                ASSERT_EQ(column_index->null_counts().size(), num_pages);
                if (validate_null_counts.has_value())
                {
                    validate_null_counts.value()(column_index->null_counts());
                }
            }
            // validate min max values
            auto total_values = 0;
            for (size_t l = 0; l < num_pages; l++)
            {
                auto page_location = offset_index->page_locations().at(l);
                PARQUET_ASSIGN_OR_THROW(auto page_buffer, source->ReadAt(page_location.offset, page_location.compressed_page_size))
                parquet::format::PageHeader header;
                uint32_t header_size = static_cast<uint32_t>(page_buffer->size());
                deserializer.DeserializeMessage(page_buffer->data(), &header_size, &header);
                ASSERT_TRUE(header.type == parquet::format::PageType::DATA_PAGE);
                if (!column_index->null_pages().at(l))
                {
                    ASSERT_EQ(header.data_page_header.statistics.min_value, column_index->encoded_min_values().at(l));
                    ASSERT_EQ(header.data_page_header.statistics.max_value, column_index->encoded_max_values().at(l));
                    if (column_index->has_null_counts())
                        ASSERT_GT(header.data_page_header.num_values, column_index->null_counts().at(l));
                }
                else
                {
                    ASSERT_EQ(header.data_page_header.num_values, column_index->null_counts().at(l));
                }
                ASSERT_EQ(page_location.first_row_index, total_values);
                total_values += header.data_page_header.num_values;
            }
        }
    }
}

void writeParquet(SourcePtr source, const FormatSettings & format_settings, String parquet_path)
{
    QueryPipelineBuilder pipeline_builder;
    pipeline_builder.init(Pipe(source));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    WriteBufferFromFile write_buffer(parquet_path);
    auto output = std::make_shared<ParquetBlockOutputFormat>(write_buffer, pipeline.getHeader(), format_settings);
    pipeline.complete(output);
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
}

TEST(Parquet, WriteParquetPageIndexParrelel)
{
    FormatSettings format_settings;
    format_settings.parquet.row_group_rows = 10000;
    format_settings.parquet.use_custom_encoder = true;
    format_settings.parquet.parallel_encoding = true;
    format_settings.parquet.write_page_index = true;
    format_settings.parquet.data_page_size = 32;
    format_settings.max_threads = 2;

    std::vector<std::vector<UInt64>> values;
    std::vector<UInt64> col;
    col.reserve(1000);
    for (size_t i = 0; i < 1000; i++)
    {
        col.push_back(i % 10);
    }
    values.push_back(col);
    values.push_back(col);
    auto source = multiColumnsSource<UInt64>(
        {makeNullable(std::make_shared<DataTypeUInt64>()), makeNullable(std::make_shared<DataTypeUInt64>())}, values, 100);
    String path = "/tmp/test.parquet";
    writeParquet(source, format_settings, path);
    validatePageIndex(
        path,
        [](auto null_pages)
        {
            for (auto null_page : null_pages)
            {
                ASSERT_TRUE(!null_page);
            }
        },
        [](auto null_counts)
        {
            for (auto null_count : null_counts)
            {
                ASSERT_TRUE(null_count > 0);
            }
        });
}

TEST(Parquet, WriteParquetPageIndexParrelelPlainEnconding)
{
    FormatSettings format_settings;
    format_settings.parquet.row_group_rows = 10000;
    format_settings.parquet.use_custom_encoder = true;
    format_settings.parquet.parallel_encoding = true;
    format_settings.parquet.write_page_index = true;
    format_settings.parquet.data_page_size = 32;
    format_settings.max_threads = 2;

    std::vector<std::vector<String>> values;
    std::vector<String> col;
    for (size_t i = 0; i < 100000; i++)
    {
        col.push_back(std::to_string(i));
    }
    values.push_back(col);
    values.push_back(col);
    auto source = multiColumnsSource<String>(
        {makeNullable(std::make_shared<DataTypeString>()), makeNullable(std::make_shared<DataTypeString>())}, values, 10);
    String path = "/tmp/test.parquet";
    writeParquet(source, format_settings, path);
    validatePageIndex(
        path,
        [](auto null_pages)
        {
            for (auto null_page : null_pages)
            {
                ASSERT_FALSE(null_page);
            }
        },
        [](auto null_counts)
        {
            for (auto null_count : null_counts)
            {
                ASSERT_EQ(null_count, 0);
            }
        });
}

TEST(Parquet, WriteParquetPageIndexParrelelAllNull)
{
    FormatSettings format_settings;
    format_settings.parquet.row_group_rows = 10000;
    format_settings.parquet.use_custom_encoder = true;
    format_settings.parquet.parallel_encoding = true;
    format_settings.parquet.write_page_index = true;
    format_settings.parquet.data_page_size = 32;
    format_settings.max_threads = 2;

    std::vector<std::vector<UInt64>> values;
    auto & col = values.emplace_back();
    for (size_t i = 0; i < 1000; i++)
    {
        col.push_back(0);
    }
    auto source = multiColumnsSource<UInt64>({makeNullable(std::make_shared<DataTypeUInt64>())}, values, 100);
    String path = "/tmp/test.parquet";
    writeParquet(source, format_settings, path);
    validatePageIndex(
        path,
        [](auto null_pages)
        {
            for (auto null_page : null_pages)
            {
                ASSERT_TRUE(null_page);
            }
        },
        [](auto null_counts)
        {
            for (auto null_count : null_counts)
            {
                ASSERT_TRUE(null_count > 0);
            }
        });
}

TEST(Parquet, WriteParquetPageIndexSingleThread)
{
    FormatSettings format_settings;
    format_settings.parquet.row_group_rows = 10000;
    format_settings.parquet.use_custom_encoder = true;
    format_settings.parquet.parallel_encoding = false;
    format_settings.parquet.write_page_index = true;
    format_settings.parquet.data_page_size = 32;

    std::vector<std::vector<UInt64>> values;
    std::vector<UInt64> col;
    for (size_t i = 0; i < 1000; i++)
    {
        col.push_back(i % 10);
    }
    values.push_back(col);
    values.push_back(col);
    auto source = multiColumnsSource<UInt64>(
        {makeNullable(std::make_shared<DataTypeUInt64>()), makeNullable(std::make_shared<DataTypeUInt64>())}, values, 100);
    String path = "/tmp/test.parquet";
    writeParquet(source, format_settings, path);
    validatePageIndex(
        path,
        [](auto null_pages)
        {
            for (auto null_page : null_pages)
            {
                ASSERT_TRUE(!null_page);
            }
        },
        [](auto null_counts)
        {
            for (auto null_count : null_counts)
            {
                ASSERT_TRUE(null_count > 0);
            }
        });
}
}
#endif
