#include "config.h"

#if USE_ARROW || USE_PARQUET


#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/util/logging.h>
#include <arrow/util/type_fwd.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <random>

#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <IO/ReadBufferFromString.h>
#include <gtest/gtest.h>


namespace DB
{

namespace
{

    std::string random_string(size_t length) {
        static const std::string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        std::random_device random_device;
        std::mt19937 generator(random_device());
        std::uniform_int_distribution<> distribution(0, static_cast<int>(characters.size()) - 1);

        std::string random_string;
        random_string.reserve(length);

        std::generate_n(std::back_inserter(random_string), length, [&]() {
                            return characters[distribution(generator)];
                        });

        return random_string;
    }

    const std::string the_string = random_string(9247124u);

    std::shared_ptr<arrow::Array> CreateIntArray(std::size_t length) {
        arrow::MemoryPool* pool = arrow::default_memory_pool();

        auto int_builder_ptr = std::make_shared<arrow::Int64Builder>(pool);
        auto & int_builder = *int_builder_ptr;
        arrow::ListBuilder list_builder(pool, int_builder_ptr);

        for (auto i = 0u; i < length; i++)
        {
            if (i % 10 == 0)
            {
                ARROW_CHECK_OK(list_builder.Append());
            }
            else
            {
                ARROW_CHECK_OK(int_builder.Append(i));
            }
        }

        std::shared_ptr<arrow::Array> int_list_array;
        ARROW_CHECK_OK(list_builder.Finish(&int_list_array));
        return int_list_array;
    }

    std::shared_ptr<arrow::Array> CreateStringArray(std::size_t length) {
        arrow::MemoryPool* pool = arrow::default_memory_pool();

        auto str_builder = std::make_shared<arrow::LargeStringBuilder>(arrow::large_utf8(), pool);

        for (auto i = 0u; i < length; i++)
        {
            if (i % 10 == 0)
            {
                ARROW_CHECK_OK(str_builder->AppendNull());
            }
            else
            {
                //      auto str = pre_calculated_random_string(i);

                ARROW_CHECK_OK(str_builder->Append(the_string));
            }
        }

        std::shared_ptr<arrow::Array> str_array;
        ARROW_CHECK_OK(str_builder->Finish(&str_array));
        return str_array;
    }

    auto createFile()
    {
        auto schema = arrow::schema({
            arrow::field("ints", arrow::list(arrow::int64())),
            arrow::field("strings", arrow::utf8())
        });

        auto l1_length = 2000;
        auto l2_length = 2000;

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

        auto int_array1 = CreateIntArray(l1_length);

        auto int_array2 = CreateIntArray(l1_length);

        auto str_array1 = CreateStringArray(l2_length);

        auto str_array2 = CreateStringArray(l2_length);

        batches.push_back(arrow::RecordBatch::Make(schema, int_array1->length(), {int_array1, str_array1}));

        batches.push_back(arrow::RecordBatch::Make(schema, int_array2->length(), {int_array2, str_array2}));

        std::shared_ptr<arrow::io::BufferOutputStream> outfile;
        PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::BufferOutputStream::Create())

        parquet::WriterProperties::Builder builder;
//        builder.compression(parquet::Compression::BROTLI);
        std::shared_ptr<parquet::WriterProperties> props = builder.build();

        std::unique_ptr<parquet::arrow::FileWriter> file_writer;
        PARQUET_ASSIGN_OR_THROW(file_writer, parquet::arrow::FileWriter::Open(*schema, ::arrow::default_memory_pool(), outfile, props))

        for (const auto& batch : batches) {
            PARQUET_THROW_NOT_OK(file_writer->WriteRecordBatch(*batch));
        }

        PARQUET_THROW_NOT_OK(file_writer->Close());

        return outfile->Finish();
    }

}


TEST(Parquet, CreateParquetReader)
{
    [[maybe_unused]] auto file = createFile();
    std::string s = "Hello!";
    ReadBufferFromString buf(s);
    FormatSettings format_settings;
    std::unique_ptr<IProcessor> processor = std::make_unique<ParquetBlockInputFormat>(buf, Block{}, format_settings, 1, 1);

    processor->work();

//    EXPECT_EQ(format.getName(), "ParquetBlockInputFormat");
}

}

#endif
