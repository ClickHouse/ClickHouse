#!/usr/bin/env bash
# Tags: long, no-ubsan, no-fasttest, no-parallel, no-asan, no-msan, no-tsan
# This test requires around 10 GB of memory and it is just too much.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Parquet"

# More info on: https://github.com/ClickHouse/ClickHouse/pull/54370

# File generated with the below code

#std::string random_string(size_t length) {
#  static const std::string characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
#
#  std::random_device random_device;
#  std::mt19937 generator(random_device());
#  std::uniform_int_distribution<> distribution(0, characters.size() - 1);
#
#  std::string random_string;
#  random_string.reserve(length);
#
#  std::generate_n(std::back_inserter(random_string), length, [&]() {
#    return characters[distribution(generator)];
#  });
#
#  return random_string;
#}
#
#static const std::string the_string = random_string(9247124u);
#
#std::shared_ptr<arrow::Array> CreateIntArray(std::size_t length) {
#  arrow::MemoryPool* pool = arrow::default_memory_pool();
#
#  auto int_builder_ptr = std::make_shared<arrow::Int64Builder>(pool);
#  auto & int_builder = *int_builder_ptr;
#  arrow::ListBuilder list_builder(pool, int_builder_ptr);
#
#  for (auto i = 0u; i < length; i++)
#  {
#    if (i % 10 == 0)
#    {
#      ARROW_CHECK_OK(list_builder.Append());
#    }
#    else
#    {
#      ARROW_CHECK_OK(int_builder.Append(i));
#    }
#  }
#
#  std::shared_ptr<arrow::Array> int_list_array;
#  ARROW_CHECK_OK(list_builder.Finish(&int_list_array));
#  return int_list_array;
#}
#
#std::shared_ptr<arrow::Array> CreateStringArray(std::size_t length) {
#  arrow::MemoryPool* pool = arrow::default_memory_pool();
#
#  auto str_builder = std::make_shared<arrow::LargeStringBuilder>(arrow::large_utf8(), pool);
#
#  for (auto i = 0u; i < length; i++)
#  {
#    if (i % 10 == 0)
#    {
#      ARROW_CHECK_OK(str_builder->AppendNull());
#    }
#    else
#    {
#      ARROW_CHECK_OK(str_builder->Append(the_string));
#    }
#  }
#
#  std::shared_ptr<arrow::Array> str_array;
#  ARROW_CHECK_OK(str_builder->Finish(&str_array));
#  return str_array;
#}
#
#void run()
#{
#  auto schema = arrow::schema({
#      arrow::field("ints", arrow::list(arrow::int64())),
#      arrow::field("strings", arrow::utf8())
#  });
#
#  auto l1_length = 2000;
#  auto l2_length = 2000;
#
#  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
#
#  auto int_array1 = CreateIntArray(l1_length);
#
#  auto int_array2 = CreateIntArray(l1_length);
#
#  auto str_array1 = CreateStringArray(l2_length);
#
#  auto str_array2 = CreateStringArray(l2_length);
#
#  batches.push_back(arrow::RecordBatch::Make(schema, int_array1->length(), {int_array1, str_array1}));
#
#  batches.push_back(arrow::RecordBatch::Make(schema, int_array2->length(), {int_array2, str_array2}));
#
#  std::shared_ptr<arrow::io::FileOutputStream> outfile;
#  PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open("generated.parquet"));
#
#  parquet::WriterProperties::Builder builder;
#  builder.compression(arrow::Compression::GZIP);
#  builder.dictionary_pagesize_limit(10*1024*1024);
#  builder.data_pagesize(20*1024*1024);
#
#  std::shared_ptr<parquet::WriterProperties> props = builder.build();
#
#  std::unique_ptr<parquet::arrow::FileWriter> file_writer;
#  PARQUET_ASSIGN_OR_THROW(file_writer, parquet::arrow::FileWriter::Open(*schema, ::arrow::default_memory_pool(), outfile, props));
#
#  for (const auto& batch : batches) {
#    PARQUET_THROW_NOT_OK(file_writer->WriteRecordBatch(*batch));
#  }
#
#  PARQUET_THROW_NOT_OK(file_writer->Close());
#}

DATA_FILE=$CUR_DIR/data_parquet/string_int_list_inconsistent_offset_multiple_batches.parquet

${CLICKHOUSE_LOCAL} "
DROP TABLE IF EXISTS parquet_load;
CREATE TABLE parquet_load (ints Array(Int64), strings Nullable(String)) ENGINE = Memory;
INSERT INTO parquet_load FROM INFILE '$DATA_FILE';
SELECT sum(cityHash64(*)) FROM parquet_load;
SELECT count() FROM parquet_load;
DROP TABLE parquet_load;
"
