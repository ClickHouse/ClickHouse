#include <exception>
#include <memory>
#include <gtest/gtest.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>
#include <IO/ReadBufferFromMemoryIterable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>

using namespace DB;

class StreamingFormatExecutorTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        input_params.max_block_size = 100;
        format_settings.tsv.try_detect_header = true;

        on_error = [&](std::exception_ptr e)
        {
            errors.push_back(getExceptionMessage(e, false));
        };
    }

    Block header = {ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")};
    FormatSettings format_settings;
    RowInputFormatParams input_params;

    std::vector<std::string> errors;
    StreamingFormatExecutor::ErrorCallback on_error;

    EmptyReadBuffer empty_buf;
};


TEST_F(StreamingFormatExecutorTest, ReadBufferFromMemoryIterableNoErrors)
{
    auto format = std::make_shared<TabSeparatedRowInputFormat>(
            header, empty_buf, input_params, false, false, false, format_settings);
    StreamingFormatExecutor executor(header, format, on_error);

    std::vector<std::string_view> rows = {
        {"0"},
        {"1"},
    };
    auto it = rows.cbegin();
    DB::ReadBufferFromMemoryIterable buffer(it, rows.cend());

    size_t num_rows = executor.execute(buffer);
    ASSERT_EQ(it, rows.cend()) << fmt::format("Not all rows had been read, left: {}", rows.cend() - it);
    ASSERT_EQ(num_rows, rows.size());
    ASSERT_EQ(errors.size(), 0);

    auto block = executor.getResultColumns();
    ASSERT_EQ(block.size(), header.columns());
    auto & column = block[0];
    ASSERT_EQ(column->size(), rows.size());
    const auto & column_data = assert_cast<const ColumnUInt8 *>(column.get())->getData();
    ASSERT_EQ(column_data[0], 0);
    ASSERT_EQ(column_data[1], 1);

    ASSERT_EQ(errors.size(), 0);
}

TEST_F(StreamingFormatExecutorTest, ReadBufferFromMemoryIterableWithErrors)
{
    input_params.allow_errors_num = 1;
    auto format = std::make_shared<TabSeparatedRowInputFormat>(
            header, empty_buf, input_params, false, false, false, format_settings);
    StreamingFormatExecutor executor(header, format, on_error);

    std::vector<std::string_view> rows = {
        {"0"},
        {"foo"},
        {"1"},
    };
    auto it = rows.cbegin();
    DB::ReadBufferFromMemoryIterable buffer(it, rows.cend());

    size_t num_rows = executor.execute(buffer);
    ASSERT_EQ(it, rows.cend()) << fmt::format("Not all rows had been read, left: {}", rows.cend() - it);
    ASSERT_EQ(num_rows, rows.size() - errors.size());

    auto block = executor.getResultColumns();
    ASSERT_EQ(block.size(), header.columns());
    auto & column = block[0];
    ASSERT_EQ(column->size(), rows.size() - errors.size());
    const auto & column_data = assert_cast<const ColumnUInt8 *>(column.get())->getData();
    ASSERT_EQ(column_data[0], 0);
    ASSERT_EQ(column_data[1], 1);

    ASSERT_EQ(errors.size(), 1);
    ASSERT_TRUE(errors.front().contains("Cannot parse input: expected '\\n' before: 'foo'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)")) << errors.front();
}
