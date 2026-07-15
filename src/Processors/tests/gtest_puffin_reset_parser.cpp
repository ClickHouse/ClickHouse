#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Formats/Impl/PuffinBlockInputFormat.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_OPEN_FILE;
}

namespace
{

String readBinaryFile(const String & path)
{
    std::ifstream in(path, std::ios::binary);
    if (!in)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open file {}", path);

    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

String puffinFixturePath(const char * filename)
{
    return (std::filesystem::path(CLICKHOUSE_TESTS_CONFIG_DIR) / ".." / "queries" / "0_stateless" / "data_puffin" / filename).string();
}

Block makeSampleBlock(const String & format_name, const String & fixture_path)
{
    ReadBufferFromFile schema_buf(fixture_path);
    NamesAndTypesList schema;
    if (format_name == "Puffin")
    {
        PuffinSchemaReader schema_reader(schema_buf);
        schema = schema_reader.readSchema();
    }
    else
    {
        PuffinMetadataSchemaReader schema_reader(schema_buf);
        schema = schema_reader.readSchema();
    }

    ColumnsWithTypeAndName columns;
    for (const auto & [name, type] : schema)
        columns.emplace_back(type->createColumn(), type, name);
    return Block(std::move(columns));
}

size_t executePuffinFormat(const String & format_name, const String & fixture_path)
{
    const auto context = getContext().context;
    const Block sample = makeSampleBlock(format_name, fixture_path);
    const String puffin_data = readBinaryFile(fixture_path);

    EmptyReadBuffer empty_buffer;
    auto format = context->getInputFormat(format_name, empty_buffer, sample, 8192);
    StreamingFormatExecutor executor(sample, format);

    ReadBufferFromMemory first_buffer(puffin_data.data(), puffin_data.size());
    const size_t first_rows = executor.execute(first_buffer);
    EXPECT_GT(first_rows, 0u);

    ReadBufferFromMemory second_buffer(puffin_data.data(), puffin_data.size());
    const size_t second_rows = executor.execute(second_buffer);
    EXPECT_EQ(second_rows, first_rows);

    return first_rows;
}

}

TEST(PuffinResetParser, PuffinReusedAcrossBuffers)
{
    tryRegisterFormats();
    executePuffinFormat("Puffin", puffinFixturePath("spark_deletion_vector.puffin"));
}

TEST(PuffinResetParser, PuffinMetadataReusedAcrossBuffers)
{
    tryRegisterFormats();
    executePuffinFormat("PuffinMetadata", puffinFixturePath("mixed_blob_types.puffin"));
}

}
