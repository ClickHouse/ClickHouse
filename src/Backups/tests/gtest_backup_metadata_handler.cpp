#include <Backups/BackupMetadataHandler.h>

#include <Poco/SAX/SAXParser.h>

#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>


using namespace DB;

namespace
{
    struct ParseResult
    {
        BackupMetadataHandler::Fields header;
        std::vector<BackupMetadataHandler::Fields> files;
        bool header_seen = false;
        bool file_seen_before_header = false;
        std::exception_ptr saved_exception;
    };

    /// Drives the handler over `xml` exactly like `BackupImpl::readBackupMetadata` does (default `SAXParser`,
    /// `parseMemoryNP`), recording the header and per-file field maps.
    ParseResult parse(const std::string & xml)
    {
        ParseResult result;
        BackupMetadataHandler handler;
        handler.on_header = [&](const BackupMetadataHandler::Fields & h)
        {
            result.header = h;
            result.header_seen = true;
        };
        handler.on_file = [&](const BackupMetadataHandler::Fields & f)
        {
            if (!result.header_seen)
                result.file_seen_before_header = true;
            result.files.push_back(f);
        };

        Poco::XML::SAXParser parser;
        parser.setContentHandler(&handler);
        parser.parseMemoryNP(xml.data(), xml.size());
        result.saved_exception = handler.saved_exception;
        return result;
    }

    /// A manifest with two files, mirroring the whitespace-free output of `writeBackupMetadata`.
    const std::string two_files_xml =
        "<config>"
        "<version>2</version>"
        "<timestamp>2020-01-01 00:00:00</timestamp>"
        "<uuid>00000000-0000-0000-0000-000000000001</uuid>"
        "<base_backup>Disk('backups', 'base')</base_backup>"
        "<base_backup_uuid>00000000-0000-0000-0000-000000000002</base_backup_uuid>"
        "<contents>"
        "<file>"
        "<name>data/db/tbl/full.bin</name>"
        "<size>100</size>"
        "<checksum>0123456789abcdef0123456789abcdef</checksum>"
        "</file>"
        "<file>"
        "<name>data/db/tbl/incremental.bin</name>"
        "<size>200</size>"
        "<checksum>fedcba9876543210fedcba9876543210</checksum>"
        "<use_base>true</use_base>"
        "<base_size>150</base_size>"
        "<base_checksum>aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa</base_checksum>"
        "<data_file>data/db/tbl/other.bin</data_file>"
        "<encrypted_by_disk>true</encrypted_by_disk>"
        "</file>"
        "</contents>"
        "</config>";
}


TEST(BackupMetadataHandler, ParsesHeaderFields)
{
    auto result = parse(two_files_xml);

    EXPECT_TRUE(result.header_seen);
    EXPECT_EQ(result.header.at("version"), "2");
    EXPECT_EQ(result.header.at("timestamp"), "2020-01-01 00:00:00");
    EXPECT_EQ(result.header.at("uuid"), "00000000-0000-0000-0000-000000000001");
    EXPECT_EQ(result.header.at("base_backup"), "Disk('backups', 'base')");
    EXPECT_EQ(result.header.at("base_backup_uuid"), "00000000-0000-0000-0000-000000000002");
    /// `<contents>` is not a header leaf and must not leak into the header map.
    EXPECT_EQ(result.header.count("contents"), 0u);
}

TEST(BackupMetadataHandler, ParsesFilesInOrderWithAllLeafFields)
{
    auto result = parse(two_files_xml);

    ASSERT_EQ(result.files.size(), 2u);

    const auto & f0 = result.files[0];
    EXPECT_EQ(f0.at("name"), "data/db/tbl/full.bin");
    EXPECT_EQ(f0.at("size"), "100");
    EXPECT_EQ(f0.at("checksum"), "0123456789abcdef0123456789abcdef");
    /// Optional fields that were not present must be absent (not empty strings).
    EXPECT_EQ(f0.count("use_base"), 0u);
    EXPECT_EQ(f0.count("base_size"), 0u);
    EXPECT_EQ(f0.count("data_file"), 0u);

    const auto & f1 = result.files[1];
    EXPECT_EQ(f1.at("name"), "data/db/tbl/incremental.bin");
    EXPECT_EQ(f1.at("size"), "200");
    EXPECT_EQ(f1.at("use_base"), "true");
    EXPECT_EQ(f1.at("base_size"), "150");
    EXPECT_EQ(f1.at("base_checksum"), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    EXPECT_EQ(f1.at("data_file"), "data/db/tbl/other.bin");
    EXPECT_EQ(f1.at("encrypted_by_disk"), "true");
}

TEST(BackupMetadataHandler, HeaderIsAppliedBeforeAnyFile)
{
    auto result = parse(two_files_xml);
    EXPECT_FALSE(result.file_seen_before_header);
}

TEST(BackupMetadataHandler, EmptyContentsStillAppliesHeader)
{
    auto result = parse("<config><version>1</version><contents></contents></config>");

    EXPECT_TRUE(result.header_seen);
    EXPECT_EQ(result.header.at("version"), "1");
    EXPECT_TRUE(result.files.empty());
}

TEST(BackupMetadataHandler, FieldMapIsResetBetweenFiles)
{
    /// The second file omits `checksum`; it must not inherit the first file's value.
    auto result = parse(
        "<config><version>2</version><contents>"
        "<file><name>a</name><size>1</size><checksum>c1</checksum></file>"
        "<file><name>b</name><size>0</size></file>"
        "</contents></config>");

    ASSERT_EQ(result.files.size(), 2u);
    EXPECT_EQ(result.files[1].at("name"), "b");
    EXPECT_EQ(result.files[1].count("checksum"), 0u);
}

TEST(BackupMetadataHandler, CapturesFileCallbackExceptionAndShortCircuits)
{
    BackupMetadataHandler handler;
    int file_calls = 0;
    handler.on_file = [&](const BackupMetadataHandler::Fields &)
    {
        ++file_calls;
        throw std::runtime_error("boom");
    };

    Poco::XML::SAXParser parser;
    parser.setContentHandler(&handler);
    /// The exception must NOT propagate through the expat-based parser.
    EXPECT_NO_THROW(parser.parseMemoryNP(two_files_xml.data(), two_files_xml.size()));

    /// The first file threw; the second must be short-circuited.
    EXPECT_EQ(file_calls, 1);
    ASSERT_TRUE(handler.saved_exception);
    EXPECT_THROW(std::rethrow_exception(handler.saved_exception), std::runtime_error);
}

TEST(BackupMetadataHandler, HeaderCallbackExceptionShortCircuitsFiles)
{
    BackupMetadataHandler handler;
    int file_calls = 0;
    handler.on_header = [&](const BackupMetadataHandler::Fields &) { throw std::runtime_error("bad header"); };
    handler.on_file = [&](const BackupMetadataHandler::Fields &) { ++file_calls; };

    Poco::XML::SAXParser parser;
    parser.setContentHandler(&handler);
    EXPECT_NO_THROW(parser.parseMemoryNP(two_files_xml.data(), two_files_xml.size()));

    EXPECT_EQ(file_calls, 0);
    ASSERT_TRUE(handler.saved_exception);
    EXPECT_THROW(std::rethrow_exception(handler.saved_exception), std::runtime_error);
}

TEST(BackupMetadataHandler, MalformedXmlThrowsFromParser)
{
    BackupMetadataHandler handler;
    Poco::XML::SAXParser parser;
    parser.setContentHandler(&handler);

    /// A parse error (mismatched tags) is reported by the parser itself, not captured in saved_exception.
    const std::string bad = "<config><version>2</version></contents>";
    EXPECT_ANY_THROW(parser.parseMemoryNP(bad.data(), bad.size()));
}
