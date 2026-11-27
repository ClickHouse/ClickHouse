#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Common/StringUtils.h>

#include <fmt/format.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <vector>


/// A data part, as a server receives it from a replica on a fetch or a user attaches it from `detached`,
/// together with the table it belongs to. `clickhouse_fuzzer.cpp` runs `clickhouse local`; this file
/// writes the part of an input into `detached` and gives the queries that attach it and read it.
/// The input is:
///
///     UInt8 flags                  bit 1: keep `checksums.txt` of the input instead of making it
///     String table                 `CREATE TABLE` after the name of the table, without `;`
///     VarUInt number of queries, then each query as a String: `SELECT` or `CHECK TABLE` of `fuzz.t`, without `;`
///     String part name
///     until the end of the input:
///         String file name         `<file>` or `<projection>.proj/<file>`
///         UInt8 compressed         bit 1: the content goes through a compressed stream
///         String content
///
/// A replica that sends a part computes its checksums too, so making them does not hide anything.

using namespace DB;
namespace fs = std::filesystem;

String makeFuzzerQuery(const uint8_t * data, size_t size);
String makeFuzzerSetupQuery();

namespace
{

/// The files that a part has outside of its checksums.
constexpr std::string_view files_without_checksums[] = {
    "checksums.txt", "columns.txt", "columns_substreams.txt", "default_compression_codec.txt",
    "metadata_version.txt", "txn_version.txt", "uuid.txt",
};

constexpr size_t max_files = 1000;
constexpr size_t max_queries = 32;

struct File
{
    String content;
    bool compressed = false;
};

using Files = std::map<String, File>;

bool isValidName(std::string_view name)
{
    return !name.empty() && name.size() <= 255 && name != "." && name != ".."
        && std::ranges::all_of(name, [](char c) { return isAlphaNumericASCII(c) || c == '_' || c == '-' || c == '.' || c == '%'; });
}

/// Writes the files of a part or of one of its projections as the writer of a part does.
MergeTreeDataPartChecksums writeFiles(const fs::path & path, const Files & files)
{
    fs::create_directories(path);
    MergeTreeDataPartChecksums checksums;
    for (const auto & [name, file] : files)
    {
        WriteBufferFromFile out((path / name).string());
        HashingWriteBuffer out_hashing(out);
        if (file.compressed)
        {
            CompressedWriteBuffer compressor(out_hashing, CompressionCodecFactory::instance().get("LZ4", {}));
            HashingWriteBuffer uncompressed_hashing(compressor);
            uncompressed_hashing.write(file.content.data(), file.content.size());
            uncompressed_hashing.finalize();
            compressor.finalize();
            out_hashing.finalize();
            checksums.files[name] = MergeTreeDataPartChecksum(
                out_hashing.count(), out_hashing.getHash(), uncompressed_hashing.count(), uncompressed_hashing.getHash());
        }
        else
        {
            out_hashing.write(file.content.data(), file.content.size());
            out_hashing.finalize();
            checksums.files[name] = MergeTreeDataPartChecksum(out_hashing.count(), out_hashing.getHash());
        }
        out.finalize();
    }

    for (const auto name : files_without_checksums)
        checksums.files.erase(String(name));
    return checksums;
}

void writeChecksums(const fs::path & path, const MergeTreeDataPartChecksums & checksums)
{
    WriteBufferFromFile out((path / "checksums.txt").string());
    checksums.write(out);
    out.finalize();
}

}

String makeFuzzerSetupQuery()
{
    return "SET checksum_on_read = 0, enable_full_text_index = 1, allow_experimental_vector_similarity_index = 1, "
           "use_query_condition_cache = 0, max_threads = 2;"
           "CREATE DATABASE IF NOT EXISTS fuzz ENGINE = Atomic;";
}

String makeFuzzerQuery(const uint8_t * data, size_t size)
{
    /// A new table gets a new UUID: `DROP TABLE ... SYNC` of the previous one would remove the files written here,
    /// so they go into the directory of the new table, which `ATTACH TABLE` takes as it is.
    static const fs::path store = fs::path(Context::getGlobalContextInstance()->getPath()) / "store";
    static String table;
    static UInt64 table_number = 0;
    auto table_path = [](UInt64 number)
    {
        auto uuid = fmt::format("00000000-0000-4000-8000-{:012x}", number);
        return std::pair{uuid, store / uuid.substr(0, 3) / uuid};
    };

    ReadBufferFromMemory in(data, size);
    UInt8 flags = 0;
    String input_table;
    std::vector<String> queries;
    String part_name;
    std::map<String, Files> directories;
    try
    {
        readBinary(flags, in);
        readStringBinary(input_table, in, size);
        UInt64 num_queries = 0;
        readVarUInt(num_queries, in);
        for (UInt64 i = 0; i < std::min<UInt64>(num_queries, max_queries); ++i)
        {
            String query;
            readStringBinary(query, in, size);
            if (query.find(';') == String::npos && (query.starts_with("SELECT ") || query.starts_with("CHECK TABLE ")))
                queries.push_back(std::move(query));
        }
        readStringBinary(part_name, in, 255);
        if (input_table.empty() || input_table.find(';') != String::npos || !isValidName(part_name))
            return {};

        for (size_t i = 0; i < max_files && !in.eof(); ++i)
        {
            String path;
            UInt8 compressed = 0;
            File file;
            readStringBinary(path, in, 512);
            readBinary(compressed, in);
            readStringBinary(file.content, in, size);
            file.compressed = compressed & 1;

            /// The directory is empty for the part itself.
            String directory;
            String name = path;
            if (auto slash = path.find('/'); slash != String::npos)
            {
                directory = path.substr(0, slash);
                name = path.substr(slash + 1);
                if (!directory.ends_with(".proj") || !isValidName(directory))
                    continue;
            }
            if (isValidName(name))
                directories[directory][name] = std::move(file);
        }
    }
    catch (const Exception &) // NOLINT(bugprone-empty-catch)
    {
        /// A truncated input attaches the files read before the end.
    }

    if (part_name.empty())
        return {};

    String script;
    if (input_table != table)
    {
        /// The table before the previous one is dropped by now, or it never attached.
        if (table_number >= 2)
            fs::remove_all(table_path(table_number - 2).second);
        table = input_table;
        ++table_number;
        script = fmt::format(
            "DROP TABLE IF EXISTS fuzz.t SYNC;ATTACH TABLE fuzz.t UUID '{}' {};SYSTEM STOP MERGES fuzz.t;",
            table_path(table_number).first, table);
    }
    else
    {
        /// The previous part is dropped first, because a failed query does not stop the next ones.
        script = "TRUNCATE TABLE fuzz.t;";
    }

    const fs::path detached = table_path(table_number).second / "detached";
    fs::remove_all(detached);

    const bool keep_checksums = flags & 1;
    const fs::path part_path = detached / part_name;
    auto part_checksums = writeFiles(part_path, directories[""]);
    for (const auto & [directory, files] : directories)
    {
        if (directory.empty())
            continue;
        auto checksums = writeFiles(part_path / directory, files);
        if (!keep_checksums || !files.contains("checksums.txt"))
            writeChecksums(part_path / directory, checksums);
        /// The checksums of a part list its projections too.
        part_checksums.addFile(directory, checksums.getTotalSizeOnDisk(), checksums.getTotalChecksumUInt128());
    }
    if (!keep_checksums || !directories[""].contains("checksums.txt"))
        writeChecksums(part_path, part_checksums);

    script += fmt::format("ALTER TABLE fuzz.t ATTACH PART '{}';", part_name);
    for (const auto & query : queries)
        script += query + ";";
    return script;
}
