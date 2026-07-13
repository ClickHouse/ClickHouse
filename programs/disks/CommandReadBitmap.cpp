#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Common/TerminalSize.h>
#include <ICommand.h>

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNKNOWN_FORMAT_VERSION;
}

/// Inspect a delete-bitmap (`.rbm`) sidecar: print header + stats via the
/// non-throwing `inspectDeleteBitmap` (tolerant of corruption, unlike `deserialize`).
class CommandReadBitmap final : public ICommand
{
public:
    CommandReadBitmap() : ICommand("CommandReadBitmap")
    {
        command_name = "read-bitmap";
        description = "Inspect a delete-bitmap (.rbm) sidecar: print header + stats from `path-from`";
        options_description.add_options()(
            "path-from", po::value<String>(), "delete-bitmap (.rbm) file to inspect (mandatory, positional)")(
            "values", "dump all set bits (the deleted row offsets) in ascending order");
        positional_options_description.add("path-from", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const auto & disk = client.getCurrentDiskWithPath();
        String path_from = disk.getRelativeFromRoot(getValueFromCommandLineOptionsThrow<String>(options, "path-from"));

        if (!disk.getDisk()->existsFile(path_from))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                "delete-bitmap file {} does not exist on disk {}", path_from, disk.getDisk()->getName());

        const bool dump_values = options.contains("values");

        const UInt64 file_size = disk.getDisk()->getFileSize(path_from);
        auto in = disk.getDisk()->readFile(path_from, getReadSettings());

        auto info = inspectDeleteBitmap(*in, dump_values);

        auto & out = std::cout;
        out << "file:      " << path_from << " (" << file_size << " bytes)\n";

        if (!info.header_read)
        {
            out.flush();
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "not a delete-bitmap (.rbm) file (truncated: {} bytes, need at least a {}-byte header)",
                file_size, DeleteBitmap::HEADER_SIZE + DeleteBitmap::CRC_SIZE);
        }

        if (!info.magic_ok)
        {
            out << "magic:     BAD\n";
            out.flush();
            throw Exception(ErrorCodes::CORRUPTED_DATA, "not a delete-bitmap (.rbm) file (bad magic)");
        }
        out << "magic:     OK\n";

        out << "version:   " << info.version;
        if (info.version == DeleteBitmap::VERSION_R32)
            out << " (Roaring32)\n";
        else if (info.version == DeleteBitmap::VERSION_R64)
            out << " (Roaring64; body is host-endian, not cross-arch portable)\n";
        else
        {
            /// Unsupported version: the inspector stops before reading the body
            /// (mirrors deserialize), so there are no stats to print.
            out << " (UNSUPPORTED)\nbody_size: " << info.body_size << " bytes\n";
            out.flush();
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
                "unsupported delete-bitmap version {} (supported: {} or {})",
                info.version, DeleteBitmap::VERSION_R32, DeleteBitmap::VERSION_R64);
        }

        out << "body_size: " << info.body_size << " bytes";
        if (!info.body_read)
            out << " (TRUNCATED — fewer bytes on disk than declared)";
        out << "\n";

        out << "crc:       stored=" << fmt::format("{:#010x}", info.crc_stored)
            << " computed=" << fmt::format("{:#010x}", info.crc_computed) << " -> "
            << (info.crc_ok ? "VALID" : "CORRUPT") << "\n";

        if (info.decoded)
        {
            out << "cardinality: " << info.cardinality << " (deleted rows)\n";
            if (info.has_minmax)
                out << "row range:   [" << info.min_row << ", " << info.max_row << "]\n";
        }
        else
        {
            out << "cardinality: <undecodable> (roaring body did not parse";
            if (!info.decode_error.empty())
                out << ": " << info.decode_error;
            out << ")\n";
        }

        if (dump_values && info.decoded)
        {
            out << "values:";
            for (UInt64 v : info.sample)
                out << ' ' << v;
            out << "\n";
        }
        out.flush();
    }
};

CommandPtr makeCommandReadBitmap()
{
    return std::make_shared<DB::CommandReadBitmap>();
}

}
