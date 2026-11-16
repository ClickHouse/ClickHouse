#include <ICommand.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Common/logger_useful.h>
#include <base/hex.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandReadChecksums final : public ICommand
{
public:
    CommandReadChecksums() : ICommand("CommandReadChecksums")
    {
        command_name = "read-checksums";
        description = "Read a `checksums.txt` file of a MergeTree data part and print it as a human-readable table";
        options_description.add_options()
            ("path", po::value<String>(), "path to the `checksums.txt` file (mandatory, positional)");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const auto & disk = client.getCurrentDiskWithPath();
        String path_arg = getValueFromCommandLineOptionsThrow<String>(options, "path");
        String path = disk.getRelativeFromRoot(path_arg);

        auto disk_ptr = disk.getDisk();
        if (disk_ptr->existsDirectory(path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cannot read checksums of '{}': Is a directory", path_arg);
        if (!disk_ptr->existsFile(path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path {} on disk {} doesn't exist", path_arg, disk_ptr->getName());

        LOG_INFO(log, "Reading checksums from '{}' at disk '{}'", path_arg, disk_ptr->getName());

        auto in = disk_ptr->readFile(path, getReadSettings());
        MergeTreeDataPartChecksums checksums;

        assertString("checksums format version: ", *in);
        size_t format_version = 0;
        readText(format_version, *in);
        assertChar('\n', *in);

        if (!checksums.read(*in, format_version))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'{}' is not a valid checksums file or uses an unsupported (too old) format", path_arg);
        assertEOF(*in);

        WriteBufferFromFileDescriptor out(STDOUT_FILENO);
        writeString("name\tfile_size\tfile_hash\tuncompressed_size\tuncompressed_hash\n", out);
        /// The order of `files` is deterministic (lexicographical by name).
        for (const auto & [name, checksum] : checksums.files)
        {
            writeString(name, out);
            out.write('\t');
            writeIntText(checksum.file_size, out);
            out.write('\t');
            writeString(getHexUIntLowercase(checksum.file_hash), out);
            if (checksum.is_compressed)
            {
                out.write('\t');
                writeIntText(checksum.uncompressed_size, out);
                out.write('\t');
                writeString(getHexUIntLowercase(checksum.uncompressed_hash), out);
            }
            out.write('\n');
        }
        out.finalize();
    }
};

CommandPtr makeCommandReadChecksums()
{
    return std::make_shared<DB::CommandReadChecksums>();
}

}
