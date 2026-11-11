#include <ICommand.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteHelpers.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class CommandWordCount final : public ICommand
{
public:
    CommandWordCount()
        : ICommand("CommandWordCount")
    {
        command_name = "wc";
        description = "Count bytes, lines and words in a file (like `wc`)";
        options_description.add_options()("path", po::value<String>(), "file to count (mandatory, positional)")(
            "bytes,c", "print the byte count")("lines,l", "print the line count (number of newlines)")(
            "words,w", "print the word count (whitespace-delimited)");
        positional_options_description.add("path", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const auto & disk = client.getCurrentDiskWithPath();
        String path_arg = getValueFromCommandLineOptionsThrow<String>(options, "path");
        String path = disk.getRelativeFromRoot(path_arg);

        auto disk_ptr = disk.getDisk();
        if (!disk_ptr->existsFile(path))
        {
            if (disk_ptr->existsDirectory(path))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Path {} on disk {} is a directory, but wc must be called on a file.",
                    path_arg,
                    disk_ptr->getName());

            throw Exception(ErrorCodes::BAD_ARGUMENTS, "File {} on disk {} doesn't exist.", path_arg, disk_ptr->getName());
        }

        bool count_bytes = options.contains("bytes");
        bool count_lines = options.contains("lines");
        bool count_words = options.contains("words");
        /// With no flags, behave like Unix `wc` and print all three.
        if (!count_bytes && !count_lines && !count_words)
            count_bytes = count_lines = count_words = true;

        size_t bytes = 0;
        size_t lines = 0;
        size_t words = 0;
        bool in_word = false;

        auto in = disk_ptr->readFile(path, getReadSettings());
        /// Single pass over the buffered chunks read from the disk.
        while (!in->eof())
        {
            char c = *in->position();
            ++bytes;
            if (c == '\n')
                ++lines;
            if (isWhitespaceASCII(c))
                in_word = false;
            else if (!in_word)
            {
                ++words;
                in_word = true;
            }
            ++in->position();
        }

        WriteBufferFromFileDescriptor out(STDOUT_FILENO);
        /// Always print in the fixed Unix `wc` order: lines, words, bytes.
        bool first = true;
        auto write_count = [&](size_t value)
        {
            if (!first)
                out.write(' ');
            writeIntText(value, out);
            first = false;
        };
        if (count_lines)
            write_count(lines);
        if (count_words)
            write_count(words);
        if (count_bytes)
            write_count(bytes);
        out.write('\n');
        out.finalize();
    }
};

CommandPtr makeCommandWordCount()
{
    return std::make_shared<DB::CommandWordCount>();
}

}
