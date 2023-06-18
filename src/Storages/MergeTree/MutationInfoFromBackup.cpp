#include <Storages/MergeTree/MutationInfoFromBackup.h>
#include <Backups/IBackup.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <filesystem>
#include <string_view>

#include <Common/logger_useful.h>

namespace fs = std::filesystem;


namespace DB
{

String MutationInfoFromBackup::toString(bool one_line) const
{
    WriteBufferFromOwnString out;
    write(out, one_line);
    return out.str();
}

MutationInfoFromBackup MutationInfoFromBackup::parseFromString(const String & str, const String & filename)
{
    ReadBufferFromString in(str);
    MutationInfoFromBackup res;
    res.read(in, filename);
    return res;
}

void MutationInfoFromBackup::write(WriteBuffer & out, bool one_line) const
{
    std::string_view eol = one_line ? " " : "\n";
    std::string_view tab = one_line ? " " : "\t";

    if (block_number)
        out << "block number: " << *block_number << eol;

    if (block_numbers)
    {
        out << "block numbers count: " << block_numbers->size() << eol;
        for (const auto & kv : *block_numbers)
        {
            const String & partition_id = kv.first;
            Int64 block_num = kv.second;
            out << partition_id << tab << block_num << eol;
        }
    }

    out << "commands: ";
    commands.writeText(out, /* with_pure_metadata_commands = */ false);
    out << eol;
}

void MutationInfoFromBackup::read(ReadBuffer & in, const String & filename)
{
    try
    {
        fs::path filename_fs = fs::path{filename};
        name = filename_fs.filename();

        const constexpr std::string_view mutation_prefix = "mutation_";
        String number_as_string = filename_fs.stem();
        if (number_as_string.starts_with(mutation_prefix))
            number_as_string = number_as_string.substr(mutation_prefix.length());
        number = ::DB::parseFromString<Int64>(number_as_string);

        auto skip_separators = [&] { ignoreCharsWhile<' ', '\t', '\n'>(in); };

        auto next_token = [&]
        {
            skip_separators();
            String tkn;
            readStringUntilCharsInto<' ', '\t', '\n'>(tkn, in);
            return tkn;
        };

        auto expect_token = [&](const char * expected)
        {
            skip_separators();
            assertString(expected, in);
        };

        expect_token("block");

        auto token = next_token();
        if (token == "number:")
        {
            /// block numbers:
            block_number = ::DB::parseFromString<Int64>(next_token());
        }
        else if (token == "numbers")
        {
            /// block numbers count:
            expect_token("count:");
            size_t count = ::DB::parseFromString<size_t>(next_token());
            block_numbers.emplace();
            for (size_t i = 0; i < count; ++i)
            {
                String partition_id = next_token();
                Int64 block_num = ::DB::parseFromString<Int64>(next_token());
                (*block_numbers)[partition_id] = block_num;
            }
        }
        else
        {
            throwAtAssertionFailed("'block number:' or 'block numbers count:'", in);
        }

        expect_token("commands:");

        skip_separators();
        commands.readText(in);
    }
    catch (Exception & e)
    {
        e.addMessage("While parsing mutation {}", filename);
        e.rethrow();
    }
}

std::vector<MutationInfoFromBackup> readMutationsFromBackup(const IBackup & backup, const String & mutations_dir_path_in_backup)
{
    fs::path dir = mutations_dir_path_in_backup;
    Strings filenames = backup.listFiles(dir);

    std::vector<MutationInfoFromBackup> res;
    res.reserve(filenames.size());

    for (const auto & filename : filenames)
    {
        auto mutation_path = dir /  filename;
        auto in = backup.readFile(mutation_path);
        MutationInfoFromBackup mutation_info;
        mutation_info.read(*in, mutation_path);
        res.emplace_back(std::move(mutation_info));
    }

    return res;
}

}
