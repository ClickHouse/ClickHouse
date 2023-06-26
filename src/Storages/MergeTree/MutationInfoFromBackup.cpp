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
    /// The function can read a mutation info from either the format of MutationInfoFromBackup::write()
    /// or MergeTreeMutationEntry::writeText() or ReplicatedMergeTreeMutationEntry::writeText().
    try
    {
        fs::path filename_fs = fs::path{filename};
        name = filename_fs.filename();

        const constexpr std::string_view mutation_prefix = "mutation_";
        String number_as_string = filename_fs.stem();
        if (number_as_string.starts_with(mutation_prefix))
            number_as_string = number_as_string.substr(mutation_prefix.length());
        number = ::DB::parseFromString<Int64>(number_as_string);

        auto skip_whitespace = [&] { ignoreCharsWhile<' ', '\t', '\n'>(in); };

        auto next_token = [&]
        {
            skip_whitespace();
            String tkn;
            readStringUntilCharsInto<' ', '\t', '\n'>(tkn, in);
            return tkn;
        };

        auto assert_token = [&](const char * expected)
        {
            skip_whitespace();
            assertString(expected, in);
        };

        auto token = next_token();
        if (token == "format")
        {
            /// format version: 1
            assert_token("version:");
            assert_token("1");

            token = next_token();
        }

        if (token == "create")
        {
            /// create time: *
            assert_token("time:");

            LocalDateTime create_time_dt;
            skip_whitespace();
            readText(create_time_dt, in);

            token = next_token();
        }

        if (token == "source")
        {
            /// source replica: *
            assert_token("replica:");

            String source_replica;
            skip_whitespace();
            readString(source_replica, in);

            token = next_token();
        }

        if (token == "block")
        {
            token = next_token();
            if (token == "number:")
            {
                /// block number: <num>
                Int64 num;
                skip_whitespace();
                readText(num, in);
                block_number = num;

                token = next_token();
            }
            else if (token == "numbers")
            {
                /// block numbers count: <count>
                /// <partition_id> <num>
                /// ...
                assert_token("count:");

                size_t count;
                skip_whitespace();
                readText(count, in);

                block_numbers.emplace();
                for (size_t i = 0; i < count; ++i)
                {
                    String partition_id = next_token();
                    Int64 num;
                    skip_whitespace();
                    readText(num, in);
                    (*block_numbers)[partition_id] = num;
                }

                token = next_token();
            }
            else
            {
                throwAtAssertionFailed("'block number:' or 'block numbers count:'", in);
            }
        }

        if (!block_number && !block_numbers)
            block_number = number;

        /// commands: *
        if (token != "commands:")
            throwAtAssertionFailed("commands:", in);

        skip_whitespace();
        commands.readText(in);
    }
    catch (Exception & e)
    {
        e.addMessage("While parsing mutation {}", filename);
        e.rethrow();
    }
}

namespace
{
    void appendMutationsFromBackup(const IBackup & backup, const String & dir_in_backup, const String & file_name_in_backup,
                                   std::vector<MutationInfoFromBackup> & result)
    {
        fs::path full_path = fs::path{dir_in_backup} / file_name_in_backup;
        if (file_name_in_backup == "mutations")
        {
            Strings names_in_subdir = backup.listFiles(full_path);
            for (const auto & name_in_subdir : names_in_subdir)
                appendMutationsFromBackup(backup, full_path, name_in_subdir, result);
        }
        else
        {
            auto in = backup.readFile(full_path);
            MutationInfoFromBackup mutation_info;
            mutation_info.read(*in, file_name_in_backup);
            result.emplace_back(std::move(mutation_info));
        }
    }
}

std::vector<MutationInfoFromBackup> readMutationsFromBackup(const IBackup & backup, const String & dir_in_backup, Strings & file_names_in_backup)
{
    if (file_names_in_backup.empty())
        return {};

    std::vector<MutationInfoFromBackup> res;
    for (const String & file_name : file_names_in_backup)
        appendMutationsFromBackup(backup, dir_in_backup, file_name, res);

    file_names_in_backup.resize(res.size());
    for (size_t i = 0; i != res.size(); ++i)
        file_names_in_backup[i] = res[i].name;

    return res;
}

}
