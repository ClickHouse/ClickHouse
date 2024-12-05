#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <unordered_set>
#include <unordered_map>
#include <list>
#include <thread>
#include <filesystem>

#include <boost/program_options.hpp>

#include <Common/TerminalSize.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/ShellCommand.h>
#include <Common/re2.h>
#include <base/find_symbols.h>

#include <IO/copyData.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>

static constexpr auto documentation = R"(
A tool to extract information from Git repository for analytics.

It dumps the data for the following tables:
- commits - commits with statistics;
- file_changes - files changed in every commit with the info about the change and statistics;
- line_changes - every changed line in every changed file in every commit with full info about the line and the information about previous change of this line.

The largest and the most important table is "line_changes".

Allows to answer questions like:
- list files with maximum number of authors;
- show me the oldest lines of code in the repository;
- show me the files with longest history;
- list favorite files for author;
- list largest files with lowest number of authors;
- at what weekday the code has highest chance to stay in repository;
- the distribution of code age across repository;
- files sorted by average code age;
- quickly show file with blame info (rough);
- commits and lines of code distribution by time; by weekday, by author; for specific subdirectories;
- show history for every subdirectory, file, line of file, the number of changes (lines and commits) across time; how the number of contributors was changed across time;
- list files with most modifications;
- list files that were rewritten most number of time or by most of authors;
- what is percentage of code removal by other authors, across authors;
- the matrix of authors that shows what authors tends to rewrite another authors code;
- what is the worst time to write code in sense that the code has highest chance to be rewritten;
- the average time before code will be rewritten and the median (half-life of code decay);
- comments/code percentage change in time / by author / by location;
- who tend to write more tests / cpp code / comments.

The data is intended for analytical purposes. It can be imprecise by many reasons but it should be good enough for its purpose.

The data is not intended to provide any conclusions for managers, it is especially counter-indicative for any kinds of "performance review". Instead you can spend multiple days looking at various interesting statistics.

Run this tool inside your git repository. It will create .tsv files that can be loaded into ClickHouse (or into other DBMS if you dare).

The tool can process large enough repositories in a reasonable time.
It has been tested on:
- ClickHouse: 31 seconds; 3 million rows;
- LLVM: 8 minutes; 62 million rows;
- Linux - 12 minutes; 85 million rows;
- Chromium - 67 minutes; 343 million rows;
(the numbers as of Sep 2020)


Prepare the database by executing the following queries:

DROP DATABASE IF EXISTS git;
CREATE DATABASE git;

CREATE TABLE git.commits
(
    hash String,
    author LowCardinality(String),
    time DateTime,
    message String,
    files_added UInt32,
    files_deleted UInt32,
    files_renamed UInt32,
    files_modified UInt32,
    lines_added UInt32,
    lines_deleted UInt32,
    hunks_added UInt32,
    hunks_removed UInt32,
    hunks_changed UInt32
) ENGINE = MergeTree ORDER BY time;

CREATE TABLE git.file_changes
(
    change_type Enum('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
    path LowCardinality(String),
    old_path LowCardinality(String),
    file_extension LowCardinality(String),
    lines_added UInt32,
    lines_deleted UInt32,
    hunks_added UInt32,
    hunks_removed UInt32,
    hunks_changed UInt32,

    commit_hash String,
    author LowCardinality(String),
    time DateTime,
    commit_message String,
    commit_files_added UInt32,
    commit_files_deleted UInt32,
    commit_files_renamed UInt32,
    commit_files_modified UInt32,
    commit_lines_added UInt32,
    commit_lines_deleted UInt32,
    commit_hunks_added UInt32,
    commit_hunks_removed UInt32,
    commit_hunks_changed UInt32
) ENGINE = MergeTree ORDER BY time;

CREATE TABLE git.line_changes
(
    sign Int8,
    line_number_old UInt32,
    line_number_new UInt32,
    hunk_num UInt32,
    hunk_start_line_number_old UInt32,
    hunk_start_line_number_new UInt32,
    hunk_lines_added UInt32,
    hunk_lines_deleted UInt32,
    hunk_context LowCardinality(String),
    line LowCardinality(String),
    indent UInt8,
    line_type Enum('Empty' = 0, 'Comment' = 1, 'Punct' = 2, 'Code' = 3),

    prev_commit_hash String,
    prev_author LowCardinality(String),
    prev_time DateTime,

    file_change_type Enum('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
    path LowCardinality(String),
    old_path LowCardinality(String),
    file_extension LowCardinality(String),
    file_lines_added UInt32,
    file_lines_deleted UInt32,
    file_hunks_added UInt32,
    file_hunks_removed UInt32,
    file_hunks_changed UInt32,

    commit_hash String,
    author LowCardinality(String),
    time DateTime,
    commit_message String,
    commit_files_added UInt32,
    commit_files_deleted UInt32,
    commit_files_renamed UInt32,
    commit_files_modified UInt32,
    commit_lines_added UInt32,
    commit_lines_deleted UInt32,
    commit_hunks_added UInt32,
    commit_hunks_removed UInt32,
    commit_hunks_changed UInt32
) ENGINE = MergeTree ORDER BY time;

Run the tool.

Then insert the data with the following commands:

clickhouse-client --query "INSERT INTO git.commits FORMAT TSV" < commits.tsv
clickhouse-client --query "INSERT INTO git.file_changes FORMAT TSV" < file_changes.tsv
clickhouse-client --query "INSERT INTO git.line_changes FORMAT TSV" < line_changes.tsv

Check out this presentation: https://presentations.clickhouse.com/matemarketing_2020/
)";

namespace po = boost::program_options;

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


struct Commit
{
    std::string hash;
    std::string author;
    LocalDateTime time{};
    std::string message;
    uint32_t files_added{};
    uint32_t files_deleted{};
    uint32_t files_renamed{};
    uint32_t files_modified{};
    uint32_t lines_added{};
    uint32_t lines_deleted{};
    uint32_t hunks_added{};
    uint32_t hunks_removed{};
    uint32_t hunks_changed{};

    void writeTextWithoutNewline(WriteBuffer & out) const
    {
        writeEscapedString(hash, out);
        writeChar('\t', out);
        writeEscapedString(author, out);
        writeChar('\t', out);
        writeText(time, out);
        writeChar('\t', out);
        writeEscapedString(message, out);
        writeChar('\t', out);
        writeText(files_added, out);
        writeChar('\t', out);
        writeText(files_deleted, out);
        writeChar('\t', out);
        writeText(files_renamed, out);
        writeChar('\t', out);
        writeText(files_modified, out);
        writeChar('\t', out);
        writeText(lines_added, out);
        writeChar('\t', out);
        writeText(lines_deleted, out);
        writeChar('\t', out);
        writeText(hunks_added, out);
        writeChar('\t', out);
        writeText(hunks_removed, out);
        writeChar('\t', out);
        writeText(hunks_changed, out);
    }
};


enum class FileChangeType : uint8_t
{
    Add,
    Delete,
    Modify,
    Rename,
    Copy,
    Type,
};

void writeText(FileChangeType type, WriteBuffer & out)
{
    switch (type)
    {
        case FileChangeType::Add: writeString("Add", out); break;
        case FileChangeType::Delete: writeString("Delete", out); break;
        case FileChangeType::Modify: writeString("Modify", out); break;
        case FileChangeType::Rename: writeString("Rename", out); break;
        case FileChangeType::Copy: writeString("Copy", out); break;
        case FileChangeType::Type: writeString("Type", out); break;
    }
}

struct FileChange
{
    FileChangeType change_type{};
    std::string path;
    std::string old_path;
    std::string file_extension;
    uint32_t lines_added{};
    uint32_t lines_deleted{};
    uint32_t hunks_added{};
    uint32_t hunks_removed{};
    uint32_t hunks_changed{};

    void writeTextWithoutNewline(WriteBuffer & out) const
    {
        writeText(change_type, out);
        writeChar('\t', out);
        writeEscapedString(path, out);
        writeChar('\t', out);
        writeEscapedString(old_path, out);
        writeChar('\t', out);
        writeEscapedString(file_extension, out);
        writeChar('\t', out);
        writeText(lines_added, out);
        writeChar('\t', out);
        writeText(lines_deleted, out);
        writeChar('\t', out);
        writeText(hunks_added, out);
        writeChar('\t', out);
        writeText(hunks_removed, out);
        writeChar('\t', out);
        writeText(hunks_changed, out);
    }
};


enum class LineType : uint8_t
{
    Empty,
    Comment,
    Punct,
    Code,
};

void writeText(LineType type, WriteBuffer & out)
{
    switch (type)
    {
        case LineType::Empty: writeString("Empty", out); break;
        case LineType::Comment: writeString("Comment", out); break;
        case LineType::Punct: writeString("Punct", out); break;
        case LineType::Code: writeString("Code", out); break;
    }
}

struct LineChange
{
    int8_t sign{}; /// 1 if added, -1 if deleted
    uint32_t line_number_old{};
    uint32_t line_number_new{};
    uint32_t hunk_num{}; /// ordinal number of hunk in diff, starting with 0
    uint32_t hunk_start_line_number_old{};
    uint32_t hunk_start_line_number_new{};
    uint32_t hunk_lines_added{};
    uint32_t hunk_lines_deleted{};
    std::string hunk_context; /// The context (like a line with function name) as it is calculated by git
    std::string line; /// Line content without leading whitespaces
    uint8_t indent{}; /// The number of leading whitespaces or tabs * 4
    LineType line_type{};
    /// Information from the history (blame).
    std::string prev_commit_hash;
    std::string prev_author;
    LocalDateTime prev_time{};

    /** Classify line to empty / code / comment / single punctuation char.
      * Very rough and mostly suitable for our C++ style.
      */
    void setLineInfo(std::string full_line)
    {
        uint32_t num_spaces = 0;

        const char * pos = full_line.data();
        const char * end = pos + full_line.size();

        while (pos < end)
        {
            if (*pos == ' ')
                ++num_spaces;
            else if (*pos == '\t')
                num_spaces += 4;
            else
                break;
            ++pos;
        }

        indent = std::min(255U, num_spaces);
        line.assign(pos, end);

        if (pos == end)
        {
            line_type = LineType::Empty;
        }
        else if (pos + 1 < end
            && ((pos[0] == '/' && (pos[1] == '/' || pos[1] == '*'))
                || (pos[0] == '*' && pos[1] == ' ')     /// This is not precise.
                || (pos[0] == '#' && pos[1] == ' ')))
        {
            line_type = LineType::Comment;
        }
        else
        {
            while (pos < end)
            {
                if (isAlphaNumericASCII(*pos))
                {
                    line_type = LineType::Code;
                    break;
                }
                ++pos;
            }
            if (pos == end)
                line_type = LineType::Punct;
        }
    }

    void writeTextWithoutNewline(WriteBuffer & out) const
    {
        writeText(sign, out);
        writeChar('\t', out);
        writeText(line_number_old, out);
        writeChar('\t', out);
        writeText(line_number_new, out);
        writeChar('\t', out);
        writeText(hunk_num, out);
        writeChar('\t', out);
        writeText(hunk_start_line_number_old, out);
        writeChar('\t', out);
        writeText(hunk_start_line_number_new, out);
        writeChar('\t', out);
        writeText(hunk_lines_added, out);
        writeChar('\t', out);
        writeText(hunk_lines_deleted, out);
        writeChar('\t', out);
        writeEscapedString(hunk_context, out);
        writeChar('\t', out);
        writeEscapedString(line, out);
        writeChar('\t', out);
        writeText(indent, out);
        writeChar('\t', out);
        writeText(line_type, out);
        writeChar('\t', out);
        writeEscapedString(prev_commit_hash, out);
        writeChar('\t', out);
        writeEscapedString(prev_author, out);
        writeChar('\t', out);
        writeText(prev_time, out);
    }
};

using LineChanges = std::vector<LineChange>;

struct FileDiff
{
    explicit FileDiff(FileChange file_change_) : file_change(file_change_) {}

    FileChange file_change;
    LineChanges line_changes;
};

using CommitDiff = std::map<std::string /* path */, FileDiff>;


/** Parsing helpers */

void skipUntilWhitespace(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\t', '\n', ' '>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\t' || *buf.position() == '\n' || *buf.position() == ' ')
            return;
    }
}

void skipUntilNextLine(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}

void readStringUntilNextLine(std::string & s, ReadBuffer & buf)
{
    s.clear();
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n'>(buf.position(), buf.buffer().end());
        s.append(buf.position(), next_pos - buf.position());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}


/** Writes the resulting tables to files that can be imported to ClickHouse.
  */
struct ResultWriter
{
    WriteBufferFromFile commits{"commits.tsv"};
    WriteBufferFromFile file_changes{"file_changes.tsv"};
    WriteBufferFromFile line_changes{"line_changes.tsv"};

    void appendCommit(const Commit & commit, const CommitDiff & files)
    {
        /// commits table
        {
            auto & out = commits;

            commit.writeTextWithoutNewline(out);
            writeChar('\n', out);
        }

        for (const auto & elem : files)
        {
            const FileChange & file_change = elem.second.file_change;

            /// file_changes table
            {
                auto & out = file_changes;

                file_change.writeTextWithoutNewline(out);
                writeChar('\t', out);
                commit.writeTextWithoutNewline(out);
                writeChar('\n', out);
            }

            /// line_changes table
            for (const auto & line_change : elem.second.line_changes)
            {
                auto & out = line_changes;

                line_change.writeTextWithoutNewline(out);
                writeChar('\t', out);
                file_change.writeTextWithoutNewline(out);
                writeChar('\t', out);
                commit.writeTextWithoutNewline(out);
                writeChar('\n', out);
            }
        }
    }
};


/** See description in "main".
  */
struct Options
{
    bool skip_commits_without_parents = true;
    bool skip_commits_with_duplicate_diffs = true;
    size_t threads = 1;
    std::optional<re2::RE2> skip_paths;
    std::optional<re2::RE2> skip_commits_with_messages;
    std::unordered_set<std::string> skip_commits;
    std::optional<size_t> diff_size_limit;
    std::string stop_after_commit;

    explicit Options(const po::variables_map & options)
    {
        skip_commits_without_parents = options["skip-commits-without-parents"].as<bool>();
        skip_commits_with_duplicate_diffs = options["skip-commits-with-duplicate-diffs"].as<bool>();
        threads = options["threads"].as<size_t>();
        if (options.count("skip-paths"))
        {
            skip_paths.emplace(options["skip-paths"].as<std::string>());
        }
        if (options.count("skip-commits-with-messages"))
        {
            skip_commits_with_messages.emplace(options["skip-commits-with-messages"].as<std::string>());
        }
        if (options.count("skip-commit"))
        {
            auto vec = options["skip-commit"].as<std::vector<std::string>>();
            skip_commits.insert(vec.begin(), vec.end());
        }
        if (options.count("diff-size-limit"))
        {
            diff_size_limit = options["diff-size-limit"].as<size_t>();
        }
        if (options.count("stop-after-commit"))
        {
            stop_after_commit = options["stop-after-commit"].as<std::string>();
        }
    }
};


/** Rough snapshot of repository calculated by application of diffs. It's used to calculate blame info.
  * Represented by a list of lines. For every line it contains information about commit that modified this line the last time.
  *
  * Note that there are many cases when this info may become incorrect.
  * The first reason is that git history is non-linear but we form this snapshot by application of commit diffs in some order
  *  that cannot give us correct results even theoretically.
  * The second reason is that we don't process merge commits. But merge commits may contain differences for conflict resolution.
  *
  * We expect that the information will be mostly correct for the purpose of analytics.
  * So, it can provide the expected "blame" info for the most of the lines.
  */
struct FileBlame
{
    using Lines = std::list<Commit>;
    Lines lines;

    /// We walk through this list adding or removing lines.
    Lines::iterator it;
    size_t current_idx = 1;

    FileBlame()
    {
        it = lines.begin();
    }

    /// This is important when file was copied or renamed.
    FileBlame & operator=(const FileBlame & rhs)
    {
        lines = rhs.lines;
        it = lines.begin();
        current_idx = 1;
        return *this;
    }

    FileBlame(const FileBlame & rhs)
    {
        *this = rhs;
    }

    /// Move iterator to requested line or stop at the end.
    void walk(uint32_t num)
    {
        while (current_idx < num && it != lines.end())
        {
            ++current_idx;
            ++it;
        }
        while (current_idx > num)
        {
            --current_idx;
            --it;
        }
    }

    const Commit * find(uint32_t num)
    {
        walk(num);

//        std::cerr << "current_idx: " << current_idx << ", num: " << num << "\n";

        if (current_idx == num && it != lines.end())
            return &*it;
        return {};
    }

    void addLine(uint32_t num, Commit commit)
    {
        walk(num);

        /// If the inserted line is over the end of file, we insert empty lines before it.
        while (it == lines.end() && current_idx < num)
        {
            lines.emplace_back();
            ++current_idx;
        }

        it = lines.insert(it, commit);
    }

    void removeLine(uint32_t num)
    {
//        std::cerr << "Removing line " << num << ", current_idx: " << current_idx << "\n";

        walk(num);

        if (current_idx == num && it != lines.end())
            it = lines.erase(it);
    }
};

/// All files with their blame info. When file is renamed, we also rename it in snapshot.
using Snapshot = std::map<std::string /* path */, FileBlame>;


/** Enrich the line changes data with the history info from the snapshot
  * - the author, time and commit of the previous change to every found line (blame).
  * And update the snapshot.
  */
void updateSnapshot(Snapshot & snapshot, const Commit & commit, CommitDiff & file_changes)
{
    /// Renames and copies.
    for (auto & elem : file_changes)
    {
        auto & file = elem.second.file_change;
        if (!file.old_path.empty() && file.path != file.old_path)
            snapshot[file.path] = snapshot[file.old_path];
    }

    for (auto & elem : file_changes)
    {
//        std::cerr << elem.first << "\n";

        FileBlame & file_snapshot = snapshot[elem.first];
        std::unordered_map<uint32_t, Commit> deleted_lines;

        /// Obtain blame info from previous state of the snapshot

        for (auto & line_change : elem.second.line_changes)
        {
            if (line_change.sign == -1)
            {
                if (const Commit * prev_commit = file_snapshot.find(line_change.line_number_old);
                    prev_commit && prev_commit->time <= commit.time)
                {
                    line_change.prev_commit_hash = prev_commit->hash;
                    line_change.prev_author = prev_commit->author;
                    line_change.prev_time = prev_commit->time;
                    deleted_lines[line_change.line_number_old] = *prev_commit;
                }
                else
                {
                    // std::cerr << "Did not find line " << line_change.line_number_old << " from file " << elem.first << ": " << line_change.line << "\n";
                }
            }
            else if (line_change.sign == 1)
            {
                uint32_t this_line_in_prev_commit = line_change.hunk_start_line_number_old
                    + (line_change.line_number_new - line_change.hunk_start_line_number_new);

                if (deleted_lines.contains(this_line_in_prev_commit))
                {
                    const auto & prev_commit = deleted_lines[this_line_in_prev_commit];
                    if (prev_commit.time <= commit.time)
                    {
                        line_change.prev_commit_hash = prev_commit.hash;
                        line_change.prev_author = prev_commit.author;
                        line_change.prev_time = prev_commit.time;
                    }
                }
            }
        }

        /// Update the snapshot

        for (const auto & line_change : elem.second.line_changes)
        {
            if (line_change.sign == -1)
            {
                file_snapshot.removeLine(line_change.line_number_new);
            }
            else if (line_change.sign == 1)
            {
                file_snapshot.addLine(line_change.line_number_new, commit);
            }
        }
    }
}


/** Deduplication of commits with identical diffs.
  */
using DiffHashes = std::unordered_set<UInt128>;

UInt128 diffHash(const CommitDiff & file_changes)
{
    SipHash hasher;

    for (const auto & elem : file_changes)
    {
        hasher.update(elem.second.file_change.change_type);
        hasher.update(elem.second.file_change.old_path.size());
        hasher.update(elem.second.file_change.old_path);
        hasher.update(elem.second.file_change.path.size());
        hasher.update(elem.second.file_change.path);

        hasher.update(elem.second.line_changes.size());
        for (const auto & line_change : elem.second.line_changes)
        {
            hasher.update(line_change.sign);
            hasher.update(line_change.line_number_old);
            hasher.update(line_change.line_number_new);
            hasher.update(line_change.indent);
            hasher.update(line_change.line.size());
            hasher.update(line_change.line);
        }
    }

    UInt128 hash_of_diff;
    hasher.get128(hash_of_diff.items[0], hash_of_diff.items[1]);

    return hash_of_diff;
}


/** File changes in form
  * :100644 100644 b90fe6bb94 3ffe4c380f M  src/Storages/MergeTree/MergeTreeDataMergerMutator.cpp
  * :100644 100644 828dedf6b5 828dedf6b5 R100       dbms/src/Functions/GeoUtils.h   dbms/src/Functions/PolygonUtils.h
  * according to the output of 'git show --raw'
  */
void processFileChanges(
    ReadBuffer & in,
    const Options & options,
    Commit & commit,
    CommitDiff & file_changes)
{
    while (checkChar(':', in))
    {
        FileChange file_change;

        /// We don't care about file mode and content hashes.
        for (size_t i = 0; i < 4; ++i)
        {
            skipUntilWhitespace(in);
            skipWhitespaceIfAny(in);
        }

        char change_type;
        readChar(change_type, in);

        /// For rename and copy there is a number called "score". We ignore it.
        int score;

        switch (change_type)
        {
            case 'A':
                file_change.change_type = FileChangeType::Add;
                ++commit.files_added;
                break;
            case 'D':
                file_change.change_type = FileChangeType::Delete;
                ++commit.files_deleted;
                break;
            case 'M':
                file_change.change_type = FileChangeType::Modify;
                ++commit.files_modified;
                break;
            case 'R':
                file_change.change_type = FileChangeType::Rename;
                ++commit.files_renamed;
                readText(score, in);
                break;
            case 'C':
                file_change.change_type = FileChangeType::Copy;
                readText(score, in);
                break;
            case 'T':
                file_change.change_type = FileChangeType::Type;
                break;
            default:
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected file change type: {}", change_type);
        }

        skipWhitespaceIfAny(in);

        if (change_type == 'R' || change_type == 'C')
        {
            readText(file_change.old_path, in);
            skipWhitespaceIfAny(in);
            readText(file_change.path, in);
        }
        else
        {
            readText(file_change.path, in);
        }

        file_change.file_extension = std::filesystem::path(file_change.path).extension();
        /// It gives us extension in form of '.cpp'. There is a reason for it but we remove initial dot for simplicity.
        if (!file_change.file_extension.empty() && file_change.file_extension.front() == '.')
            file_change.file_extension = file_change.file_extension.substr(1, std::string::npos);

        assertChar('\n', in);

        if (!(options.skip_paths && re2::RE2::PartialMatch(file_change.path, *options.skip_paths)))
        {
            file_changes.emplace(
                file_change.path,
                FileDiff(file_change));
        }
    }
}


/** Process the list of diffs for every file from the result of "git show".
  * Caveats:
  * - changes in binary files can be ignored;
  * - if a line content begins with '+' or '-' it will be skipped
  *   it means that if you store diffs in repository and "git show" will display diff-of-diff for you,
  *   it won't be processed correctly;
  * - we expect some specific format of the diff; but it may actually depend on git config;
  * - non-ASCII file names are not processed correctly (they will not be found and will be ignored).
  */
void processDiffs(
    ReadBuffer & in,
    std::optional<size_t> size_limit,
    Commit & commit,
    CommitDiff & file_changes)
{
    std::string old_file_path;
    std::string new_file_path;
    FileDiff * file_change_and_line_changes = nullptr;
    LineChange line_change;

    /// Diffs for every file in form of
    /// --- a/src/Storages/StorageReplicatedMergeTree.cpp
    /// +++ b/src/Storages/StorageReplicatedMergeTree.cpp
    /// @@ -1387,2 +1387 @@ bool StorageReplicatedMergeTree::tryExecuteMerge(const LogEntry & entry)
    /// -            table_lock, entry.create_time, reserved_space, entry.deduplicate,
    /// -            entry.force_ttl);
    /// +            table_lock, entry.create_time, reserved_space, entry.deduplicate);

    size_t diff_size = 0;
    while (!in.eof())
    {
        if (checkString("@@ ", in))
        {
            if (!file_change_and_line_changes)
            {
                auto file_name = new_file_path.empty() ? old_file_path : new_file_path;
                auto it = file_changes.find(file_name);
                if (file_changes.end() != it)
                    file_change_and_line_changes = &it->second;
            }

            if (file_change_and_line_changes)
            {
                uint32_t old_lines = 1;
                uint32_t new_lines = 1;

                assertChar('-', in);
                readText(line_change.hunk_start_line_number_old, in);
                if (checkChar(',', in))
                    readText(old_lines, in);

                assertString(" +", in);
                readText(line_change.hunk_start_line_number_new, in);
                if (checkChar(',', in))
                    readText(new_lines, in);

                /// This is needed to simplify the logic of updating snapshot:
                /// When all lines are removed we can treat it as repeated removal of line with number 1.
                if (line_change.hunk_start_line_number_new == 0)
                    line_change.hunk_start_line_number_new = 1;

                assertString(" @@", in);
                if (checkChar(' ', in))
                    readStringUntilNextLine(line_change.hunk_context, in);
                else
                    assertChar('\n', in);

                line_change.hunk_lines_added = new_lines;
                line_change.hunk_lines_deleted = old_lines;

                ++line_change.hunk_num;
                line_change.line_number_old = line_change.hunk_start_line_number_old;
                line_change.line_number_new = line_change.hunk_start_line_number_new;

                if (old_lines && new_lines)
                {
                    ++commit.hunks_changed;
                    ++file_change_and_line_changes->file_change.hunks_changed;
                }
                else if (old_lines)
                {
                    ++commit.hunks_removed;
                    ++file_change_and_line_changes->file_change.hunks_removed;
                }
                else if (new_lines)
                {
                    ++commit.hunks_added;
                    ++file_change_and_line_changes->file_change.hunks_added;
                }
            }
        }
        else if (checkChar('-', in))
        {
            if (checkString("-- ", in))
            {
                if (checkString("a/", in))
                {
                    readStringUntilNextLine(old_file_path, in);
                    line_change = LineChange{};
                    file_change_and_line_changes = nullptr;
                }
                else if (checkString("/dev/null", in))
                {
                    old_file_path.clear();
                    assertChar('\n', in);
                    line_change = LineChange{};
                    file_change_and_line_changes = nullptr;
                }
                else
                    skipUntilNextLine(in); /// Actually it can be the line in diff. Skip it for simplicity.
            }
            else
            {
                ++diff_size;
                if (file_change_and_line_changes)
                {
                    ++commit.lines_deleted;
                    ++file_change_and_line_changes->file_change.lines_deleted;

                    line_change.sign = -1;
                    readStringUntilNextLine(line_change.line, in);
                    line_change.setLineInfo(line_change.line);

                    file_change_and_line_changes->line_changes.push_back(line_change);
                    ++line_change.line_number_old;
                }
            }
        }
        else if (checkChar('+', in))
        {
            if (checkString("++ ", in))
            {
                if (checkString("b/", in))
                {
                    readStringUntilNextLine(new_file_path, in);
                    line_change = LineChange{};
                    file_change_and_line_changes = nullptr;
                }
                else if (checkString("/dev/null", in))
                {
                    new_file_path.clear();
                    assertChar('\n', in);
                    line_change = LineChange{};
                    file_change_and_line_changes = nullptr;
                }
                else
                    skipUntilNextLine(in); /// Actually it can be the line in diff. Skip it for simplicity.
            }
            else
            {
                ++diff_size;
                if (file_change_and_line_changes)
                {
                    ++commit.lines_added;
                    ++file_change_and_line_changes->file_change.lines_added;

                    line_change.sign = 1;
                    readStringUntilNextLine(line_change.line, in);
                    line_change.setLineInfo(line_change.line);

                    file_change_and_line_changes->line_changes.push_back(line_change);
                    ++line_change.line_number_new;
                }
            }
        }
        else
        {
            /// Unknown lines are ignored.
            skipUntilNextLine(in);
        }

        if (size_limit && diff_size > *size_limit)
        {
            return;
        }
    }
}


/** Process the "git show" result for a single commit. Append the result to tables.
  */
void processCommit(
    ReadBuffer & in,
    const Options & options,
    size_t commit_num,
    size_t total_commits,
    std::string hash,
    Snapshot & snapshot,
    DiffHashes & diff_hashes,
    ResultWriter & result)
{
    Commit commit;
    commit.hash = hash;

    time_t commit_time;
    readText(commit_time, in);
    commit.time = LocalDateTime(commit_time);
    assertChar('\0', in);
    readNullTerminated(commit.author, in);
    std::string parent_hash;
    readNullTerminated(parent_hash, in);
    readNullTerminated(commit.message, in);

    if (options.skip_commits_with_messages && re2::RE2::PartialMatch(commit.message, *options.skip_commits_with_messages))
        return;

    std::string message_to_print = commit.message;
    std::replace_if(message_to_print.begin(), message_to_print.end(), [](char c){ return std::iscntrl(c); }, ' ');

    std::cerr << fmt::format("{}%  {}  {}  {}\n",
        commit_num * 100 / total_commits, toString(commit.time), hash, message_to_print);

    if (options.skip_commits_without_parents && commit_num != 0 && parent_hash.empty())
    {
        std::cerr << "Warning: skipping commit without parents\n";
        return;
    }

    if (!in.eof())
        assertChar('\n', in);

    CommitDiff file_changes;
    processFileChanges(in, options, commit, file_changes);

    if (!in.eof())
    {
        assertChar('\n', in);
        processDiffs(in, commit_num != 0 ? options.diff_size_limit : std::nullopt, commit, file_changes);
    }

    /// Skip commits with too large diffs.
    if (options.diff_size_limit && commit_num != 0 && commit.lines_added + commit.lines_deleted > *options.diff_size_limit)
        return;

    /// Calculate hash of diff and skip duplicates
    if (options.skip_commits_with_duplicate_diffs && !diff_hashes.insert(diffHash(file_changes)).second)
        return;

    /// Update snapshot and blame info
    updateSnapshot(snapshot, commit, file_changes);

    /// Write the result
    result.appendCommit(commit, file_changes);
}


/** Runs child process and allows to read the result.
  * Multiple processes can be run for parallel processing.
  */
auto gitShow(const std::string & hash)
{
    std::string command = fmt::format(
        "git show --raw --pretty='format:%ct%x00%aN%x00%P%x00%s%x00' --patch --unified=0 {}",
        hash);

    return ShellCommand::execute(command);
}


/** Obtain the list of commits and process them.
  */
void processLog(const Options & options)
{
    ResultWriter result;

    std::string command = "git log --reverse --no-merges --pretty=%H";
    fmt::print("{}\n", command);
    auto git_log = ShellCommand::execute(command);

    /// Collect hashes in memory. This is inefficient but allows to display beautiful progress.
    /// The number of commits is in order of single millions for the largest repositories,
    /// so don't care about potential waste of ~100 MB of memory.

    std::vector<std::string> hashes;

    auto & in = git_log->out;
    while (!in.eof())
    {
        std::string hash;
        readString(hash, in);
        assertChar('\n', in);

        if (!options.skip_commits.contains(hash))
            hashes.emplace_back(std::move(hash));
    }

    size_t num_commits = hashes.size();
    fmt::print("Total {} commits to process.\n", num_commits);

    /// Will run multiple processes in parallel
    size_t num_threads = options.threads;
    if (num_threads == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "num-threads cannot be zero");

    std::vector<std::unique_ptr<ShellCommand>> show_commands(num_threads);
    for (size_t i = 0; i < num_commits && i < num_threads; ++i)
        show_commands[i] = gitShow(hashes[i]);

    Snapshot snapshot;
    DiffHashes diff_hashes;

    for (size_t i = 0; i < num_commits; ++i)
    {
        processCommit(show_commands[i % num_threads]->out, options, i, num_commits, hashes[i], snapshot, diff_hashes, result);

        if (!options.stop_after_commit.empty() && hashes[i] == options.stop_after_commit)
            break;

        if (i + num_threads < num_commits)
            show_commands[i % num_threads] = gitShow(hashes[i + num_threads]);
    }
}


}

int mainEntryClickHouseGitImport(int argc, char ** argv)
try
{
    using namespace DB;

    po::options_description desc("Allowed options", getTerminalWidth());
    desc.add_options()
        ("help,h", "produce help message")
        ("skip-commits-without-parents", po::value<bool>()->default_value(true),
            "Skip commits without parents (except the initial commit)."
            " These commits are usually erroneous but they can make sense in very rare cases.")
        ("skip-commits-with-duplicate-diffs", po::value<bool>()->default_value(true),
            "Skip commits with duplicate diffs."
            " These commits are usually results of cherry-pick or merge after rebase.")
        ("skip-commit", po::value<std::vector<std::string>>(),
            "Skip commit with specified hash. The option can be specified multiple times.")
        ("skip-paths", po::value<std::string>(),
            "Skip paths that matches regular expression (re2 syntax).")
        ("skip-commits-with-messages", po::value<std::string>(),
            "Skip commits whose messages matches regular expression (re2 syntax).")
        ("diff-size-limit", po::value<size_t>()->default_value(100000),
            "Skip commits whose diff size (number of added + removed lines) is larger than specified threshold. Does not apply for initial commit.")
        ("stop-after-commit", po::value<std::string>(),
            "Stop processing after specified commit hash.")
        ("threads", po::value<size_t>()->default_value(std::thread::hardware_concurrency()),
            "Number of concurrent git subprocesses to spawn")
    ;

    po::variables_map options;
    po::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << documentation << '\n'
            << "Usage: " << argv[0] << '\n'
            << desc << '\n'
            << "\nExample:\n"
            << "\nclickhouse git-import --skip-paths 'generated\\.cpp|^(contrib|docs?|website|libs/(libcityhash|liblz4|libdivide|libvectorclass|libdouble-conversion|libcpuid|libzstd|libfarmhash|libmetrohash|libpoco|libwidechar_width))/' --skip-commits-with-messages '^Merge branch '\n";
        return 1;
    }

    processLog(Options(options));
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    return DB::getCurrentExceptionCode();
}
