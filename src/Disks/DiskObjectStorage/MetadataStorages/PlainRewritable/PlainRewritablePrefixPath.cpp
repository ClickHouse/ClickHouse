#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritablePrefixPath.h>

#include <Common/Exception.h>

#include <algorithm>
#include <charconv>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

void trimInPlace(std::string_view & value)
{
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t' || value.front() == '\r'))
        value.remove_prefix(1);
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t' || value.back() == '\r'))
        value.remove_suffix(1);
}

std::string normalizeLogicalPath(std::string_view path)
{
    trimInPlace(path);
    if (path.empty())
        return "/";

    std::string result(path);
    if (!result.ends_with('/'))
        result.push_back('/');
    return result;
}

bool startsWith(std::string_view value, std::string_view prefix)
{
    return value.size() >= prefix.size() && value.substr(0, prefix.size()) == prefix;
}

size_t parseFilesCount(std::string_view line)
{
    static constexpr std::string_view prefix = "files:";
    if (!startsWith(line, prefix))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected 'files: N' line in explicit prefix.path, got: '{}'", line);

    std::string_view count_str = line.substr(prefix.size());
    trimInPlace(count_str);
    if (count_str.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing file count in explicit prefix.path line: '{}'", line);

    size_t count = 0;
    const auto * begin = count_str.data();
    const auto * end = count_str.data() + count_str.size();
    auto [ptr, ec] = std::from_chars(begin, end, count);
    if (ec != std::errc() || ptr != end)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid file count in explicit prefix.path line: '{}'", line);
    return count;
}

std::pair<std::string, std::string> parseFileMappingLine(std::string_view line)
{
    trimInPlace(line);
    const auto split = line.find_first_of(" \t");
    if (split == std::string_view::npos)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid file mapping line in prefix.path (expected 'name blob_path'): '{}'", line);

    std::string_view name = line.substr(0, split);
    std::string_view blob = line.substr(split);
    trimInPlace(blob);

    if (name.empty() || blob.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid file mapping line in prefix.path (empty name or blob path): '{}'", line);
    if (blob.find_first_of(" \t") != std::string_view::npos)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Blob path must not contain whitespace in prefix.path: '{}'", line);

    return {std::string(name), std::string(blob)};
}

}

std::string serializePlainRewritablePrefixPath(const PlainRewritablePrefixPath & prefix_path)
{
    const std::string logical_path = normalizeLogicalPath(prefix_path.logical_path);

    if (!prefix_path.explicit_files)
    {
        if (!prefix_path.files.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Implicit prefix.path must not contain an explicit file list");
        return logical_path;
    }

    auto files = prefix_path.files;
    std::sort(files.begin(), files.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    std::string result;
    result.reserve(logical_path.size() + 32 + files.size() * 64);
    result.append(logical_path);
    result.push_back('\n');
    result.append("files: ");
    result.append(std::to_string(files.size()));

    for (const auto & [name, blob] : files)
    {
        if (name.empty() || blob.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Explicit prefix.path file mapping must have non-empty name and blob path");
        if (name.find_first_of(" \t\n") != std::string::npos)
            throw Exception(ErrorCodes::INCORRECT_DATA, "File name must not contain whitespace in prefix.path: '{}'", name);
        if (blob.find_first_of(" \t\n") != std::string::npos)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Blob path must not contain whitespace in prefix.path: '{}'", blob);

        result.push_back('\n');
        result.append(name);
        result.append("\t");
        result.append(blob);
    }

    return result;
}

PlainRewritablePrefixPath parsePlainRewritablePrefixPath(std::string_view content)
{
    /// Trim a single trailing newline for convenience; keep internal newlines.
    if (!content.empty() && content.back() == '\n')
        content.remove_suffix(1);
    if (!content.empty() && content.back() == '\r')
        content.remove_suffix(1);

    if (content.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "prefix.path content is empty");

    const auto first_newline = content.find('\n');
    if (first_newline == std::string_view::npos)
    {
        /// Implicit form: a single logical path line.
        return PlainRewritablePrefixPath{.logical_path = normalizeLogicalPath(content), .explicit_files = false, .files = {}};
    }

    PlainRewritablePrefixPath result;
    result.logical_path = normalizeLogicalPath(content.substr(0, first_newline));
    result.explicit_files = true;

    std::string_view rest = content.substr(first_newline + 1);
    if (!rest.empty() && rest.front() == '\r')
        rest.remove_prefix(1);

    const auto second_newline = rest.find('\n');
    std::string_view files_line = second_newline == std::string_view::npos ? rest : rest.substr(0, second_newline);
    trimInPlace(files_line);
    const size_t expected_count = parseFilesCount(files_line);

    if (second_newline == std::string_view::npos)
    {
        if (expected_count != 0)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Explicit prefix.path declares {} files but no file mapping lines follow",
                expected_count);
        return result;
    }

    std::string_view mappings = rest.substr(second_newline + 1);
    while (!mappings.empty())
    {
        if (mappings.front() == '\r')
            mappings.remove_prefix(1);

        const auto newline = mappings.find('\n');
        std::string_view line = newline == std::string_view::npos ? mappings : mappings.substr(0, newline);
        if (newline == std::string_view::npos)
            mappings = {};
        else
            mappings.remove_prefix(newline + 1);

        trimInPlace(line);
        if (line.empty())
            continue;

        result.files.push_back(parseFileMappingLine(line));
    }

    if (result.files.size() != expected_count)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Explicit prefix.path declares {} files but contains {} mapping lines",
            expected_count,
            result.files.size());

    return result;
}

}
