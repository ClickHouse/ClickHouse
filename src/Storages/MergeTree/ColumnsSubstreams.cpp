#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/NestedUtils.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <xxhash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnsSubstreams::ColumnEntry & ColumnsSubstreams::lastEntryForModification()
{
    chassert(!columns_substreams.empty());
    /// Entries are modified only while being built. Once interned they are shared with other parts, and
    /// this object is copyable, so refuse to modify an entry that someone else can see. Checked in
    /// release builds too: the copy would silently corrupt the other holders.
    if (columns_substreams.back().use_count() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot modify the substreams of column {}: they are shared with another data part", columns_substreams.back()->column);

    return const_cast<ColumnEntry &>(*columns_substreams.back());
}

void ColumnsSubstreams::addColumn(const String & column)
{
    if (!columns_substreams.empty() && columns_substreams.back()->substreams.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add new column {} to ColumnsSubstreams: previous column {} has empty substreams", column, columns_substreams.back()->column);

    auto entry = std::make_shared<ColumnEntry>();
    entry->column = column;
    columns_substreams.emplace_back(std::move(entry));
    first_substream_positions.push_back(static_cast<UInt32>(total_substreams));
}

void ColumnsSubstreams::addSubstreamToLastColumn(const String & substream)
{
    if (columns_substreams.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add new substream {} to ColumnsSubstreams: there are no columns", substream);

    /// In Wide part we can write to the same stream several times, keep only the first occurrence.
    if (columns_substreams.back()->substream_to_local_position.contains(substream))
        return;

    auto & entry = lastEntryForModification();
    entry.substream_to_local_position[substream] = entry.substreams.size();
    entry.substreams.emplace_back(substream);
    ++total_substreams;
}

void ColumnsSubstreams::addSubstreamsToLastColumn(const std::vector<String> & substreams)
{
    for (const auto & substream : substreams)
        addSubstreamToLastColumn(substream);
}

size_t ColumnsSubstreams::getSubstreamPosition(size_t column_position, const String & substream) const
{
    if (column_position >= columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get position for substream {}: column position {} is invalid, there are only {} columns", substream, column_position, columns_substreams.size());

    const auto & entry = *columns_substreams[column_position];
    auto it = entry.substream_to_local_position.find(substream);
    if (it == entry.substream_to_local_position.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get position for substream {}: column {} with position {} doesn't have such substream", substream, entry.column, column_position);

    return first_substream_positions[column_position] + it->second;
}

std::optional<size_t> ColumnsSubstreams::tryGetSubstreamPosition(size_t column_position, const String & substream) const
{
    if (column_position >= columns_substreams.size())
        return std::nullopt;

    const auto & entry = *columns_substreams[column_position];
    auto it = entry.substream_to_local_position.find(substream);
    if (it == entry.substream_to_local_position.end())
        return std::nullopt;

    return first_substream_positions[column_position] + it->second;
}

size_t ColumnsSubstreams::getSubstreamPosition(
    size_t column_position,
    const NameAndTypePair & name_and_type,
    const ISerialization::SubstreamPath & substream_path,
    const MergeTreeSettingsPtr & storage_settings) const
{
    ISerialization::StreamFileNameSettings stream_file_name_settings(*storage_settings);
    auto substream = ISerialization::getFileNameForStream(name_and_type, substream_path, stream_file_name_settings);
    if (auto position = tryGetSubstreamPosition(column_position, substream))
        return *position;

    /// To be able to read old parts after changes in stream file name settings, try to change settings and try to find it again.
    if (ISerialization::tryToChangeStreamFileNameSettingsForNotFoundStream(substream_path, stream_file_name_settings))
    {
        substream = ISerialization::getFileNameForStream(name_and_type, substream_path, stream_file_name_settings);
        if (auto position = tryGetSubstreamPosition(column_position, substream))
            return *position;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get position for substream {}: column {} with position {} doesn't have such substream", substream, name_and_type.name, column_position);
}


std::optional<size_t> ColumnsSubstreams::tryGetSubstreamPosition(const String & substream) const
{
    for (size_t i = 0; i != columns_substreams.size(); ++i)
    {
        const auto & entry = *columns_substreams[i];
        auto it = entry.substream_to_local_position.find(substream);
        if (it != entry.substream_to_local_position.end())
            return first_substream_positions[i] + it->second;
    }

    return std::nullopt;
}

size_t ColumnsSubstreams::getFirstSubstreamPosition(size_t column_position) const
{
    if (column_position >= columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get first substream position: column position {} is invalid, there are only {} columns", column_position, columns_substreams.size());

    if (columns_substreams[column_position]->substreams.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get first substream position: column {} with position {} doesn't have substreams", columns_substreams[column_position]->column, column_position);

    return first_substream_positions[column_position];
}

size_t ColumnsSubstreams::getLastSubstreamPosition(size_t column_position) const
{
    if (column_position >= columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get last substream position: column position {} is invalid, there are only {} columns", column_position, columns_substreams.size());

    if (columns_substreams[column_position]->substreams.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get last substream position: column {} with position {} doesn't have substreams", columns_substreams[column_position]->column, column_position);

    return first_substream_positions[column_position] + columns_substreams[column_position]->substreams.size() - 1;
}

const std::vector<String> & ColumnsSubstreams::getColumnSubstreams(size_t column_position) const
{
    if (column_position >= columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get substreams: column position {} is invalid, there are only {} columns", column_position, columns_substreams.size());

    return columns_substreams[column_position]->substreams;
}

const std::vector<String> * ColumnsSubstreams::tryGetColumnSubstreams(const String & column_name) const
{
    for (const auto & entry : columns_substreams)
    {
        if (entry->column == column_name)
            return &entry->substreams;
    }
    return nullptr;
}

ColumnsSubstreams ColumnsSubstreams::merge(const ColumnsSubstreams & left, const ColumnsSubstreams & right, const std::vector<String> & columns_order)
{
    std::unordered_map<std::string_view, size_t> left_column_to_position;
    left_column_to_position.reserve(left.columns_substreams.size());
    for (size_t i = 0; i != left.columns_substreams.size(); ++i)
        left_column_to_position[left.columns_substreams[i]->column] = i;

    std::unordered_map<std::string_view, size_t> right_column_to_position;
    right_column_to_position.reserve(right.columns_substreams.size());
    for (size_t i = 0; i != right.columns_substreams.size(); ++i)
        right_column_to_position[right.columns_substreams[i]->column] = i;

    /// Reuse the (possibly shared) per-column entries of the inputs instead of rebuilding them.
    auto append_entry = [](ColumnsSubstreams & to, const ColumnEntryPtr & entry)
    {
        to.columns_substreams.push_back(entry);
        to.first_substream_positions.push_back(static_cast<UInt32>(to.total_substreams));
        to.total_substreams += entry->substreams.size();
    };

    ColumnsSubstreams merged;
    for (const auto & column : columns_order)
    {
        if (auto left_it = left_column_to_position.find(column); left_it != left_column_to_position.end())
            append_entry(merged, left.columns_substreams[left_it->second]);
        else if (auto right_it = right_column_to_position.find(column); right_it != right_column_to_position.end())
            append_entry(merged, right.columns_substreams[right_it->second]);
    }

    return merged;
}

void ColumnsSubstreams::writeText(WriteBuffer & buf) const
{
    writeString("columns substreams version: 1\n", buf);
    DB::writeText(columns_substreams.size(), buf);
    writeString(" columns:\n", buf);
    for (const auto & entry : columns_substreams)
    {
        DB::writeText(entry->substreams.size(), buf);
        writeString(" substreams for column ", buf);
        writeBackQuotedString(entry->column, buf);
        writeCString(":\n", buf);
        for (const auto & substream : entry->substreams)
        {
            writeChar('\t', buf);
            writeString(substream, buf);
            writeChar('\n', buf);
        }
    }
}

void ColumnsSubstreams::readText(ReadBuffer & buf)
{
    columns_substreams.clear();
    first_substream_positions.clear();
    total_substreams = 0;

    assertString("columns substreams version: 1\n", buf);
    size_t num_columns = 0;
    DB::readText(num_columns, buf);
    assertString(" columns:\n", buf);
    columns_substreams.reserve(num_columns);
    first_substream_positions.reserve(num_columns);
    for (size_t i = 0; i != num_columns; ++i)
    {
        size_t num_substreams = 0;
        DB::readText(num_substreams, buf);
        assertString(" substreams for column ", buf);
        auto entry = std::make_shared<ColumnEntry>();
        readBackQuotedStringWithSQLStyle(entry->column, buf);
        assertString(":\n", buf);

        entry->substreams.resize(num_substreams);
        entry->substream_to_local_position.reserve(num_substreams);
        for (size_t j = 0; j != num_substreams; ++j)
        {
            assertChar('\t', buf);
            readString(entry->substreams[j], buf);
            assertChar('\n', buf);
            entry->substream_to_local_position[entry->substreams[j]] = j;
        }

        first_substream_positions.push_back(static_cast<UInt32>(total_substreams));
        total_substreams += num_substreams;
        columns_substreams.emplace_back(std::move(entry));
    }
}

String ColumnsSubstreams::toString() const
{
    WriteBufferFromOwnString buf;
    writeText(buf);
    return buf.str();
}

bool ColumnsSubstreams::operator==(const ColumnsSubstreams & other) const
{
    if (columns_substreams.size() != other.columns_substreams.size())
        return false;

    for (size_t i = 0; i != columns_substreams.size(); ++i)
    {
        const auto & lhs = columns_substreams[i];
        const auto & rhs = other.columns_substreams[i];
        if (lhs == rhs)
            continue;
        if (lhs->column != rhs->column || lhs->substreams != rhs->substreams)
            return false;
    }

    return true;
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"

/// Feed a string into the hash state, length-prefixed so concatenations are unambiguous.
static void updateHashWithString(XXH3_state_t & state, std::string_view s)
{
    UInt64 size = s.size();
    XXH_INLINE_XXH3_128bits_update(&state, &size, sizeof(size));
    XXH_INLINE_XXH3_128bits_update(&state, s.data(), s.size());
}

static void updateHashWithColumnEntry(XXH3_state_t & state, const ColumnsSubstreams::ColumnEntry & entry)
{
    updateHashWithString(state, entry.column);
    UInt64 size = entry.substreams.size();
    XXH_INLINE_XXH3_128bits_update(&state, &size, sizeof(size));
    for (const auto & substream : entry.substreams)
        updateHashWithString(state, substream);
}

UInt128 ColumnsSubstreams::getHash() const
{
    /// XXH3 instead of the more usual SipHash: this hashes hundreds of substream names per part
    /// on the part loading path, and XXH3 is several times faster (the hash is only used to key
    /// an in-memory cache, so it does not need to be cryptographic or stable across versions).
    XXH3_state_t state;
    XXH_INLINE_XXH3_128bits_reset(&state);

    UInt64 columns = columns_substreams.size();
    XXH_INLINE_XXH3_128bits_update(&state, &columns, sizeof(columns));

    for (const auto & entry : columns_substreams)
        updateHashWithColumnEntry(state, *entry);

    auto hash = XXH_INLINE_XXH3_128bits_digest(&state);
    return {hash.low64, hash.high64};
}

UInt128 ColumnsSubstreams::getColumnEntryHash(const ColumnEntry & entry)
{
    XXH3_state_t state;
    XXH_INLINE_XXH3_128bits_reset(&state);
    updateHashWithColumnEntry(state, entry);
    auto hash = XXH_INLINE_XXH3_128bits_digest(&state);
    return {hash.low64, hash.high64};
}

#pragma clang diagnostic pop

void ColumnsSubstreams::internColumnEntries(const std::function<ColumnEntryPtr(const ColumnEntryPtr &)> & intern)
{
    for (auto & entry : columns_substreams)
        entry = intern(entry);
}

void ColumnsSubstreams::validateColumns(const std::vector<String> & columns) const
{
    if (columns.size() != columns_substreams.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid columns substreams: expected {} columns, got {}", columns.size(), columns_substreams.size());

    for (size_t i = 0; i != columns_substreams.size(); ++i)
    {
        if (columns_substreams[i]->column != columns[i])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column at position {} in columns substreams: expected {}, got {}", i, columns[i], columns_substreams[i]->column);
    }
}

/// Check if substream name has a valid prefix: it must be exactly the prefix
/// or start with prefix followed by '.' or '%2E' (escaped dot used for Tuple element substreams).
static bool hasValidPrefix(const String & substream, const String & escaped_prefix)
{
    if (!substream.starts_with(escaped_prefix))
        return false;
    /// Must be exactly the prefix, or followed by '.' or '%2E' separator.
    /// Tuple element substreams use escapeForFileName(".element_name") which produces "%2Eelement_name".
    if (substream.size() == escaped_prefix.size())
        return true;
    if (substream[escaped_prefix.size()] == '.')
        return true;
    if (substream.substr(escaped_prefix.size(), 3) == "%2E")
        return true;
    return false;
}

std::pair<String, String> ColumnsSubstreams::findInvalidSubstreamName() const
{
    for (const auto & entry : columns_substreams)
    {
        auto escaped_column_name = escapeForFileName(entry->column);
        auto nested_table_name = Nested::extractTableName(entry->column);
        auto escaped_nested_table_name = escapeForFileName(nested_table_name);
        bool has_nested_prefix = (escaped_nested_table_name != escaped_column_name);

        for (const auto & substream : entry->substreams)
        {
            if (hasValidPrefix(substream, escaped_column_name))
                continue;

            if (has_nested_prefix && hasValidPrefix(substream, escaped_nested_table_name))
                continue;

            return {substream, entry->column};
        }
    }

    return {};
}

}
