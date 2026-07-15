#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexBloomSliced.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <numbers>
#include <ranges>

#include <roaring/roaring.hh>
#include <zstd.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Settings.h>
#include <Functions/Regexps.h>
#include <Functions/checkHyperscanRegexp.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/IndicesDescription.h>
#include <fmt/ranges.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/isValidUTF8.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int LOGICAL_ERROR;
extern const int CORRUPTED_DATA;
extern const int NO_SUCH_COLUMN_IN_TABLE;
extern const int SUPPORT_IS_DISABLED;
extern const int ZSTD_ENCODER_FAILED;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
}

namespace Setting
{
    extern const SettingsBool allow_hyperscan;
    extern const SettingsUInt64 max_hyperscan_regexp_length;
    extern const SettingsUInt64 max_hyperscan_regexp_total_length;
    extern const SettingsBool reject_expensive_hyperscan_regexps;
}

namespace
{

constexpr UInt8 CURRENT_BLOOM_SLICED_INDEX_VERSION = 1;
constexpr UInt64 DEFAULT_BITS = 8192;
constexpr UInt64 DEFAULT_HASHES = 4;
constexpr UInt64 DEFAULT_MIN_HASHES = 1;
constexpr double DEFAULT_FALSE_POSITIVE_RATE = 0.05;
constexpr UInt64 DEFAULT_ROWS_PER_SIGNATURE = 16;
constexpr UInt64 MIN_INFERRED_BITS = 8192;
constexpr UInt64 MAX_INFERRED_HASHES = 4;
constexpr UInt64 DEFAULT_ROWS_PER_BLOOM_SLICED_CHUNK = 1ULL << 20;
constexpr double DEFAULT_TARGET_BIT_DENSITY = 0.15;
constexpr double DEFAULT_SIGNAL_TO_NOISE_RATIO = 10.0;
constexpr std::string_view ARGUMENT_TOKENIZER = "tokenizer";
constexpr std::string_view ARGUMENT_PREPROCESSOR = "preprocessor";
constexpr std::string_view ARGUMENT_BITS = "bits";
constexpr std::string_view ARGUMENT_HASHES = "hashes";
constexpr std::string_view ARGUMENT_MIN_HASHES = "min_hashes";
constexpr std::string_view ARGUMENT_FALSE_POSITIVE_RATE = "false_positive_rate";
constexpr std::string_view ARGUMENT_ROWS_PER_SIGNATURE = "rows_per_signature";
constexpr UInt64 BLOOM_SLICED_HASH_SEED = 0x9e3779b97f4a7c15ULL;

/// Hard upper bounds for `bits` and `hashes`, enforced both when parsing an index declaration and
/// when reading parameters back from on-disk metadata. Inferred `bits` are clamped to
/// `MAX_BLOOM_SLICED_BITS`, and inferred `hashes` never exceed the validated declared value.
/// Rejecting larger on-disk values turns corrupted or crafted metadata into a `CORRUPTED_DATA`
/// exception instead of an excessive allocation attempt.
constexpr UInt64 MAX_BLOOM_SLICED_BITS = 1ULL << 26;
constexpr UInt64 MAX_BLOOM_SLICED_HASHES = 64;

/// Upper bound for token hash count entries during deserialization. It keeps a corrupted varint
/// count from driving either a huge allocation or an unbounded read loop before stream exhaustion.
constexpr UInt64 MAX_BLOOM_SLICED_TOKEN_HASH_COUNTS = 1ULL << 26;
constexpr UInt64 MAX_BLOOM_SLICED_TOKEN_HASH_COUNTS_RESERVE = 1ULL << 20;

template <typename Type>
std::optional<Type> extractUIntOption(NamedIndexArgumentsMap & options, std::string_view option_name)
{
    auto it = options.find(String(option_name));
    if (it == options.end())
        return std::nullopt;

    Field value = getFieldFromIndexArgumentAST(it->second);
    if (value.getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bloom-sliced index argument '{}' must be an unsigned integer", String(option_name));

    UInt64 raw = value.safeGet<UInt64>();
    if (raw > static_cast<UInt64>(std::numeric_limits<Type>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bloom-sliced index argument '{}' is too large: {}", String(option_name), raw);

    options.erase(it);
    return static_cast<Type>(raw);
}

std::optional<double> extractFloatOption(NamedIndexArgumentsMap & options, std::string_view option_name)
{
    auto it = options.find(String(option_name));
    if (it == options.end())
        return std::nullopt;

    Field value = getFieldFromIndexArgumentAST(it->second);
    if (value.getType() != Field::Types::Float64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bloom-sliced index argument '{}' must be a floating-point number", String(option_name));

    const double raw = value.safeGet<Float64>();
    options.erase(it);
    return raw;
}

void validateType(const IndexDescription & index)
{
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Bloom-sliced index must be created on a single expression");

    const auto nested_type = removeLowCardinalityAndNullable(index.data_types[0]);
    if (!WhichDataType(nested_type).isStringOrFixedString())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Bloom-sliced index supports only String and FixedString expressions, got: {}",
            index.data_types[0]->getName());
}


void validateParams(UInt64 bits, UInt64 hashes, UInt64 min_hashes, UInt64 rows_per_signature, int error_code)
{
    if (bits == 0)
        throw Exception(error_code, "Bloom-sliced index argument '{}' must be greater than 0", String(ARGUMENT_BITS));
    if (hashes == 0)
        throw Exception(error_code, "Bloom-sliced index argument '{}' must be greater than 0", String(ARGUMENT_HASHES));
    if (min_hashes == 0)
        throw Exception(error_code, "Bloom-sliced index argument '{}' must be greater than 0", String(ARGUMENT_MIN_HASHES));
    if (min_hashes > hashes)
        throw Exception(
            error_code,
            "Bloom-sliced index argument '{}' must be less than or equal to '{}'",
            String(ARGUMENT_MIN_HASHES),
            String(ARGUMENT_HASHES));
    if (rows_per_signature == 0)
        throw Exception(error_code, "Bloom-sliced index argument '{}' must be greater than 0", String(ARGUMENT_ROWS_PER_SIGNATURE));
    if (bits > MAX_BLOOM_SLICED_BITS)
        throw Exception(
            error_code,
            "Bloom-sliced index argument '{}' must be less than or equal to {}, got: {}",
            String(ARGUMENT_BITS),
            MAX_BLOOM_SLICED_BITS,
            bits);
    if (hashes > MAX_BLOOM_SLICED_HASHES)
        throw Exception(
            error_code,
            "Bloom-sliced index argument '{}' must be less than or equal to {}, got: {}",
            String(ARGUMENT_HASHES),
            MAX_BLOOM_SLICED_HASHES,
            hashes);
}

void validateFalsePositiveRate(double false_positive_rate, int error_code)
{
    if (!std::isfinite(false_positive_rate) || false_positive_rate <= 0.0 || false_positive_rate >= 1.0)
        throw Exception(
            error_code,
            "Bloom-sliced index argument '{}' must be greater than 0 and less than 1",
            String(ARGUMENT_FALSE_POSITIVE_RATE));
}

MergeTreeIndexBloomSlicedParams parseParams(NamedIndexArgumentsMap & options)
{
    MergeTreeIndexBloomSlicedParams params;

    auto false_positive_rate = extractFloatOption(options, ARGUMENT_FALSE_POSITIVE_RATE);
    params.false_positive_rate = false_positive_rate.value_or(DEFAULT_FALSE_POSITIVE_RATE);
    validateFalsePositiveRate(params.false_positive_rate, ErrorCodes::BAD_ARGUMENTS);

    auto bits = extractUIntOption<size_t>(options, ARGUMENT_BITS);
    params.bits = bits.value_or(DEFAULT_BITS);
    params.bits_explicit = bits.has_value();

    auto hashes = extractUIntOption<size_t>(options, ARGUMENT_HASHES);
    params.hashes = hashes.value_or(DEFAULT_HASHES);
    params.hashes_explicit = hashes.has_value();

    auto min_hashes = extractUIntOption<size_t>(options, ARGUMENT_MIN_HASHES);
    params.min_hashes = min_hashes.value_or(DEFAULT_MIN_HASHES);

    params.rows_per_signature = extractUIntOption<size_t>(options, ARGUMENT_ROWS_PER_SIGNATURE).value_or(DEFAULT_ROWS_PER_SIGNATURE);
    params.infer_from_false_positive_rate = !params.bits_explicit && !params.hashes_explicit;

    validateParams(params.bits, params.hashes, params.min_hashes, params.rows_per_signature, ErrorCodes::BAD_ARGUMENTS);
    return params;
}

std::unique_ptr<ITokenizer> extractTokenizer(NamedIndexArgumentsMap & options)
{
    auto tokenizer_ast = extractASTOption(options, ARGUMENT_TOKENIZER);
    if (tokenizer_ast)
        return TokenizerFactory::instance().get(tokenizer_ast);

    return TokenizerFactory::instance().get("ngrams(3)");
}

struct ParsedBloomSlicedIndex
{
    MergeTreeIndexBloomSlicedParams params;
    std::unique_ptr<ITokenizer> tokenizer;
};

ParsedBloomSlicedIndex parseAndValidate(const IndexDescription & index)
{
    validateType(index);

    auto options = parseNamedIndexArguments(
        index.arguments,
        [](const ASTPtr & argument)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Bloom-sliced index arguments must be key-value pairs, got {}",
                argument->formatForErrorMessage());
        },
        [](const ASTPtr & argument)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Bloom-sliced index argument key must be an identifier, got {}",
                argument->formatForErrorMessage());
        },
        [](std::string_view key)
        { throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bloom-sliced index argument '{}' is specified more than once", String(key)); });
    auto tokenizer = extractTokenizer(options);
    auto preprocessor_ast = extractASTOption(options, ARGUMENT_PREPROCESSOR);
    auto params = parseParams(options);
    params.preprocessor = std::move(preprocessor_ast);

    if (!options.empty())
    {
        std::vector<String> option_names;
        option_names.reserve(options.size());
        for (const auto & [name, _] : options)
            option_names.push_back(name);

        std::ranges::sort(option_names);
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected bloom_sliced index arguments: {}", fmt::join(option_names, ", "));
    }

    return {.params = params, .tokenizer = std::move(tokenizer)};
}

bool usesVariableHashes(const MergeTreeIndexBloomSlicedParams & params)
{
    // Variable hash mode is active only when token frequency can choose fewer than `hashes` positions.
    return params.min_hashes < params.hashes;
}

size_t hashCountForTokenFrequency(size_t groups_with_token, UInt64 total_groups, const MergeTreeIndexBloomSlicedParams & params)
{
    if (!usesVariableHashes(params) || total_groups == 0)
        return params.hashes;

    if (groups_with_token >= total_groups)
        return params.min_hashes;

    if (groups_with_token == 0)
        return params.hashes;

    const double signal = static_cast<double>(groups_with_token) / static_cast<double>(total_groups);
    const double target_false_positive_rate = signal / ((1.0 - signal) * DEFAULT_SIGNAL_TO_NOISE_RATIO);
    const double raw_hash_count = std::log(target_false_positive_rate) / std::log(DEFAULT_TARGET_BIT_DENSITY);
    const size_t hash_count = raw_hash_count > 0.0 ? static_cast<size_t>(std::ceil(raw_hash_count)) : 1;
    return std::clamp(hash_count, params.min_hashes, params.hashes);
}

size_t roundUpToPowerOfTwo(UInt64 value)
{
    if (value <= 1)
        return 1;

    if (value > (UInt64{1} << 63))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Inferred bloom_sliced bit count is too large: {}", value);

    return static_cast<size_t>(std::bit_ceil(value));
}

std::pair<size_t, size_t> inferBloomSlicedBitsAndHashes(
    double mark_false_positive_rate, double tokens_per_signature, UInt64 rows_per_signature, UInt64 index_granularity_rows)
{
    validateFalsePositiveRate(mark_false_positive_rate, ErrorCodes::BAD_ARGUMENTS);

    if (!std::isfinite(tokens_per_signature) || tokens_per_signature <= 0.0)
        return {DEFAULT_BITS, DEFAULT_HASHES};

    const double groups_per_mark = static_cast<double>(
        std::max<UInt64>(1, (index_granularity_rows + rows_per_signature - 1) / rows_per_signature));
    const double group_false_positive_rate = 1.0 - std::pow(1.0 - mark_false_positive_rate, 1.0 / groups_per_mark);
    validateFalsePositiveRate(group_false_positive_rate, ErrorCodes::BAD_ARGUMENTS);

    const double bits_per_token = -std::log(group_false_positive_rate) / (std::numbers::ln2 * std::numbers::ln2);
    const double raw_bits = std::ceil(tokens_per_signature * bits_per_token);
    if (!std::isfinite(raw_bits) || raw_bits > static_cast<double>(std::numeric_limits<UInt64>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Inferred bloom_sliced bit count is too large");

    /// Clamp to the hard limit so that any built part passes parameter validation on read.
    const auto requested_bits
        = std::min<UInt64>(static_cast<UInt64>(std::max<double>(raw_bits, MIN_INFERRED_BITS)), MAX_BLOOM_SLICED_BITS);
    const size_t bits = roundUpToPowerOfTwo(requested_bits);

    const double raw_hashes = std::round((static_cast<double>(bits) / tokens_per_signature) * std::numbers::ln2);
    const size_t hashes = std::clamp<size_t>(raw_hashes > 0.0 ? static_cast<size_t>(raw_hashes) : 1, 1, MAX_INFERRED_HASHES);
    return {bits, hashes};
}

template <typename Func>
void forEachBloomPosition(UInt64 hash1, UInt64 hash2, size_t hash_count, const MergeTreeIndexBloomSlicedParams & params, Func && func)
{
    UInt64 acc = hash1;
    if (std::has_single_bit(params.bits))
    {
        const UInt64 mask = params.bits - 1;
        for (size_t i = 0; i < hash_count; ++i)
        {
            func((acc + i * i) & mask);
            acc += hash2;
        }
        return;
    }

    for (size_t i = 0; i < hash_count; ++i)
    {
        func((acc + i * i) % params.bits);
        acc += hash2;
    }
}

std::vector<size_t> bloomPositions(UInt64 hash1, UInt64 hash2, size_t hash_count, const MergeTreeIndexBloomSlicedParams & params)
{
    std::vector<size_t> positions;
    positions.reserve(hash_count);
    forEachBloomPosition(hash1, hash2, hash_count, params, [&](size_t position) { positions.push_back(position); });
    return positions;
}

std::vector<size_t> bloomPositions(const char * data, size_t length, size_t hash_count, const MergeTreeIndexBloomSlicedParams & params)
{
    auto pair = BloomFilter::computeHashPair(data, length, BLOOM_SLICED_HASH_SEED);
    return bloomPositions(pair.hash1, pair.hash2, hash_count, params);
}

std::vector<size_t> bloomPositions(const char * data, size_t length, const MergeTreeIndexBloomSlicedParams & params)
{
    return bloomPositions(data, length, params.hashes, params);
}

roaring::Roaring bitmapForRange(const BloomSlicedIndexRowRange & range)
{
    roaring::Roaring result;
    result.addRangeClosed(static_cast<UInt32>(range.begin), static_cast<UInt32>(range.end));
    return result;
}

size_t rawBitsetBytes(UInt64 groups)
{
    return (groups + 7) / 8;
}

UInt64 computeGroupCount(UInt64 row_count, UInt64 rows_per_signature)
{
    if (row_count == 0)
        return 0;

    return 1 + (row_count - 1) / rows_per_signature;
}

UInt64 computeGroupsPerChunk(UInt64 rows_per_signature)
{
    return std::max<UInt64>(1, (DEFAULT_ROWS_PER_BLOOM_SLICED_CHUNK + rows_per_signature - 1) / rows_per_signature);
}

UInt64 rowsForChunk(UInt64 first_group, UInt64 group_count, UInt64 row_count, UInt64 rows_per_signature)
{
    const UInt64 first_row = first_group * rows_per_signature;
    if (first_row >= row_count)
        return 0;

    const UInt64 row_end = std::min(row_count, (first_group + group_count) * rows_per_signature);
    return row_end - first_row;
}

size_t checkedUInt64ToSizeT(UInt64 value, std::string_view name)
{
#if SIZE_MAX < UINT64_MAX
    if (value > static_cast<UInt64>(std::numeric_limits<size_t>::max()))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Bloom-sliced index {} is too large: {}", String(name), value);
#else
    (void)name;
#endif

    return static_cast<size_t>(value);
}

UInt64 rawBitsetCardinality(const std::vector<char> & raw, UInt64 groups)
{
    UInt64 cardinality = 0;
    for (char value : raw)
        cardinality += std::popcount(static_cast<unsigned int>(static_cast<UInt8>(value)));

    const UInt64 tail_bits = groups % 8;
    if (tail_bits != 0 && !raw.empty())
    {
        const UInt8 valid_bits_mask = static_cast<UInt8>((UInt8{1} << tail_bits) - 1);
        const UInt8 tail = static_cast<UInt8>(raw.back());
        if ((tail & ~valid_bits_mask) != 0)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Bloom-sliced raw-zstd bitset has bits set beyond group count {}", groups);
    }

    return cardinality;
}

bool andRawBitsets(std::vector<char> & lhs, const std::vector<char> & rhs)
{
    chassert(lhs.size() == rhs.size());

    bool is_empty = true;
    size_t offset = 0;
    for (; offset + sizeof(UInt64) <= lhs.size(); offset += sizeof(UInt64))
    {
        UInt64 left = 0;
        UInt64 right = 0;
        memcpy(&left, lhs.data() + offset, sizeof(left));
        memcpy(&right, rhs.data() + offset, sizeof(right));

        const UInt64 combined = left & right;
        memcpy(lhs.data() + offset, &combined, sizeof(combined));
        is_empty &= combined == 0;
    }

    for (; offset < lhs.size(); ++offset)
    {
        lhs[offset] = static_cast<char>(static_cast<UInt8>(lhs[offset]) & static_cast<UInt8>(rhs[offset]));
        is_empty &= lhs[offset] == 0;
    }

    return is_empty;
}

roaring::Roaring
rawBitsetToRowsBitmapRange(const std::vector<char> & raw, UInt64 first_group, UInt64 groups, UInt64 row_count, size_t rows_per_signature)
{
    roaring::Roaring result;
    for (UInt64 byte_index = 0; byte_index < raw.size(); ++byte_index)
    {
        UInt8 byte = static_cast<UInt8>(raw[byte_index]);
        while (byte != 0)
        {
            const UInt8 bit = static_cast<UInt8>(std::countr_zero(static_cast<unsigned int>(byte)));
            const UInt64 local_group_id = byte_index * 8 + bit;
            if (local_group_id < groups)
            {
                const UInt64 group_id = first_group + local_group_id;
                const UInt64 row_begin = group_id * rows_per_signature;
                const UInt64 row_end = std::min<UInt64>(row_count, row_begin + rows_per_signature);
                if (row_begin < row_end)
                    result.addRange(static_cast<UInt32>(row_begin), static_cast<UInt32>(row_end));
            }
            byte = static_cast<UInt8>(byte & (byte - 1));
        }
    }
    return result;
}

std::vector<char> compressRawBitsetZstd(const std::vector<char> & raw)
{
    std::vector<char> compressed(ZSTD_compressBound(raw.size()));
    const size_t compressed_size = ZSTD_compress(compressed.data(), compressed.size(), raw.data(), raw.size(), 3);
    if (ZSTD_isError(compressed_size))
    {
        throw Exception(
            ErrorCodes::ZSTD_ENCODER_FAILED,
            "Failed to compress bloom_sliced raw bitset with zstd: {}; zstd version: {}",
            ZSTD_getErrorName(compressed_size),
            ZSTD_VERSION_STRING);
    }
    compressed.resize(compressed_size);
    return compressed;
}

std::vector<char> decompressRawBitsetZstd(const std::vector<char> & compressed, size_t raw_size)
{
    const UInt64 frame_content_size = ZSTD_getFrameContentSize(compressed.data(), compressed.size());
    if (frame_content_size == ZSTD_CONTENTSIZE_ERROR)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Bloom-sliced raw-zstd bitmap is not a valid zstd frame");
    if (frame_content_size != ZSTD_CONTENTSIZE_UNKNOWN && frame_content_size != raw_size)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Unexpected bloom_sliced raw bitset size in zstd frame: {}, expected {}",
            frame_content_size,
            raw_size);

    std::vector<char> raw(raw_size);
    const size_t decompressed_size = ZSTD_decompress(raw.data(), raw.size(), compressed.data(), compressed.size());
    if (ZSTD_isError(decompressed_size))
    {
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Failed to decompress bloom_sliced raw bitset with zstd: {}; zstd version: {}",
            ZSTD_getErrorName(decompressed_size),
            ZSTD_VERSION_STRING);
    }
    if (decompressed_size != raw_size)
    {
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Unexpected bloom_sliced raw bitset size after zstd decompression: {}, expected {}",
            decompressed_size,
            raw_size);
    }
    return raw;
}

void writeBitmapCodec(BloomSlicedBitmapCodec codec, WriteBuffer & out)
{
    writeBinary(static_cast<UInt8>(codec), out);
}

BloomSlicedBitmapCodec readBitmapCodec(ReadBuffer & in)
{
    UInt8 raw = 0;
    readBinary(raw, in);
    if (raw > static_cast<UInt8>(BloomSlicedBitmapCodec::RawZstd))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown bloom_sliced bitmap codec {}", static_cast<unsigned int>(raw));
    return static_cast<BloomSlicedBitmapCodec>(raw);
}

/// Tombstone Bloom filters (only written for lossy preprocessors, see `has_lossy_preprocessor`).
/// One plain Bloom filter per chunk over the distinct raw tokens the preprocessor destroyed in
/// that chunk. The filter is sized from the observed number of distinct lost tokens for a ~1%
/// false-positive rate: `bits = ceil(n * ln(1/p) / ln^2(2))` and `hashes = round(bits / n * ln 2)`.
/// False positives only widen fail-open (a chunk is rechecked although the main slices could have
/// pruned it), so a small target rate suffices and the hard cap below is safe: overfull filters
/// degrade towards always-hit, i.e. towards total fail-open, never towards false negatives.
/// The cap also bounds the allocation for corrupted or crafted on-disk metadata.
constexpr double TOMBSTONE_BLOOM_FALSE_POSITIVE_RATE = 0.01;
constexpr UInt64 MIN_TOMBSTONE_BLOOM_BITS = 64;
constexpr UInt64 MAX_TOMBSTONE_BLOOM_BITS = 1ULL << 24;

std::pair<UInt64, UInt64> tombstoneBloomBitsAndHashes(UInt64 token_count)
{
    if (token_count == 0)
        return {0, 0};

    const double n = static_cast<double>(token_count);
    const double raw_bits = std::ceil(n * std::log(1.0 / TOMBSTONE_BLOOM_FALSE_POSITIVE_RATE) / (std::numbers::ln2 * std::numbers::ln2));
    const UInt64 bits = std::clamp<UInt64>(
        raw_bits >= static_cast<double>(MAX_TOMBSTONE_BLOOM_BITS) ? MAX_TOMBSTONE_BLOOM_BITS : static_cast<UInt64>(raw_bits),
        MIN_TOMBSTONE_BLOOM_BITS,
        MAX_TOMBSTONE_BLOOM_BITS);

    const double raw_hashes = std::round(static_cast<double>(bits) / n * std::numbers::ln2);
    const UInt64 hashes = std::clamp<UInt64>(raw_hashes > 0.0 ? static_cast<UInt64>(raw_hashes) : 1, 1, MAX_BLOOM_SLICED_HASHES);
    return {bits, hashes};
}

template <typename Func>
void forEachTombstoneBloomPosition(UInt64 hash1, UInt64 hash2, UInt64 hashes, UInt64 bits, Func && func)
{
    /// Same double-hashing scheme as `forEachBloomPosition`, but against the chunk-local
    /// tombstone bit count instead of the slice count.
    UInt64 acc = hash1;
    for (UInt64 i = 0; i < hashes; ++i)
    {
        func((acc + i * i) % bits);
        acc += hash2;
    }
}

void tombstoneBloomAdd(std::vector<char> & bloom, UInt64 bits, UInt64 hashes, const BloomSlicedHashPairKey & key)
{
    forEachTombstoneBloomPosition(
        key.hash1,
        key.hash2,
        hashes,
        bits,
        [&](UInt64 position)
        { bloom[position / 8] = static_cast<char>(static_cast<UInt8>(bloom[position / 8]) | (UInt8{1} << (position % 8))); });
}

bool tombstoneBloomProbe(const std::vector<char> & bloom, UInt64 bits, UInt64 hashes, const char * data, size_t size)
{
    if (bloom.empty())
        return false;

    const auto pair = BloomFilter::computeHashPair(data, size, BLOOM_SLICED_HASH_SEED);
    bool all_set = true;
    forEachTombstoneBloomPosition(
        pair.hash1,
        pair.hash2,
        hashes,
        bits,
        [&](UInt64 position) { all_set &= (static_cast<UInt8>(bloom[position / 8]) & (UInt8{1} << (position % 8))) != 0; });
    return all_set;
}

std::vector<String> compactBloomSlicedTokens(const ITokenizer & tokenizer, const VectorWithMemoryTracking<String> & tokens)
{
    auto compact_tokens = tokenizer.compactTokens(tokens);
    std::vector<String> result(compact_tokens.begin(), compact_tokens.end());
    std::ranges::sort(result);
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

/// The probe mapping of a raw token: the needle pipeline (`processConstant`, then tokenize) applied
/// to the token itself. The build side uses it to decide whether a raw token is lost in a row (the
/// mapping is not fully contained in the row's stored tokens) and the query side probes the main
/// slices with exactly the same mapping, so the two sides always agree by construction.
std::vector<String> probeTokensForRawToken(const ITokenizer & tokenizer, const MergeTreeIndexTextPreprocessor & preprocessor, const String & raw_token)
{
    const String value = preprocessor.processConstant(raw_token);
    VectorWithMemoryTracking<String> tokens;
    tokenizer.stringToTokens(value.data(), value.size(), tokens);
    return compactBloomSlicedTokens(tokenizer, tokens);
}

}

SipHash BloomSlicedTokenPredicate::getHash() const
{
    SipHash hash;
    hash.update(function_name);
    hash.update(tokens.size());
    for (const auto & token : tokens)
    {
        hash.update(token.size());
        hash.update(token);
    }
    hash.update(token_groups.size());
    for (const auto & group : token_groups)
    {
        hash.update(group.raw_token.size());
        hash.update(group.raw_token);
        hash.update(group.probe_tokens.size());
        for (const auto & token : group.probe_tokens)
        {
            hash.update(token.size());
            hash.update(token);
        }
    }
    return hash;
}

size_t BloomSlicedHashPairKeyHash::operator()(const BloomSlicedHashPairKey & key) const
{
    return std::hash<UInt64>{}(key.hash1) ^ (std::hash<UInt64>{}(key.hash2) + 0x9e3779b97f4a7c15ULL + (key.hash1 << 6) + (key.hash1 >> 2));
}

bool isBloomSlicedVirtualColumn(const String & column_name)
{
    return column_name.starts_with(BLOOM_SLICED_VIRTUAL_COLUMN_PREFIX);
}

MergeTreeIndexGranuleBloomSliced::MergeTreeIndexGranuleBloomSliced(MergeTreeIndexBloomSlicedParams params_)
    : params(params_)
{
}

size_t MergeTreeIndexGranuleBloomSliced::memoryUsageBytes() const
{
    size_t result = sizeof(*this);
    for (const auto & chunk : chunks)
    {
        result += sizeof(chunk) + chunk.bitmap_metadata.size() * sizeof(BloomSlicedBitmapMetadata);
        result += chunk.token_hash_counts.size() * (sizeof(UInt64) + sizeof(UInt64));
        result += chunk.tombstone_bloom.size();
        for (const auto & bitmap : chunk.raw_bitmaps)
            result += bitmap.size();
    }
    return result;
}

UInt64 MergeTreeIndexGranuleBloomSliced::groupCount() const
{
    return computeGroupCount(row_count, params.rows_per_signature);
}

UInt64 MergeTreeIndexGranuleBloomSliced::groupsPerChunk() const
{
    return computeGroupsPerChunk(params.rows_per_signature);
}

roaring::Roaring MergeTreeIndexGranuleBloomSliced::allRowsBitmap() const
{
    roaring::Roaring result;
    if (row_count > 0)
        result.addRangeClosed(0, static_cast<UInt32>(row_count - 1));
    return result;
}

namespace
{

/// Verdict of intersecting the slice bitsets of one probe-token set within one chunk.
enum class BloomSlicedChunkProbeVerdict
{
    /// No group of the chunk can contain all probe tokens.
    NoGroups,
    /// The tokens impose no constraint within the chunk (empty token set, or every checked slice is dense).
    AllGroups,
    /// The intersection of the checked slices is in the output bitset.
    Bitset,
    /// A required slice payload is not loaded; the caller must fail the whole chunk open.
    NotLoaded,
};

BloomSlicedChunkProbeVerdict probeChunkForTokens(
    const BloomSlicedChunkMetadata & chunk,
    const MergeTreeIndexBloomSlicedParams & params,
    const std::vector<String> & tokens,
    std::optional<std::vector<char>> & bitset)
{
    bitset.reset();

    for (const auto & token : tokens)
    {
        auto pair = BloomFilter::computeHashPair(token.data(), token.size(), BLOOM_SLICED_HASH_SEED);
        size_t hashes_for_token = params.hashes;
        if (usesVariableHashes(params))
        {
            auto it = chunk.token_hash_counts.find(pair.hash1);
            if (it == chunk.token_hash_counts.end())
                return BloomSlicedChunkProbeVerdict::NoGroups;

            hashes_for_token = std::min<size_t>(it->second, params.hashes);
        }

        for (size_t position : bloomPositions(pair.hash1, pair.hash2, hashes_for_token, params))
        {
            if (position >= chunk.bitmap_metadata.size())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Bloom-sliced bitmap position {} is out of range, bits: {}",
                    position,
                    chunk.bitmap_metadata.size());

            const auto & metadata = chunk.bitmap_metadata[position];
            if (metadata.codec == BloomSlicedBitmapCodec::Empty || metadata.cardinality == 0)
                return BloomSlicedChunkProbeVerdict::NoGroups;

            if (metadata.codec == BloomSlicedBitmapCodec::Dense || metadata.cardinality == chunk.group_count)
                continue;

            if (metadata.codec != BloomSlicedBitmapCodec::RawZstd)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Unexpected bloom_sliced bitmap codec {}", static_cast<unsigned int>(metadata.codec));

            /// The reader did not load this slice for this chunk. This can happen when a read hint is
            /// reused outside the row ranges that created it; keep correctness by failing open for the chunk.
            if (position >= chunk.raw_bitmaps.size() || chunk.raw_bitmaps[position].empty())
                return BloomSlicedChunkProbeVerdict::NotLoaded;

            if (bitset)
            {
                if (andRawBitsets(*bitset, chunk.raw_bitmaps[position]))
                    return BloomSlicedChunkProbeVerdict::NoGroups;
            }
            else
            {
                bitset = chunk.raw_bitmaps[position];
            }
        }
    }

    return bitset ? BloomSlicedChunkProbeVerdict::Bitset : BloomSlicedChunkProbeVerdict::AllGroups;
}

}

roaring::Roaring MergeTreeIndexGranuleBloomSliced::bitmapForPredicate(const BloomSlicedTokenPredicate & predicate) const
{
    if (!predicate.token_groups.empty())
        return bitmapForTokenGroups(predicate.token_groups);

    return bitmapForTokens(predicate.tokens);
}

roaring::Roaring MergeTreeIndexGranuleBloomSliced::bitmapForTokens(const std::vector<String> & tokens) const
{
    if (tokens.empty())
        return allRowsBitmap();

    roaring::Roaring result;
    const UInt64 groups = groupCount();

    for (const auto & chunk : chunks)
    {
        if (chunk.group_count == 0 || chunk.row_count == 0)
            continue;

        /// Chunks outside `readable_ranges` may intentionally not have slice payloads loaded. Fail open for
        /// them; mark pruning only intersects the resulting bitmap with candidate ranges.
        if (!chunk.loaded)
        {
            result.addRange(static_cast<UInt32>(chunk.first_row), static_cast<UInt32>(chunk.first_row + chunk.row_count));
            continue;
        }

        std::optional<std::vector<char>> raw_group_bitmap;
        switch (probeChunkForTokens(chunk, params, tokens, raw_group_bitmap))
        {
            case BloomSlicedChunkProbeVerdict::NoGroups:
                break;
            case BloomSlicedChunkProbeVerdict::AllGroups:
            case BloomSlicedChunkProbeVerdict::NotLoaded:
                result.addRange(static_cast<UInt32>(chunk.first_row), static_cast<UInt32>(chunk.first_row + chunk.row_count));
                break;
            case BloomSlicedChunkProbeVerdict::Bitset:
                result |= rawBitsetToRowsBitmapRange(
                    *raw_group_bitmap, chunk.first_group, chunk.group_count, row_count, params.rows_per_signature);
                break;
        }
    }

    if (chunks.empty() && groups == 0)
        return {};

    return result;
}

roaring::Roaring MergeTreeIndexGranuleBloomSliced::bitmapForTokenGroups(const std::vector<BloomSlicedTokenGroup> & token_groups) const
{
    if (token_groups.empty())
        return allRowsBitmap();

    roaring::Roaring result;
    const UInt64 groups = groupCount();

    for (const auto & chunk : chunks)
    {
        if (chunk.group_count == 0 || chunk.row_count == 0)
            continue;

        /// Chunks outside `readable_ranges` may intentionally not have slice payloads loaded. Fail open for
        /// them; mark pruning only intersects the resulting bitmap with candidate ranges.
        if (!chunk.loaded)
        {
            result.addRange(static_cast<UInt32>(chunk.first_row), static_cast<UInt32>(chunk.first_row + chunk.row_count));
            continue;
        }

        /// Per required raw token `t`, the rows the chunk may still admit are
        ///
        ///     allowed(t) = (intersection of the main slices over the probe tokens `Q(t)`)
        ///                  UNION (whole chunk if the tombstone Bloom filter contains raw `t`)
        ///
        /// and the chunk verdict is the intersection of `allowed(t)` over all required tokens.
        /// The tombstone widening must be applied to the per-token allowed set *before* the fold
        /// across tokens ("union before fold"): a token whose main probe misses because the
        /// preprocessor destroyed it in some row widens only its own allowed set to the whole
        /// chunk, while every other token's slices keep constraining the chunk. Dropping the
        /// token from the query instead would be unsound in disjunctive folds, so it is never
        /// done, even though this particular fold is purely conjunctive.
        bool chunk_is_empty = false;
        bool chunk_failed_open = false;
        std::optional<std::vector<char>> chunk_bitset;

        for (const auto & group : token_groups)
        {
            /// A tombstone hit widens allowed(t) to the whole chunk; the union absorbs the main
            /// probe verdict, so the main slices need not be checked for this token.
            if (tombstoneBloomProbe(
                    chunk.tombstone_bloom, chunk.tombstone_bits, chunk.tombstone_hashes, group.raw_token.data(), group.raw_token.size()))
                continue;

            /// An empty probe mapping (the preprocessor annihilates the token) constrains nothing:
            /// the build side never tombstones such tokens because the empty set is vacuously
            /// contained in every row's stored tokens, so the probe must fail open symmetrically.
            if (group.probe_tokens.empty())
                continue;

            std::optional<std::vector<char>> group_bitset;
            const auto verdict = probeChunkForTokens(chunk, params, group.probe_tokens, group_bitset);
            if (verdict == BloomSlicedChunkProbeVerdict::NotLoaded)
            {
                chunk_failed_open = true;
                break;
            }
            if (verdict == BloomSlicedChunkProbeVerdict::NoGroups)
            {
                chunk_is_empty = true;
                break;
            }
            if (verdict == BloomSlicedChunkProbeVerdict::AllGroups)
                continue;

            if (chunk_bitset)
            {
                if (andRawBitsets(*chunk_bitset, *group_bitset))
                {
                    chunk_is_empty = true;
                    break;
                }
            }
            else
            {
                chunk_bitset = std::move(group_bitset);
            }
        }

        if (chunk_is_empty && !chunk_failed_open)
            continue;

        if (chunk_failed_open || !chunk_bitset)
            result.addRange(static_cast<UInt32>(chunk.first_row), static_cast<UInt32>(chunk.first_row + chunk.row_count));
        else
            result |= rawBitsetToRowsBitmapRange(
                *chunk_bitset, chunk.first_group, chunk.group_count, row_count, params.rows_per_signature);
    }

    if (chunks.empty() && groups == 0)
        return {};

    return result;
}

void MergeTreeIndexGranuleBloomSliced::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "bloom_sliced indexes must be serialized with metadata and bitmap substreams");
}

void MergeTreeIndexGranuleBloomSliced::serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const
{
    auto * metadata_stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    auto * bitmaps_stream = streams.at(MergeTreeIndexSubstream::Type::BloomSlicedIndexBitmaps);

    const UInt64 groups = groupCount();
    const UInt64 chunk_groups = groupsPerChunk();
    const UInt64 chunk_count = groups == 0 ? 0 : (groups + chunk_groups - 1) / chunk_groups;

    std::vector<BloomSlicedChunkMetadata> serialized_chunks;
    serialized_chunks.reserve(chunks.size());
    for (const auto & source_chunk : chunks)
    {
        BloomSlicedChunkMetadata chunk = source_chunk;
        if (chunk.bitmap_metadata.size() != params.bits || chunk.raw_bitmaps.size() != params.bits)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Prepared bloom_sliced chunk has {} metadata entries and {} payloads, expected {}",
                chunk.bitmap_metadata.size(),
                chunk.raw_bitmaps.size(),
                params.bits);

        for (size_t position = 0; position < chunk.bitmap_metadata.size(); ++position)
        {
            auto & entry = chunk.bitmap_metadata[position];
            if (entry.codec == BloomSlicedBitmapCodec::RawZstd)
            {
                const auto & compressed = chunk.raw_bitmaps[position];
                if (compressed.empty())
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "Prepared bloom_sliced raw-zstd bitmap at position {} has no payload", position);

                entry.offset = bitmaps_stream->plain_hashing.count();
                entry.compressed_size = compressed.size();
                bitmaps_stream->plain_hashing.write(compressed.data(), compressed.size());
            }
            else
            {
                entry.offset = 0;
                entry.compressed_size = 0;
            }
        }
        serialized_chunks.push_back(std::move(chunk));
    }

    if (serialized_chunks.size() != chunk_count)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected number of prepared bloom_sliced chunks: {}, expected {}",
            serialized_chunks.size(),
            chunk_count);

    auto & out = metadata_stream->compressed_hashing;
    writeBinary(row_count, out);
    writeBinary(groups, out);
    writeBinary(static_cast<UInt64>(params.bits), out);
    writeBinary(static_cast<UInt64>(params.hashes), out);
    writeBinary(static_cast<UInt64>(params.min_hashes), out);
    writeBinary(static_cast<UInt64>(params.rows_per_signature), out);
    writeBinary(chunk_groups, out);
    writeVarUInt(serialized_chunks.size(), out);
    for (const auto & chunk : serialized_chunks)
    {
        writeBinary(chunk.first_group, out);
        writeBinary(chunk.group_count, out);
        writeBinary(chunk.first_row, out);
        writeBinary(chunk.row_count, out);
        writeVarUInt(chunk.bitmap_metadata.size(), out);
        for (const auto & entry : chunk.bitmap_metadata)
        {
            writeBitmapCodec(entry.codec, out);
            writeBinary(entry.offset, out);
            writeBinary(entry.compressed_size, out);
            writeBinary(entry.cardinality, out);
        }
        writeVarUInt(chunk.token_hash_counts.size(), out);
        for (const auto & [hash, hash_count] : chunk.token_hash_counts)
        {
            writeBinary(hash, out);
            writeBinary(hash_count, out);
        }

        /// The tombstone section exists only for lossy preprocessors; the reader derives the same
        /// decision from the index declaration. Case-fold and no-preprocessor indexes therefore
        /// keep a byte-identical format and pay nothing.
        if (params.has_lossy_preprocessor)
        {
            if (chunk.tombstone_bloom.empty())
            {
                writeBinary(UInt8{0}, out);
            }
            else
            {
                if (chunk.tombstone_bloom.size() != (chunk.tombstone_bits + 7) / 8 || chunk.tombstone_hashes == 0
                    || chunk.tombstone_token_count == 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Prepared bloom_sliced chunk has an invalid tombstone Bloom filter");

                writeBinary(UInt8{1}, out);
                writeBinary(chunk.tombstone_bits, out);
                writeBinary(chunk.tombstone_hashes, out);
                writeBinary(chunk.tombstone_token_count, out);
                out.write(chunk.tombstone_bloom.data(), chunk.tombstone_bloom.size());
            }
        }
        else if (!chunk.tombstone_bloom.empty())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Prepared bloom_sliced chunk has a tombstone Bloom filter without a lossy preprocessor");
        }
    }
}

void MergeTreeIndexGranuleBloomSliced::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != CURRENT_BLOOM_SLICED_INDEX_VERSION)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown bloom_sliced index version {}", version);

    UInt64 bits = 0;
    UInt64 hashes = 0;
    UInt64 min_hashes = 0;
    UInt64 rows_per_signature = 0;
    UInt64 groups = 0;
    UInt64 chunk_groups = 0;
    UInt64 num_chunks = 0;

    readBinary(row_count, istr);
    readBinary(groups, istr);
    readBinary(bits, istr);
    readBinary(hashes, istr);
    readBinary(min_hashes, istr);
    readBinary(rows_per_signature, istr);
    readBinary(chunk_groups, istr);
    readVarUInt(num_chunks, istr);

    validateParams(bits, hashes, min_hashes, rows_per_signature, ErrorCodes::CORRUPTED_DATA);

    if (rows_per_signature != params.rows_per_signature)
    {
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Bloom-sliced index metadata rows_per_signature does not match index declaration: "
            "metadata rows_per_signature={}; declaration rows_per_signature={}",
            rows_per_signature,
            params.rows_per_signature);
    }

    if (bits != params.bits || hashes != params.hashes)
    {
        if (params.infer_from_false_positive_rate)
        {
            params.bits = checkedUInt64ToSizeT(bits, "bit count");
            params.hashes = checkedUInt64ToSizeT(hashes, "hash count");
        }
        else
        {
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Bloom-sliced index metadata parameters do not match index declaration: "
                "metadata bits={}, hashes={}; declaration bits={}, hashes={}",
                bits,
                hashes,
                params.bits,
                params.hashes);
        }
    }

    if (min_hashes != params.min_hashes)
    {
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Bloom-sliced index metadata minimum hash count does not match index declaration: "
            "metadata min_hashes={}; declaration min_hashes={}",
            min_hashes,
            params.min_hashes);
    }

    if (row_count > std::numeric_limits<UInt32>::max())
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Bloom-sliced index row count {} exceeds supported maximum {}",
            row_count,
            std::numeric_limits<UInt32>::max());

    const UInt64 expected_groups = computeGroupCount(row_count, rows_per_signature);
    if (groups != expected_groups)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected bloom_sliced group count: {}, expected {}", groups, expected_groups);
    if (groups > std::numeric_limits<UInt32>::max())
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Bloom-sliced index group count {} exceeds supported maximum {}",
            groups,
            std::numeric_limits<UInt32>::max());

    const UInt64 expected_chunk_groups = computeGroupsPerChunk(rows_per_signature);
    if (chunk_groups != expected_chunk_groups)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA, "Unexpected bloom_sliced groups per chunk: {}, expected {}", chunk_groups, expected_chunk_groups);

    const UInt64 expected_chunks = groups == 0 ? 0 : (groups + chunk_groups - 1) / chunk_groups;
    if (num_chunks != expected_chunks)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected bloom_sliced chunk count: {}, expected {}", num_chunks, expected_chunks);

    chunks.clear();
    chunks.reserve(checkedUInt64ToSizeT(num_chunks, "chunk count"));

    bool has_previous_raw_bitmap = false;
    UInt64 previous_raw_bitmap_end = 0;
    for (UInt64 chunk_index = 0; chunk_index < num_chunks; ++chunk_index)
    {
        BloomSlicedChunkMetadata chunk;
        readBinary(chunk.first_group, istr);
        readBinary(chunk.group_count, istr);
        readBinary(chunk.first_row, istr);
        readBinary(chunk.row_count, istr);

        const UInt64 expected_first_group = chunk_index * chunk_groups;
        const UInt64 expected_group_count = std::min(chunk_groups, groups - expected_first_group);
        const UInt64 expected_first_row = expected_first_group * rows_per_signature;
        const UInt64 expected_row_count = rowsForChunk(expected_first_group, expected_group_count, row_count, rows_per_signature);
        if (chunk.first_group != expected_first_group || chunk.group_count != expected_group_count || chunk.first_row != expected_first_row
            || chunk.row_count != expected_row_count)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected bloom_sliced chunk directory entry at chunk {}", chunk_index);
        }

        UInt64 num_bitmaps = 0;
        readVarUInt(num_bitmaps, istr);
        if (num_bitmaps != bits)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected number of bloom_sliced bitmaps: {}, expected {}", num_bitmaps, bits);

        chunk.bitmap_metadata.assign(checkedUInt64ToSizeT(num_bitmaps, "bitmap count"), BloomSlicedBitmapMetadata{});
        chunk.raw_bitmaps.assign(checkedUInt64ToSizeT(num_bitmaps, "bitmap count"), std::vector<char>{});

        for (UInt64 i = 0; i < num_bitmaps; ++i)
        {
            auto & entry = chunk.bitmap_metadata[static_cast<size_t>(i)];
            entry.codec = readBitmapCodec(istr);
            readBinary(entry.offset, istr);
            readBinary(entry.compressed_size, istr);
            readBinary(entry.cardinality, istr);

            if (entry.cardinality > chunk.group_count)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Bloom-sliced bitmap cardinality {} at chunk {}, position {} exceeds chunk group count {}",
                    entry.cardinality,
                    chunk_index,
                    i,
                    chunk.group_count);

            if (entry.codec == BloomSlicedBitmapCodec::Empty)
            {
                if (entry.cardinality != 0 || entry.offset != 0 || entry.compressed_size != 0)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA, "Invalid bloom_sliced empty bitmap metadata at chunk {}, position {}", chunk_index, i);
            }
            else if (entry.codec == BloomSlicedBitmapCodec::Dense)
            {
                if (chunk.group_count == 0 || entry.cardinality != chunk.group_count || entry.offset != 0 || entry.compressed_size != 0)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Invalid bloom_sliced dense bitmap metadata at chunk {}, position {}: cardinality {}, expected {}",
                        chunk_index,
                        i,
                        entry.cardinality,
                        chunk.group_count);
            }
            else if (entry.codec == BloomSlicedBitmapCodec::RawZstd)
            {
                if (entry.cardinality == 0 || entry.cardinality >= chunk.group_count || entry.compressed_size == 0)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Invalid bloom_sliced raw-zstd bitmap metadata at chunk {}, position {}",
                        chunk_index,
                        i);
                /// The payload is a zstd frame of exactly rawBitsetBytes(chunk.group_count) decompressed bytes,
                /// so its compressed size cannot exceed the zstd worst-case bound. Rejecting larger values here
                /// keeps the payload read allocation bounded for corrupted or crafted metadata.
                if (const UInt64 max_compressed_size = ZSTD_compressBound(rawBitsetBytes(chunk.group_count));
                    entry.compressed_size > max_compressed_size)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Bloom-sliced raw-zstd bitmap at chunk {}, position {} has compressed size {} larger than maximum {}",
                        chunk_index,
                        i,
                        entry.compressed_size,
                        max_compressed_size);
                if (entry.offset > std::numeric_limits<UInt64>::max() - entry.compressed_size)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Bloom-sliced raw-zstd bitmap offset overflows at chunk {}, position {}",
                        chunk_index,
                        i);

                if (has_previous_raw_bitmap && entry.offset != previous_raw_bitmap_end)
                {
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Bloom-sliced raw-zstd bitmap at chunk {}, position {} has offset {}, expected {}",
                        chunk_index,
                        i,
                        entry.offset,
                        previous_raw_bitmap_end);
                }

                previous_raw_bitmap_end = entry.offset + entry.compressed_size;
                has_previous_raw_bitmap = true;
            }
        }

        UInt64 num_chunk_token_hash_counts = 0;
        readVarUInt(num_chunk_token_hash_counts, istr);
        if (!usesVariableHashes(params) && num_chunk_token_hash_counts != 0)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Bloom-sliced index chunk has token hash counts without variable hashes enabled");
        if (num_chunk_token_hash_counts > MAX_BLOOM_SLICED_TOKEN_HASH_COUNTS)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Bloom-sliced index chunk token hash count {} exceeds supported maximum {}",
                num_chunk_token_hash_counts,
                MAX_BLOOM_SLICED_TOKEN_HASH_COUNTS);

        chunk.token_hash_counts.reserve(
            checkedUInt64ToSizeT(std::min(num_chunk_token_hash_counts, MAX_BLOOM_SLICED_TOKEN_HASH_COUNTS_RESERVE), "chunk token hash count"));
        for (UInt64 i = 0; i < num_chunk_token_hash_counts; ++i)
        {
            UInt64 token_hash = 0;
            UInt64 hash_count = 0;
            readBinary(token_hash, istr);
            readBinary(hash_count, istr);

            if (hash_count < params.min_hashes || hash_count > params.hashes)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Bloom-sliced chunk token hash count {} is outside allowed range [{}, {}]",
                    hash_count,
                    params.min_hashes,
                    params.hashes);

            if (!chunk.token_hash_counts.emplace(token_hash, hash_count).second)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicate bloom_sliced chunk token hash metadata for hash {}", token_hash);
        }

        /// The tombstone section is present exactly when the index declaration has a lossy
        /// preprocessor (see `serializeBinaryWithMultipleStreams`). The index is experimental, so
        /// parts written by pre-tombstone development snapshots are not supported: reading such a
        /// part with a lossy preprocessor misaligns the metadata stream and fails the strict
        /// validation above with a `CORRUPTED_DATA` exception instead of returning wrong hints.
        if (params.has_lossy_preprocessor)
        {
            UInt8 tombstone_marker = 0;
            readBinary(tombstone_marker, istr);
            if (tombstone_marker > 1)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Unknown bloom_sliced tombstone marker {} at chunk {}",
                    static_cast<unsigned int>(tombstone_marker),
                    chunk_index);

            if (tombstone_marker == 1)
            {
                readBinary(chunk.tombstone_bits, istr);
                readBinary(chunk.tombstone_hashes, istr);
                readBinary(chunk.tombstone_token_count, istr);

                /// Bounds-check every field before the payload allocation, like the slice
                /// metadata above: corrupted or crafted values must raise an exception, not
                /// drive a huge allocation.
                if (chunk.tombstone_bits == 0 || chunk.tombstone_bits > MAX_TOMBSTONE_BLOOM_BITS)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Bloom-sliced tombstone bit count {} at chunk {} is outside allowed range [1, {}]",
                        chunk.tombstone_bits,
                        chunk_index,
                        MAX_TOMBSTONE_BLOOM_BITS);
                if (chunk.tombstone_hashes == 0 || chunk.tombstone_hashes > MAX_BLOOM_SLICED_HASHES)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Bloom-sliced tombstone hash count {} at chunk {} is outside allowed range [1, {}]",
                        chunk.tombstone_hashes,
                        chunk_index,
                        MAX_BLOOM_SLICED_HASHES);
                if (chunk.tombstone_token_count == 0)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Bloom-sliced tombstone Bloom filter at chunk {} has zero tokens; empty tombstone sets must use the empty marker",
                        chunk_index);

                chunk.tombstone_bloom.resize(checkedUInt64ToSizeT((chunk.tombstone_bits + 7) / 8, "tombstone Bloom filter size"));
                istr.readStrict(chunk.tombstone_bloom.data(), chunk.tombstone_bloom.size());
            }
        }

        chunks.push_back(std::move(chunk));
    }
}

void MergeTreeIndexGranuleBloomSliced::deserializeBinaryWithMultipleStreams(
    MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state)
{
    auto * metadata_stream = streams.at(MergeTreeIndexSubstream::Type::Regular);
    deserializeBinary(*metadata_stream->getDataBuffer(), state.version);

    const auto * condition = typeid_cast<const MergeTreeIndexConditionBloomSliced *>(state.condition);
    if (!condition)
        return;

    auto positions = condition->getNeededBitmapPositions(&params);
    auto * bitmaps_stream = streams.at(MergeTreeIndexSubstream::Type::BloomSlicedIndexBitmaps);

    std::vector<bool> chunks_to_load(chunks.size(), state.readable_ranges == nullptr);
    if (state.readable_ranges)
    {
        const auto & index_granularity = *state.part.index_granularity;
        for (const auto & range : *state.readable_ranges)
        {
            const UInt64 row_begin = index_granularity.getMarkStartingRow(range.begin);
            const UInt64 row_end = index_granularity.getMarkStartingRow(range.end);
            if (row_begin >= row_end)
                continue;

            const UInt64 first_group = row_begin / params.rows_per_signature;
            const UInt64 last_group = (row_end - 1) / params.rows_per_signature;
            const UInt64 first_chunk = first_group / groupsPerChunk();
            const UInt64 last_chunk = last_group / groupsPerChunk();
            for (UInt64 chunk_index = first_chunk; chunk_index <= last_chunk && chunk_index < chunks_to_load.size(); ++chunk_index)
                chunks_to_load[static_cast<size_t>(chunk_index)] = true;
        }
    }

    for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index)
    {
        auto & chunk = chunks[chunk_index];
        if (!chunks_to_load[chunk_index])
            continue;

        chunk.loaded = true;
        for (size_t position : positions)
        {
            if (position >= chunk.bitmap_metadata.size())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Bloom-sliced bitmap position {} is out of range, bits: {}",
                    position,
                    chunk.bitmap_metadata.size());

            const auto & metadata = chunk.bitmap_metadata[position];
            if (metadata.codec == BloomSlicedBitmapCodec::Empty || metadata.codec == BloomSlicedBitmapCodec::Dense)
                continue;

            if (metadata.codec != BloomSlicedBitmapCodec::RawZstd)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Unexpected bloom_sliced bitmap codec {}", static_cast<unsigned int>(metadata.codec));

            bitmaps_stream->seekToMark(MarkInCompressedFile{metadata.offset, 0});
            std::vector<char> compressed(checkedUInt64ToSizeT(metadata.compressed_size, "compressed bitmap size"));
            bitmaps_stream->getDataBuffer()->readStrict(compressed.data(), compressed.size());
            chunk.raw_bitmaps[position] = decompressRawBitsetZstd(compressed, rawBitsetBytes(chunk.group_count));

            const UInt64 actual_cardinality = rawBitsetCardinality(chunk.raw_bitmaps[position], chunk.group_count);
            if (actual_cardinality != metadata.cardinality)
            {
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Bloom-sliced raw-zstd bitmap at chunk {}, position {} has cardinality {}, expected {}",
                    chunk_index,
                    position,
                    actual_cardinality,
                    metadata.cardinality);
            }
        }
    }
}

MergeTreeIndexAggregatorBloomSliced::MergeTreeIndexAggregatorBloomSliced(
    MergeTreeIndexBloomSlicedParams params_, const ITokenizer * tokenizer_, MergeTreeIndexTextPreprocessorPtr preprocessor_)
    : params(params_)
    , tokenizer(tokenizer_)
    , preprocessor(std::move(preprocessor_))
    /// Freeze the buffering mode for the whole lifetime of the aggregator, before any row is consumed.
    /// In inference mode the token frequency statistics of the first chunk are needed to size the
    /// signature, so tokens must be buffered per group even if inference later lands on a fixed hash
    /// count. Whether per-token hash counts are actually written to disk is decided separately from
    /// `usesVariableHashes(params)` after inference (see `flushVariableHashChunk`), so the on-disk
    /// format stays consistent for every chunk of the part.
    , variable_hash_buffering(params.infer_from_false_positive_rate || usesVariableHashes(params))
{
}

void MergeTreeIndexAggregatorBloomSliced::addFixedHashTokenToGroup(const char * data, size_t length, UInt64 group_id)
{
    const UInt64 local_group_id = group_id - chunk_first_group;
    const size_t byte_index = checkedUInt64ToSizeT(local_group_id / 8, "local group byte index");
    const UInt8 mask = static_cast<UInt8>(UInt8{1} << (local_group_id % 8));
    const size_t raw_size = rawBitsetBytes(computeGroupsPerChunk(params.rows_per_signature));

    const auto pair = BloomFilter::computeHashPair(data, length, BLOOM_SLICED_HASH_SEED);
    forEachBloomPosition(
        pair.hash1,
        pair.hash2,
        params.hashes,
        params,
        [&](size_t position)
        {
            auto & raw = chunk_raw_bitmaps[position];
            if (raw.empty())
                raw.assign(raw_size, 0);

            UInt8 value = static_cast<UInt8>(raw[byte_index]);
            if ((value & mask) == 0)
            {
                raw[byte_index] = static_cast<char>(value | mask);
                ++chunk_cardinalities[position];
            }
        });
}

void MergeTreeIndexAggregatorBloomSliced::addVariableHashTokenToGroup(const char * data, size_t length, UInt64 group_id)
{
    const UInt64 local_group_id = group_id - chunk_first_group;
    if (local_group_id > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bloom-sliced local group id {} is too large", local_group_id);

    const auto pair = BloomFilter::computeHashPair(data, length, BLOOM_SLICED_HASH_SEED);
    auto & token_groups = variable_hash_chunk_tokens[BloomSlicedHashPairKey{.hash1 = pair.hash1, .hash2 = pair.hash2}];
    const auto local_group_id_32 = static_cast<UInt32>(local_group_id);
    if (token_groups.last_local_group != local_group_id_32)
    {
        token_groups.last_local_group = local_group_id_32;
        token_groups.local_groups.push_back(local_group_id_32);
    }
}

void MergeTreeIndexAggregatorBloomSliced::collectTombstoneTokensForRow(
    const char * raw_data, size_t raw_size, const std::unordered_set<std::string_view> & stored_tokens)
{
    /// Upper bound for the per-chunk memoization of raw-token probe mappings. Tokens beyond the
    /// cap are still processed correctly, just without caching.
    static constexpr size_t MAX_PROBE_TOKENS_CACHE_SIZE = 1ULL << 20;

    forEachToken(
        *tokenizer,
        raw_data,
        raw_size,
        [&](const char * token_data, size_t token_length)
        {
            const auto pair = BloomFilter::computeHashPair(token_data, token_length, BLOOM_SLICED_HASH_SEED);
            const BloomSlicedHashPairKey key{.hash1 = pair.hash1, .hash2 = pair.hash2};
            /// Already known to be lost somewhere in this chunk; the Bloom filter only needs the
            /// distinct set, so the (comparatively expensive) probe mapping can be skipped.
            if (chunk_tombstone_hashes.contains(key))
                return false;

            String raw_token(token_data, token_length);
            const std::vector<String> * probe_tokens = nullptr;
            std::vector<String> uncached_probe_tokens;
            if (auto it = chunk_probe_tokens_cache.find(raw_token); it != chunk_probe_tokens_cache.end())
            {
                probe_tokens = &it->second;
            }
            else
            {
                uncached_probe_tokens = probeTokensForRawToken(*tokenizer, *preprocessor, raw_token);
                if (chunk_probe_tokens_cache.size() < MAX_PROBE_TOKENS_CACHE_SIZE)
                    probe_tokens = &chunk_probe_tokens_cache.emplace(std::move(raw_token), std::move(uncached_probe_tokens)).first->second;
                else
                    probe_tokens = &uncached_probe_tokens;
            }

            /// The raw token is lost for this row iff its probe mapping is not fully contained in
            /// the row's stored tokens. An empty mapping is vacuously contained: the query side
            /// symmetrically treats it as "no constraint" and fails open without tombstones.
            for (const auto & probe_token : *probe_tokens)
            {
                if (!stored_tokens.contains(std::string_view(probe_token)))
                {
                    chunk_tombstone_hashes.insert(key);
                    break;
                }
            }

            return false;
        });
}

void MergeTreeIndexAggregatorBloomSliced::finishTombstoneBloomForChunk(BloomSlicedChunkMetadata & chunk)
{
    if (!params.has_lossy_preprocessor)
        return;

    chunk.tombstone_token_count = chunk_tombstone_hashes.size();
    std::tie(chunk.tombstone_bits, chunk.tombstone_hashes) = tombstoneBloomBitsAndHashes(chunk.tombstone_token_count);
    if (chunk.tombstone_bits != 0)
    {
        chunk.tombstone_bloom.assign((chunk.tombstone_bits + 7) / 8, 0);
        for (const auto & key : chunk_tombstone_hashes)
            tombstoneBloomAdd(chunk.tombstone_bloom, chunk.tombstone_bits, chunk.tombstone_hashes, key);
    }

    chunk_tombstone_hashes.clear();
    chunk_probe_tokens_cache.clear();
}

void MergeTreeIndexAggregatorBloomSliced::ensureFixedHashChunkForGroup(UInt64 group_id)
{
    const UInt64 chunk_groups = computeGroupsPerChunk(params.rows_per_signature);
    const UInt64 current_chunk_first_group = (group_id / chunk_groups) * chunk_groups;

    if (!has_chunk)
    {
        chunk_first_group = current_chunk_first_group;
        chunk_raw_bitmaps.assign(params.bits, std::vector<char>{});
        chunk_cardinalities.assign(params.bits, 0);
        has_chunk = true;
        return;
    }

    while (group_id >= chunk_first_group + chunk_groups)
    {
        flushFixedHashChunk(chunk_groups);
        chunk_first_group += chunk_groups;
        chunk_raw_bitmaps.assign(params.bits, std::vector<char>{});
        chunk_cardinalities.assign(params.bits, 0);
    }
}

void MergeTreeIndexAggregatorBloomSliced::ensureVariableHashChunkForGroup(UInt64 group_id)
{
    const UInt64 chunk_groups = computeGroupsPerChunk(params.rows_per_signature);
    const UInt64 current_chunk_first_group = (group_id / chunk_groups) * chunk_groups;

    if (!has_chunk)
    {
        chunk_first_group = current_chunk_first_group;
        /// No up-front reserve: the map grows geometrically as tokens arrive, so small inserts
        /// do not pay a multi-MiB bucket allocation.
        variable_hash_chunk_tokens.clear();
        has_chunk = true;
        return;
    }

    while (group_id >= chunk_first_group + chunk_groups)
    {
        flushVariableHashChunk(chunk_groups);
        chunk_first_group += chunk_groups;
        variable_hash_chunk_tokens.clear();
    }
}

void MergeTreeIndexAggregatorBloomSliced::inferParamsFromCurrentChunk(UInt64 group_count)
{
    if (!params.infer_from_false_positive_rate)
        return;

    UInt64 token_group_memberships = 0;
    for (const auto & [_, token_groups] : variable_hash_chunk_tokens)
        token_group_memberships += token_groups.local_groups.size();

    const double tokens_per_signature = group_count == 0 ? 0.0 : static_cast<double>(token_group_memberships) / static_cast<double>(group_count);
    auto [inferred_bits, inferred_hashes] = inferBloomSlicedBitsAndHashes(
        params.false_positive_rate, tokens_per_signature, params.rows_per_signature, params.index_granularity_rows);

    params.bits = inferred_bits;
    params.hashes = std::max(inferred_hashes, params.min_hashes);
    params.infer_from_false_positive_rate = false;
}

void MergeTreeIndexAggregatorBloomSliced::flushFixedHashChunk(UInt64 group_count)
{
    if (!has_chunk)
        return;

    BloomSlicedChunkMetadata chunk;
    chunk.first_group = chunk_first_group;
    chunk.group_count = group_count;
    chunk.first_row = chunk_first_group * params.rows_per_signature;
    chunk.row_count = rowsForChunk(chunk.first_group, chunk.group_count, row_count, params.rows_per_signature);
    chunk.bitmap_metadata.reserve(params.bits);
    chunk.raw_bitmaps.assign(params.bits, std::vector<char>{});

    const size_t raw_size = rawBitsetBytes(group_count);
    for (size_t position = 0; position < params.bits; ++position)
    {
        BloomSlicedBitmapMetadata entry;
        entry.cardinality = chunk_cardinalities[position];
        if (entry.cardinality == 0)
        {
            entry.codec = BloomSlicedBitmapCodec::Empty;
        }
        else if (entry.cardinality == group_count)
        {
            entry.codec = BloomSlicedBitmapCodec::Dense;
        }
        else
        {
            entry.codec = BloomSlicedBitmapCodec::RawZstd;
            auto raw = std::move(chunk_raw_bitmaps[position]);
            raw.resize(raw_size);
            auto compressed = compressRawBitsetZstd(raw);
            entry.compressed_size = compressed.size();
            chunk.raw_bitmaps[position] = std::move(compressed);
        }
        chunk.bitmap_metadata.push_back(entry);
    }

    finishTombstoneBloomForChunk(chunk);
    chunked_hash_chunks.push_back(std::move(chunk));
}

void MergeTreeIndexAggregatorBloomSliced::flushVariableHashChunk(UInt64 group_count)
{
    if (!has_chunk)
        return;

    inferParamsFromCurrentChunk(group_count);

    /// Inference runs exactly once, before the first chunk of the part is flushed, so this
    /// decision is the same for every chunk of the part and matches what the read side derives
    /// from the serialized parameters. If inference lands on `hashes == min_hashes`, per-token
    /// hash counts carry no information (every token uses exactly `hashes` hashes) and must not
    /// be written: the reader rejects chunks with token hash counts when variable hashes are off.
    const bool store_token_hash_counts = usesVariableHashes(params);

    BloomSlicedChunkMetadata chunk;
    chunk.first_group = chunk_first_group;
    chunk.group_count = group_count;
    chunk.first_row = chunk_first_group * params.rows_per_signature;
    chunk.row_count = rowsForChunk(chunk.first_group, chunk.group_count, row_count, params.rows_per_signature);
    chunk.bitmap_metadata.reserve(params.bits);
    chunk.raw_bitmaps.assign(params.bits, std::vector<char>{});
    if (store_token_hash_counts)
        chunk.token_hash_counts.reserve(variable_hash_chunk_tokens.size());

    chunk_raw_bitmaps.assign(params.bits, std::vector<char>{});
    chunk_cardinalities.assign(params.bits, 0);
    const size_t raw_size = rawBitsetBytes(group_count);

    for (const auto & [key, token_groups] : variable_hash_chunk_tokens)
    {
        const size_t hash_count = hashCountForTokenFrequency(token_groups.local_groups.size(), group_count, params);
        if (store_token_hash_counts)
        {
            auto [it, inserted] = chunk.token_hash_counts.emplace(key.hash1, hash_count);
            if (!inserted)
                it->second = std::min<UInt64>(it->second, hash_count);
        }

        forEachBloomPosition(
            key.hash1,
            key.hash2,
            hash_count,
            params,
            [&](size_t position)
            {
                auto & raw = chunk_raw_bitmaps[position];
                if (raw.empty())
                    raw.assign(raw_size, 0);

                for (UInt32 local_group_id : token_groups.local_groups)
                {
                    if (local_group_id >= group_count)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Bloom-sliced token local group id {} is outside chunk group count {}",
                            local_group_id,
                            group_count);

                    const size_t byte_index = local_group_id / 8;
                    const UInt8 mask = static_cast<UInt8>(UInt8{1} << (local_group_id % 8));
                    UInt8 value = static_cast<UInt8>(raw[byte_index]);
                    if ((value & mask) == 0)
                    {
                        raw[byte_index] = static_cast<char>(value | mask);
                        ++chunk_cardinalities[position];
                    }
                }
            });
    }

    for (size_t position = 0; position < params.bits; ++position)
    {
        BloomSlicedBitmapMetadata entry;
        entry.cardinality = chunk_cardinalities[position];
        if (entry.cardinality == 0)
        {
            entry.codec = BloomSlicedBitmapCodec::Empty;
        }
        else if (entry.cardinality == group_count)
        {
            entry.codec = BloomSlicedBitmapCodec::Dense;
        }
        else
        {
            entry.codec = BloomSlicedBitmapCodec::RawZstd;
            auto raw = std::move(chunk_raw_bitmaps[position]);
            raw.resize(raw_size);
            auto compressed = compressRawBitsetZstd(raw);
            entry.compressed_size = compressed.size();
            chunk.raw_bitmaps[position] = std::move(compressed);
        }
        chunk.bitmap_metadata.push_back(entry);
    }

    finishTombstoneBloomForChunk(chunk);
    chunked_hash_chunks.push_back(std::move(chunk));
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorBloomSliced::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleBloomSliced>(params);
    granule->row_count = row_count;

    const UInt64 groups = granule->groupCount();
    if (has_chunk)
    {
        const UInt64 remaining_groups = groups - chunk_first_group;
        if (variable_hash_buffering)
            flushVariableHashChunk(remaining_groups);
        else
            flushFixedHashChunk(remaining_groups);
    }

    granule->params = params;
    granule->chunks = std::move(chunked_hash_chunks);

    row_count = 0;
    chunked_hash_chunks.clear();
    chunk_raw_bitmaps.clear();
    chunk_cardinalities.clear();
    variable_hash_chunk_tokens.clear();
    chunk_first_group = 0;
    has_chunk = false;
    return granule;
}

void MergeTreeIndexAggregatorBloomSliced::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}",
            *pos,
            block.rows());

    const size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    /// The read side rejects parts with more than UInt32 max rows (row ids are stored in 32-bit
    /// roaring bitmaps), so enforce the same row limit here: any part that builds must be readable.
    if (row_count + rows_read > std::numeric_limits<UInt32>::max())
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot build bloom_sliced index in part with {} rows. Materialization of bloom_sliced index is not supported for parts "
            "with more than {} rows",
            row_count + rows_read,
            std::numeric_limits<UInt32>::max());
    }

    auto [preprocessed_column, offset] = preprocessor->processColumn(block.getByPosition(0), *pos, rows_read);

    // Expand `LowCardinality` wrappers before tokenizing so the index stores full string values, not dictionary keys.
    const auto column = preprocessed_column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();

    /// Under a lossy preprocessor, also walk the raw column value of every row to find the raw
    /// tokens the preprocessor destroyed (see `collectTombstoneTokensForRow`); the distinct lost
    /// tokens of a chunk go into the chunk's tombstone Bloom filter.
    ColumnPtr raw_column;
    if (params.has_lossy_preprocessor)
        raw_column = block.getByPosition(0).column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();

    if (!variable_hash_buffering)
    {
        for (size_t i = 0; i < rows_read; ++i)
        {
            const UInt64 group_id = row_count / params.rows_per_signature;
            ensureFixedHashChunkForGroup(group_id);

            const size_t row_in_block = offset + i;
            row_stored_tokens.clear();
            if (!column->isNullAt(row_in_block))
            {
                const auto ref = column->getDataAt(row_in_block);
                forEachToken(
                    *tokenizer,
                    ref.data(),
                    ref.size(),
                    [&](const char * token_data, size_t token_length)
                    {
                        addFixedHashTokenToGroup(token_data, token_length, group_id);
                        if (raw_column)
                            row_stored_tokens.emplace(token_data, token_length);
                        return false;
                    });
            }

            /// A NULL raw value never satisfies a lowered predicate, so it needs no tombstones. A
            /// non-NULL raw value whose preprocessed form is NULL stores no tokens at all, so
            /// every raw token with a non-empty probe mapping is lost and must be tombstoned;
            /// that is exactly what an empty stored-token set produces here.
            if (raw_column && !raw_column->isNullAt(*pos + i))
            {
                const auto raw_ref = raw_column->getDataAt(*pos + i);
                collectTombstoneTokensForRow(raw_ref.data(), raw_ref.size(), row_stored_tokens);
            }

            ++row_count;
        }

        *pos += rows_read;
        return;
    }

    for (size_t i = 0; i < rows_read; ++i)
    {
        const UInt64 group_id = row_count / params.rows_per_signature;
        ensureVariableHashChunkForGroup(group_id);

        const size_t row_in_block = offset + i;
        row_stored_tokens.clear();
        if (!column->isNullAt(row_in_block))
        {
            const auto ref = column->getDataAt(row_in_block);
            forEachToken(
                *tokenizer,
                ref.data(),
                ref.size(),
                [&](const char * token_data, size_t token_length)
                {
                    addVariableHashTokenToGroup(token_data, token_length, group_id);
                    if (raw_column)
                        row_stored_tokens.emplace(token_data, token_length);
                    return false;
                });
        }

        /// A NULL raw value never satisfies a lowered predicate, so it needs no tombstones. A
        /// non-NULL raw value whose preprocessed form is NULL stores no tokens at all, so
        /// every raw token with a non-empty probe mapping is lost and must be tombstoned;
        /// that is exactly what an empty stored-token set produces here.
        if (raw_column && !raw_column->isNullAt(*pos + i))
        {
            const auto raw_ref = raw_column->getDataAt(*pos + i);
            collectTombstoneTokensForRow(raw_ref.data(), raw_ref.size(), row_stored_tokens);
        }

        ++row_count;
    }

    *pos += rows_read;
}

MergeTreeIndexConditionBloomSliced::MergeTreeIndexConditionBloomSliced(
    const ActionsDAG::Node * predicate,
    ContextPtr context,
    const IndexDescription & index_description,
    MergeTreeIndexBloomSlicedParams params_,
    const ITokenizer * tokenizer_,
    MergeTreeIndexTextPreprocessorPtr preprocessor_)
    : index_column_name(index_description.column_names.at(0))
    , query_context(context)
    , params(params_)
    , tokenizer(tokenizer_)
    , preprocessor(std::move(preprocessor_))
{
    if (!predicate)
    {
        rpn.emplace_back(RPNElement::FUNCTION_UNKNOWN);
        return;
    }

    rpn = std::move(
        RPNBuilder<RPNElement>(
            predicate, context, [&](const RPNBuilderTreeNode & node, RPNElement & out) { return traverseAtomNode(node, out); })
            .extractRPN());

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_TOKEN_PREDICATE && element.predicate)
        {
            auto predicate_hash = element.predicate->getHash();
            all_token_predicates.emplace(predicate_hash.get128(), *element.predicate);
        }
    }
}

bool MergeTreeIndexConditionBloomSliced::alwaysUnknownOrTrue() const
{
    return rpnEvaluatesAlwaysUnknownOrTrue(rpn, {RPNElement::FUNCTION_TOKEN_PREDICATE, RPNElement::ALWAYS_FALSE});
}

std::vector<String> MergeTreeIndexConditionBloomSliced::stringToTokens(const Field & field, bool preprocess) const
{
    if (field.getType() != Field::Types::String)
        return {};

    VectorWithMemoryTracking<String> tokens;
    const String value = preprocess ? preprocessor->processConstant(field.safeGet<String>()) : field.safeGet<String>();
    tokenizer->stringToTokens(value.data(), value.size(), tokens);
    return compactBloomSlicedTokens(*tokenizer, tokens);
}

std::vector<String> MergeTreeIndexConditionBloomSliced::stringLikeToTokens(const Field & field, bool preprocess) const
{
    if (field.getType() != Field::Types::String || !tokenizer->supportsStringLike())
        return {};

    VectorWithMemoryTracking<String> tokens;
    const String value = preprocess ? preprocessor->processConstant(field.safeGet<String>()) : field.safeGet<String>();
    tokenizer->stringLikeToTokens(value.data(), value.size(), tokens);
    return compactBloomSlicedTokens(*tokenizer, tokens);
}

std::vector<String> MergeTreeIndexConditionBloomSliced::substringToTokens(const Field & field, bool is_prefix, bool is_suffix, bool preprocess) const
{
    if (field.getType() != Field::Types::String)
        return {};

    return substringToTokens(field.safeGet<String>(), is_prefix, is_suffix, preprocess);
}

std::vector<String> MergeTreeIndexConditionBloomSliced::substringToTokens(const String & raw_value, bool is_prefix, bool is_suffix, bool preprocess) const
{
    if (!tokenizer->supportsStringLike())
        return {};

    const String value = preprocess ? preprocessor->processConstant(raw_value) : raw_value;
    if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(value.data()), value.size()))
        return {};

    VectorWithMemoryTracking<String> tokens;
    tokenizer->substringToTokens(value.data(), value.size(), tokens, is_prefix, is_suffix);
    return compactBloomSlicedTokens(*tokenizer, tokens);
}

std::optional<std::vector<String>> MergeTreeIndexConditionBloomSliced::regexpToTokens(const String & regexp, bool preprocess) const
{
    if (!tokenizer->supportsStringLike())
        return std::nullopt;

    RegexpAnalysisResult analysis = OptimizedRegularExpression::analyze(regexp);
    VectorWithMemoryTracking<String> tokens;

    if (!analysis.required_substring.empty())
    {
        auto required_tokens = substringToTokens(analysis.required_substring, /*is_prefix=*/false, /*is_suffix=*/false, preprocess);
        tokens.insert(tokens.end(), required_tokens.begin(), required_tokens.end());
    }

    if (analysis.alternatives.empty())
    {
        auto result = compactBloomSlicedTokens(*tokenizer, tokens);
        if (result.empty())
            return std::nullopt;
        return result;
    }

    /// `bloom_sliced` hint predicates are conjunctive token sets. Multiple regexp alternatives
    /// would require an OR of token sets, so only use regexps whose alternatives all collapse
    /// to the same single required token set. Anything else fails open.
    std::optional<std::vector<String>> common_result;
    for (const auto & alternative : analysis.alternatives)
    {
        auto alternative_tokens = tokens;
        auto extracted_tokens = substringToTokens(alternative, /*is_prefix=*/false, /*is_suffix=*/false, preprocess);
        alternative_tokens.insert(alternative_tokens.end(), extracted_tokens.begin(), extracted_tokens.end());

        auto result = compactBloomSlicedTokens(*tokenizer, alternative_tokens);
        if (result.empty())
            return std::nullopt;

        if (!common_result)
            common_result = std::move(result);
        else if (*common_result != result)
            return std::nullopt;
    }

    return common_result;
}

std::vector<BloomSlicedTokenGroup> MergeTreeIndexConditionBloomSliced::makeTokenGroups(std::vector<String> raw_tokens) const
{
    std::vector<BloomSlicedTokenGroup> groups;
    groups.reserve(raw_tokens.size());
    for (auto & raw_token : raw_tokens)
    {
        auto probe_tokens = probeTokensForRawToken(*tokenizer, *preprocessor, raw_token);
        groups.push_back(BloomSlicedTokenGroup{.raw_token = std::move(raw_token), .probe_tokens = std::move(probe_tokens)});
    }
    return groups;
}

static void validateBloomSlicedRegexpPatterns(const Array & patterns, const Settings & settings)
{
    VectorWithMemoryTracking<std::string_view> needles;
    needles.reserve(patterns.size());

    for (const auto & pattern : patterns)
    {
        if (pattern.getType() == Field::Types::String)
            needles.emplace_back(pattern.safeGet<String>());
    }

    /// Validate the patterns exactly as `multiMatchAny` execution would, so the index
    /// does not silently prune granules where the function would raise an exception instead.
    checkHyperscanFunctionArguments(
        needles,
        settings[Setting::allow_hyperscan],
        settings[Setting::max_hyperscan_regexp_length],
        settings[Setting::max_hyperscan_regexp_total_length],
        settings[Setting::reject_expensive_hyperscan_regexps]);

#if USE_VECTORSCAN
    /// Compile the patterns as `multiMatchAny` execution does, so invalid regexps raise an exception.
    if (!needles.empty())
        MultiRegexps::getOrSet</*SaveIndices=*/ false, /*WithEditDistance=*/ false>(needles, std::nullopt)->get();
#endif
}

std::optional<BloomSlicedTokenPredicate>
MergeTreeIndexConditionBloomSliced::predicateFromFunctionNode(const RPNBuilderFunctionTreeNode & function_node) const
{
    if (function_node.getArgumentsSize() != 2)
        return std::nullopt;

    const String function_name = function_node.getFunctionName();
    if (function_name != "hasToken" && function_name != "hasAllTokens" && function_name != "hasTokenCaseInsensitive"
        && function_name != "like" && function_name != "ilike" && function_name != "startsWith" && function_name != "endsWith"
        && function_name != "match" && function_name != "multiMatchAny")
    {
        return std::nullopt;
    }

    /// `bloom_sliced` is a hint-only index: the original predicate is always re-evaluated on the
    /// raw column, and the index is used only to prune marks (or to zero the hint virtual column).
    /// The index stores tokens of `preprocessor(column)`, and needles are transformed with
    /// `processConstant`, so pruning must guarantee: whenever a raw row satisfies the raw
    /// predicate, the index admits the row. Two regimes provide that guarantee:
    ///
    /// * No preprocessor, or a pure case fold: `lower`, `lowerUTF8`, `upper` or `upperUTF8`
    ///   applied directly to the index column (`isLowerOrUpper`). Case folding maps characters in
    ///   place: letters stay letters and token separators stay separators, so if a needle (or a
    ///   required substring of a LIKE pattern or regexp) occurs in a raw row, its case-folded
    ///   form occurs at the same position in the case-folded row and yields the same tokens. For
    ///   a case-sensitive predicate such as `hasToken(text, 'World')` under a `lower`
    ///   preprocessor the index stores lowercased tokens and the needle is lowercased to `world`:
    ///   a matching row `World x` is indexed with token `world`, so it is never pruned; a
    ///   non-matching row `world x` may pass the hint - a false positive, which is allowed. The
    ///   needle is preprocessed as a whole and probed as a flat token set
    ///   (`BloomSlicedTokenPredicate::tokens`).
    ///
    /// * Any other deterministic preprocessor (`substring`, `trim`, `replaceRegexpAll`, arbitrary
    ///   expressions, ...) may drop or rewrite text, so the transformed needle can be absent from
    ///   the stored tokens of a matching row. For these, the index relies on the per-chunk
    ///   tombstone Bloom filters written at build time: required tokens are derived from the
    ///   *raw* needle with the existing boundary logic, and each raw token `t` becomes a token
    ///   group probing the main slices with `Q(t) = tokenize(processConstant(t))` and the chunk
    ///   tombstone filter with raw `t` (`BloomSlicedTokenPredicate::token_groups`).
    ///
    ///   Soundness of the tombstone regime: if a raw row `r` matches, every required raw token
    ///   `t` is a token of `r` (that is the same token-derivation property the no-preprocessor
    ///   case already relies on, applied to the raw needle). At build time the aggregator tested
    ///   the exact disjunction the probe evaluates: either `Q(t)` was fully contained in `r`'s
    ///   stored tokens - then the main probe with `Q(t)` admits `r`'s group (slice Bloom false
    ///   positives only ever admit more) - or `t` was recorded in the tombstone Bloom filter of
    ///   `r`'s chunk, which then fails open as a whole (tombstone Bloom false positives again
    ///   only ever admit more). Either way `r` survives; see `bitmapForTokenGroups` for the
    ///   union-before-fold combination across multiple required tokens. `NOT` and `OR` above the
    ///   atom stay safe: tombstone widening only ever *adds* rows to an atom's bitmap, `OR` in
    ///   `bitmapForGranule` unions per-atom over-approximations, and `FUNCTION_NOT` discards the
    ///   child bitmap entirely and fails open to all rows.
    ///
    ///   The tombstone section is guaranteed to be present in every readable part: it is written
    ///   for every chunk whenever the index declaration has a lossy preprocessor, and parts from
    ///   pre-tombstone development snapshots of this experimental index fail deserialization
    ///   outright (see `deserializeBinary`), so there is no readable part that would silently
    ///   lack tombstone data.
    ///
    /// The case-insensitive predicates (`hasTokenCaseInsensitive`, `ilike`) always require the
    /// case-fold preprocessor (checked per branch below): tombstones certify nothing about case
    /// variants of stored tokens, so without a case-folded dictionary the index cannot answer
    /// case-insensitive queries without false negatives, and such predicates fail open.
    const bool lossy_preprocessor = preprocessor->hasActions() && !preprocessor->isLowerOrUpper();

    auto haystack = function_node.getArgumentAt(0);
    auto needle = function_node.getArgumentAt(1);
    if (haystack.getColumnName() != index_column_name)
        return std::nullopt;

    Field const_value;
    DataTypePtr const_type;
    if (!needle.tryGetConstant(const_value, const_type))
        return std::nullopt;

    std::vector<String> tokens;
    if (function_name == "hasToken")
    {
        if (const_value.getType() != Field::Types::String)
            return std::nullopt;

        const auto & needle_value = const_value.safeGet<String>();
        /// Match `hasToken` execution semantics: ASCII non-alphanumeric separators in the
        /// needle are invalid and must be left to the original predicate so it can throw.
        if (std::ranges::any_of(needle_value, [](unsigned char c) { return isASCII(c) && !isAlphaNumericASCII(c); }))
            return std::nullopt;

        tokens = stringToTokens(const_value, /*preprocess=*/!lossy_preprocessor);
    }
    else if (function_name == "hasAllTokens")
    {
        if (const_value.getType() != Field::Types::String)
            return std::nullopt;
        tokens = stringToTokens(const_value, /*preprocess=*/!lossy_preprocessor);
    }
    else if (function_name == "hasTokenCaseInsensitive")
    {
        if (const_value.getType() != Field::Types::String || !preprocessor->isLowerOrUpper())
            return std::nullopt;

        const auto & needle_value = const_value.safeGet<String>();
        if (std::ranges::any_of(needle_value, [](unsigned char c) { return isASCII(c) && !isAlphaNumericASCII(c); }))
            return std::nullopt;

        tokens = stringToTokens(const_value, /*preprocess=*/true);
    }
    else if (function_name == "like")
    {
        if (const_value.getType() != Field::Types::String)
            return std::nullopt;
        tokens = stringLikeToTokens(const_value, /*preprocess=*/!lossy_preprocessor);
    }
    else if (function_name == "ilike")
    {
        if (const_value.getType() != Field::Types::String || !preprocessor->isLowerOrUpper())
            return std::nullopt;
        tokens = stringLikeToTokens(const_value, /*preprocess=*/true);
    }
    else if (function_name == "startsWith")
    {
        if (const_value.getType() != Field::Types::String)
            return std::nullopt;
        tokens = substringToTokens(const_value, /*is_prefix=*/true, /*is_suffix=*/false, /*preprocess=*/!lossy_preprocessor);
    }
    else if (function_name == "endsWith")
    {
        if (const_value.getType() != Field::Types::String)
            return std::nullopt;
        tokens = substringToTokens(const_value, /*is_prefix=*/false, /*is_suffix=*/true, /*preprocess=*/!lossy_preprocessor);
    }
    else if (function_name == "match")
    {
        if (const_value.getType() != Field::Types::String)
            return std::nullopt;

        const auto & pattern = const_value.safeGet<String>();
        /// Compile the pattern as `match` execution does, so an invalid regexp raises an exception.
        Regexps::createRegexp</*like=*/ false, /*no_capture=*/ true, /*case_insensitive=*/ false>(pattern);

        auto regexp_tokens = regexpToTokens(pattern, /*preprocess=*/!lossy_preprocessor);
        if (!regexp_tokens)
            return std::nullopt;
        tokens = std::move(*regexp_tokens);
    }
    else if (function_name == "multiMatchAny")
    {
        if (const_value.getType() != Field::Types::Array)
            return std::nullopt;

        const auto & patterns = const_value.safeGet<Array>();
        if (patterns.size() != 1 || patterns.front().getType() != Field::Types::String)
            return std::nullopt;

        /// Validate the pattern exactly as `multiMatchAny` execution would, so the index
        /// does not silently prune granules where the function would raise an exception instead.
        validateBloomSlicedRegexpPatterns(patterns, query_context->getSettingsRef());

        auto regexp_tokens = regexpToTokens(patterns.front().safeGet<String>(), /*preprocess=*/!lossy_preprocessor);
        if (!regexp_tokens)
            return std::nullopt;
        tokens = std::move(*regexp_tokens);
    }

    if (tokens.empty())
        return std::nullopt;

    /// Under a lossy preprocessor `tokens` is in the raw namespace; wrap every raw token into a
    /// token group carrying its probe mapping, so the granule can apply the tombstone rule.
    if (lossy_preprocessor)
        return BloomSlicedTokenPredicate{.function_name = function_name, .tokens = {}, .token_groups = makeTokenGroups(std::move(tokens))};

    return BloomSlicedTokenPredicate{.function_name = function_name, .tokens = std::move(tokens), .token_groups = {}};
}

std::vector<size_t> MergeTreeIndexConditionBloomSliced::getNeededBitmapPositions(const MergeTreeIndexBloomSlicedParams * actual_params) const
{
    const auto & bloom_params = actual_params ? *actual_params : params;
    std::vector<size_t> result;
    for (const auto & element : rpn)
    {
        if (element.function != RPNElement::FUNCTION_TOKEN_PREDICATE)
            continue;

        chassert(element.predicate.has_value());
        for (const auto & token : element.predicate->tokens)
        {
            auto positions = bloomPositions(token.data(), token.size(), bloom_params);
            result.insert(result.end(), positions.begin(), positions.end());
        }
        for (const auto & group : element.predicate->token_groups)
        {
            for (const auto & token : group.probe_tokens)
            {
                auto positions = bloomPositions(token.data(), token.size(), bloom_params);
                result.insert(result.end(), positions.begin(), positions.end());
            }
        }
    }

    std::ranges::sort(result);
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

std::optional<BloomSlicedTokenPredicate>
MergeTreeIndexConditionBloomSliced::createTokenPredicate(const ActionsDAG::Node & node, ContextPtr context) const
{
    auto node_rpn
        = RPNBuilder<RPNElement>(
              &node, context, [&](const RPNBuilderTreeNode & tree_node, RPNElement & out) { return traverseAtomNode(tree_node, out); })
              .extractRPN();

    if (node_rpn.size() != 1 || node_rpn.front().function != RPNElement::FUNCTION_TOKEN_PREDICATE || !node_rpn.front().predicate)
        return std::nullopt;

    return node_rpn.front().predicate;
}

String MergeTreeIndexConditionBloomSliced::replaceToVirtualColumn(const BloomSlicedTokenPredicate & predicate, const String & index_name)
{
    auto predicate_hash = predicate.getHash();
    auto hash_str = getSipHash128AsHexString(predicate_hash);
    String virtual_column_name
        = fmt::format("{}{}_{}_{}", BLOOM_SLICED_VIRTUAL_COLUMN_PREFIX, index_name, predicate.function_name, hash_str);

    auto it = all_token_predicates.find(predicate_hash.get128());
    virtual_column_to_token_predicate[virtual_column_name] = it == all_token_predicates.end() ? predicate : it->second;
    return virtual_column_name;
}

BloomSlicedTokenPredicate MergeTreeIndexConditionBloomSliced::getTokenPredicateForVirtualColumn(const String & column_name) const
{
    auto it = virtual_column_to_token_predicate.find(column_name);
    if (it == virtual_column_to_token_predicate.end())
        throw Exception(
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Virtual column {} not found in MergeTreeIndexConditionBloomSliced", column_name);

    return it->second;
}

bool MergeTreeIndexConditionBloomSliced::traverseAtomNode(const RPNBuilderTreeNode & node, RPNElement & out) const
{
    Field const_value;
    DataTypePtr const_type;
    if (node.tryGetConstant(const_value, const_type))
    {
        if (const_value.getType() == Field::Types::UInt64)
        {
            out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
        if (const_value.getType() == Field::Types::Int64)
        {
            out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            return true;
        }
    }

    if (!node.isFunction())
        return false;

    auto predicate = predicateFromFunctionNode(node.toFunctionNode());
    if (!predicate)
        return false;

    out.function = RPNElement::FUNCTION_TOKEN_PREDICATE;
    out.predicate = std::move(*predicate);
    return true;
}

roaring::Roaring MergeTreeIndexConditionBloomSliced::bitmapForGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    const auto * granule = typeid_cast<const MergeTreeIndexGranuleBloomSliced *>(idx_granule.get());
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bloom-sliced index condition got a granule with the wrong type");

    std::vector<roaring::Roaring> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_TOKEN_PREDICATE)
        {
            chassert(element.predicate.has_value());
            rpn_stack.push_back(granule->bitmapForPredicate(*element.predicate));
        }
        else if (element.function == RPNElement::FUNCTION_UNKNOWN || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(granule->allRowsBitmap());
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back({});
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = granule->allRowsBitmap();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto rhs = std::move(rpn_stack.back());
            rpn_stack.pop_back();
            rpn_stack.back() &= rhs;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto rhs = std::move(rpn_stack.back());
            rpn_stack.pop_back();
            rpn_stack.back() |= rhs;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in MergeTreeIndexConditionBloomSliced::RPNElement");
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in MergeTreeIndexConditionBloomSliced::bitmapForGranule");

    return std::move(rpn_stack.back());
}

bool MergeTreeIndexConditionBloomSliced::mayBeTrueOnGranule(
    MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const
{
    const auto * granule = typeid_cast<const MergeTreeIndexGranuleBloomSliced *>(idx_granule.get());
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bloom-sliced index condition got a granule with the wrong type");

    auto result = bitmapForGranule(idx_granule);
    if (const auto & current_range = granule->getCurrentRange())
        result &= bitmapForRange(*current_range);

    /// `bloom_sliced` computes one aggregate bitmap for the whole RPN. Reporting that
    /// result at an arbitrary RPN position would make disjunction pruning unsound.
    (void)update_partial_disjunction_result_fn;

    return !result.isEmpty();
}

std::string MergeTreeIndexConditionBloomSliced::getDescription() const
{
    return fmt::format(
        "(column: {}; tokenizer: {}; bits: {}; hashes: {}; min_hashes: {}; rows_per_signature: {})",
        backQuote(index_column_name),
        tokenizer->getDescription(),
        params.bits,
        params.hashes,
        params.min_hashes,
        params.rows_per_signature);
}

MergeTreeIndexBloomSliced::MergeTreeIndexBloomSliced(
    StorageMetadataPtr metadata_snapshot_,
    const IndexDescription & index_,
    MergeTreeIndexBloomSlicedParams params_,
    std::unique_ptr<ITokenizer> tokenizer_)
    : IMergeTreeIndex(std::move(metadata_snapshot_), index_)
    , params(params_)
    , tokenizer(std::move(tokenizer_))
    , preprocessor(std::make_shared<MergeTreeIndexTextPreprocessor>(params.preprocessor, index_))
{
    /// A lossy (non-case-fold) preprocessor switches on the per-chunk tombstone Bloom filters,
    /// both when building parts and when deciding whether the serialized chunk metadata contains
    /// the tombstone section.
    params.has_lossy_preprocessor = preprocessor->hasActions() && !preprocessor->isLowerOrUpper();
}

MergeTreeIndexSubstreams MergeTreeIndexBloomSliced::getSubstreams() const
{
    return {
        {MergeTreeIndexSubstream::Type::Regular, "", ".idx"},
        {MergeTreeIndexSubstream::Type::BloomSlicedIndexBitmaps, ".bsb", ".idx"},
    };
}

MergeTreeIndexFormat MergeTreeIndexBloomSliced::getDeserializedFormat(
    const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix, const IDataPartStorage * storage) const
{
    if (indexFileExistsInChecksums(checksums, path_prefix, ".idx", storage)
        && indexFileExistsInChecksums(checksums, path_prefix + ".bsb", ".idx", storage))
    {
        return {CURRENT_BLOOM_SLICED_INDEX_VERSION, getSubstreams()};
    }

    return {0, {}};
}

MergeTreeIndexGranulePtr MergeTreeIndexBloomSliced::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBloomSliced>(params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBloomSliced::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorBloomSliced>(params, tokenizer.get(), preprocessor);
}

MergeTreeIndexConditionPtr MergeTreeIndexBloomSliced::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionBloomSliced>(predicate, context, index, params, tokenizer.get(), preprocessor);
}

MergeTreeIndexPtr bloomSlicedIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & settings)
{
    auto parsed = parseAndValidate(index);
    parsed.params.index_granularity_rows = settings[MergeTreeSetting::index_granularity];
    return std::make_shared<MergeTreeIndexBloomSliced>(std::move(metadata_snapshot), index, parsed.params, std::move(parsed.tokenizer));
}

void bloomSlicedIndexValidator(const IndexDescription & index, bool, const MergeTreeSettings & settings)
{
    auto parsed = parseAndValidate(index);
    parsed.params.index_granularity_rows = settings[MergeTreeSetting::index_granularity];
    MergeTreeIndexTextPreprocessor preprocessor(parsed.params.preprocessor, index);
}

}
