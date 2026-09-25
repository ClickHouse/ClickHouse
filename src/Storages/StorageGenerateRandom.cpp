#include <Storages/IStorage.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageGenerateRandom.h>
#include <Storages/GenerateRandomSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/SelectQueryInfo.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Common/DateLUTImpl.h>
#include <Common/SipHash.h>
#include <Common/intExp10.h>
#include <Common/randomSeed.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionGenerateRandomStructure.h>

#include <pcg_random.hpp>
#include <Common/re2.h>

#include <algorithm>
#include <cmath>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 preferred_block_size_bytes;
}

namespace GenerateRandomSetting
{
    extern const GenerateRandomSettingsFloat null_ratio;
    extern const GenerateRandomSettingsUInt64 max_json_depth;
    extern const GenerateRandomSettingsUInt64 max_json_keys_per_object;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int TOO_LARGE_STRING_SIZE;
}


UInt32 GenerateRandomOptions::nullThreshold() const
{
    const Int64 threshold = std::lround(static_cast<double>(null_ratio) * 65536);
    return static_cast<UInt32>(std::clamp<Int64>(threshold, 0, 65536));
}

namespace
{

struct GenerateRandomState
{
    std::atomic<UInt64> add_total_rows = 0;
};
using GenerateRandomStatePtr = std::shared_ptr<GenerateRandomState>;

/// `Time` and `Time64` hold a signed number of seconds (scaled by 10^scale for `Time64`) in
/// [-999:59:59, 999:59:59]; larger values saturate to that boundary on text output,
/// see the cap in `DateLUTImpl.h`.
constexpr Int64 MAX_TIME_SECONDS = 3'599'999;

void fillBufferWithRandomBytes(char * __restrict data, size_t size, pcg64 & rng)
{
    char * __restrict end = data + size;
    while (data < end)
    {
        /// The loop can be further optimized.
        UInt64 number = rng();
        if constexpr (std::endian::native == std::endian::big)
            unalignedStoreLittleEndian<UInt64>(data, number);
        else
            unalignedStore<UInt64>(data, number);
        data += sizeof(UInt64); /// We assume that data has at least 7-byte padding (see PaddedPODArray)
    }
}

void fillBufferWithRandomPrintableASCIIBytes(char * __restrict data, size_t size, pcg64 & rng)
{
    size_t pos = 0;
    for (; pos + 4 <= size; pos += 4)
    {
        UInt64 rand = rng();

        UInt16 rand1 = static_cast<UInt16>(rand);
        UInt16 rand2 = static_cast<UInt16>(rand >> 16);
        UInt16 rand3 = static_cast<UInt16>(rand >> 32);
        UInt16 rand4 = static_cast<UInt16>(rand >> 48);

        /// Printable characters are from range [32; 126].
        /// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/

        data[pos + 0] = static_cast<char>(32 + ((rand1 * 95) >> 16));
        data[pos + 1] = static_cast<char>(32 + ((rand2 * 95) >> 16));
        data[pos + 2] = static_cast<char>(32 + ((rand3 * 95) >> 16));
        data[pos + 3] = static_cast<char>(32 + ((rand4 * 95) >> 16));

        /// NOTE gcc failed to vectorize this code (aliasing of char?)
    }

    if (pos < size)
    {
        UInt64 rand = rng();
        for (; pos < size; ++pos)
        {
            data[pos] = static_cast<char>(32 + ((static_cast<UInt16>(rand) * 95) >> 16));
            rand >>= 16;
        }
    }
}

template <typename T>
T randomInteger(pcg64 & rng)
{
    if constexpr (sizeof(T) <= 8)
        return T(rng());
    else if constexpr (sizeof(T) == 16)
        return T({rng(), rng()});
    else if constexpr (sizeof(T) == 32)
        return T({rng(), rng(), rng(), rng()});
    else
        static_assert(false, "unexpected sizeof(T)");
}

/// Pick a random bit width and randomize only so many lowest bits, so that small numbers are more likely.
/// Otherwise near-zero values for 64-bit types would effectively never be generated, and many code paths wouldn't be hit.
/// Set the remaining upper bits to either all 0 or all 1 (i.e. negative number close to 0).
///
/// Implementation works on a UInt64 representation to avoid UB (signed overflow, shift-into-sign-bit).
/// For wide integers, we apply the same logic to the lowest 64-bit word and extend the sign to upper words.
template <typename T>
T fuzzyRandomInteger(pcg64 & rng)
{
    T number = randomInteger<T>(rng);

    size_t num_bits = rng() % (sizeof(T) * 8);

    if constexpr (sizeof(T) <= sizeof(UInt64))
    {
        UInt64 low_mask = num_bits == 64 ? ~UInt64(0) : (UInt64(1) << num_bits) - 1;
        UInt64 u_number = static_cast<UInt64>(static_cast<std::make_unsigned_t<T>>(number));
        UInt64 sign = -(u_number >> (sizeof(T) * 8 - 1));
        return static_cast<T>((u_number & low_mask) | (sign & ~low_mask));
    }
    else
    {
        /// For wide integers, build the mask and sign-extension word by word.
        /// Each word is 64 bits. We process from the least significant word upward.
        constexpr size_t num_words = sizeof(T) / sizeof(UInt64);
        UInt64 words[num_words];
        memcpy(words, &number, sizeof(T));

        UInt64 sign_word = (words[num_words - 1] >> 63) ? ~UInt64(0) : UInt64(0);

        for (size_t w = 0; w < num_words; ++w)
        {
            size_t word_lo = w * 64;
            size_t word_hi = word_lo + 64;

            if (num_bits >= word_hi)
            {
                /// This entire word is within the random part — keep as-is.
            }
            else if (num_bits <= word_lo)
            {
                /// This entire word is above the random part — fill with sign.
                words[w] = sign_word;
            }
            else
            {
                /// The boundary falls within this word.
                size_t bits_in_word = num_bits - word_lo;
                UInt64 low_mask = (UInt64(1) << bits_in_word) - 1;
                words[w] = (words[w] & low_mask) | (sign_word & ~low_mask);
            }
        }

        T result{};
        memcpy(&result, words, sizeof(T));
        return result;
    }
}

/// Note: only sizeof(T) matters, it's ok to e.g. use T = UInt64 for Int64 column to reduce the number of template instantiations.
template <typename T>
void fillBufferWithRandomNumbers(char * __restrict data, size_t count, pcg64 & rng, bool fuzzy)
{
    T * values = reinterpret_cast<T*>(data);
    if (fuzzy)
    {
        for (size_t i = 0; i < count; ++i)
            values[i] = fuzzyRandomInteger<T>(rng);
    }
    else
    {
        fillBufferWithRandomBytes(data, count * sizeof(T), rng);

        /// Byteswap each value so that the generated *number* is the same on big-endian and little-endian machines (for the same seed).
        /// (Why not store rng outputs in native endianness in the first place?
        ///  For non-64 bit values it would produce results different from previous versions of clickhouse.
        ///  Maybe that would be ok, we'd just have to update all the tests that rely on it.)
        if constexpr (std::endian::native == std::endian::big && sizeof(T) > 1)
        {
            for (size_t i = 0; i < count; ++i)
                values[i] = unalignedLoadLittleEndian<T>(&values[i]);
        }
    }
}

template <typename T>
void fillRandomDecimals(char * __restrict data, size_t count, T range, pcg64 & rng, bool fuzzy)
{
    T * values = reinterpret_cast<T*>(data);
    if (fuzzy)
    {
        /// Ignore range because out-of-range values can appear in practice, e.g. `toDecimal32(2000000000, 0)`.
        for (size_t i = 0; i < count; ++i)
            values[i] = fuzzyRandomInteger<T>(rng);
    }
    else
    {
        for (size_t i = 0; i != count; ++i)
            /// Note: '%' preserves sign, so we get value in [-range + 1, range - 1].
            values[i] = randomInteger<T>(rng) % range;
    }
}

void appendFuzzyRandomString(ColumnString::Chars & out, size_t max_length, pcg64 & rng)
{
    const size_t initial_size = out.size();
    /// With probability 1/3 generate some special value like date or decimal.
    UInt64 which = rng() % 18;
    switch (which)
    {
        case 0: // Date
        case 1: // DateTime64
        {
            /// Each component possibly slightly out of valid range.
            UInt64 year = rng() % (2302 - 1898 + 1) + 1898;
            UInt64 month = rng() % 14;
            UInt64 day = rng() % 33;
            String s = fmt::format("{}-{}-{}", year, month, day);
            if (which == 1)
            {
                UInt64 hour = rng() % 25;
                UInt64 minute = rng() % 61;
                UInt64 second = rng() % 61;
                UInt64 scale = rng() % 11;
                s += fmt::format(" {}:{}:{}", hour, minute, second);
                if (scale > 0)
                    s += fmt::format(".{:0{}}", rng() % intExp10(static_cast<int>(scale)), static_cast<int>(scale));
            }
            out.insert(out.end(), s.begin(), s.end());
            break;
        }
        case 2: // UUID
        {
            auto s = formatUUID(UUID(fuzzyRandomInteger<UInt128>(rng)));
            out.insert(out.end(), s.begin(), s.end());
            break;
        }
        case 3: // IPv4
        {
            WriteBufferFromVector<ColumnString::Chars> buf(out, AppendModeTag{});
            writeIPv4Text(IPv4(fuzzyRandomInteger<UInt32>(rng)), buf);
            buf.finalize();
            break;
        }
        case 4: // IPv6
        {
            WriteBufferFromVector<ColumnString::Chars> buf(out, AppendModeTag{});
            writeIPv6Text(IPv6(fuzzyRandomInteger<UInt128>(rng)), buf);
            buf.finalize();
            break;
        }
        case 5: // type name
        {
            String s = FunctionGenerateRandomStructure::generateRandomDataType(rng, /*allow_suspicious_lc_types=*/ true, /*allow_complex_types=*/ rng() % 2);
            out.insert(out.end(), s.begin(), s.end());
            break;
        }
        default: break;
    }

    size_t size = out.size() - initial_size;
    if (size > 0)
    {
        if (size > max_length)
        {
            size = max_length;
            out.resize(initial_size + size);
        }
    }
    else
    {
        /// Just generate some characters at random.

        size = rng() % (max_length + 1);
        /// Generate short size more often.
        UInt64 shift = rng() % (65 - getLeadingZeroBits(max_length));
        size &= (shift < 64 ? (1ull << shift) : 0) - 1;

        out.resize(initial_size + size);
        if (size == 0)
            return;
        /// Pick from a few alphabets.
        switch (rng() % 3)
        {
            case 0: // arbitrary bytes
                fillBufferWithRandomBytes(reinterpret_cast<char *>(out.data() + initial_size), size, rng);
                break;
            case 1: // printable ASCII
                fillBufferWithRandomPrintableASCIIBytes(reinterpret_cast<char *>(out.data() + initial_size), size, rng);
                break;
            case 2: // digits, maybe with a leading '-' and/or a '.' somewhere
            {
                size_t i = 0;
                UInt64 r = rng();

                /// Maybe prepend '-'.
                if (r % 4 == 0)
                {
                    out[initial_size + i] = '-';
                    ++i;
                }
                r >>= 2;

                /// Fill the digits.
                for (; i < size; i += 8)
                {
                    UInt64 t = rng();
                    for (size_t j = 0; j < 8; ++j)
                    {
                        /// Relying on padding, suppress bounds check.
                        out.data()[initial_size + i + j] = char((t & 0xff) % 10 + '0');
                        t >>= 8;
                    }
                }

                /// Maybe insert a '.'.
                if (r % 4 == 0)
                    out[initial_size + (r >> 2) % size] = '.';

                break;
            }
            default: chassert(false);
        }
    }

    /// Randomly add, remove, or overwrite characters sometimes.
    while (rng() % 10 == 0)
    {
        switch (rng() % 3)
        {
            case 0:
                if (size < max_length)
                {
                    ++size;
                    size_t idx = rng() % size;
                    UInt8 ch[1] = {UInt8(rng() & 0xff)};
                    out.insert(out.begin() + initial_size + idx, &ch[0], &ch[0] + 1);
                }
                break;
            case 1:
                if (size > 0)
                {
                    size_t idx = rng() % size;
                    out.erase(out.begin() + initial_size + idx);
                    --size;
                }
                break;
            case 2:
                if (size > 0)
                {
                    size_t idx = rng() % size;
                    out[initial_size + idx] = char(rng() & 0xff);
                }
                break;
            default: chassert(false);
        }
    }
}


/// The shape of a self-describing type - which keys a `JSON` object has, which types a `Dynamic`
/// column mixes - is decided once per position in the type tree, not once per block, so that all
/// blocks and all streams of one query agree on it. Such a position is identified by a seed derived
/// from the seed of its parent. There is no two-argument `sipHash64`, hence these two helpers.
UInt64 deriveSeed(UInt64 seed, UInt64 salt)
{
    SipHash hash;
    hash.update(seed);
    hash.update(salt);
    return hash.get64();
}

UInt64 deriveSeed(UInt64 seed, std::string_view name)
{
    SipHash hash;
    hash.update(seed);
    hash.update(name);
    return hash.get64();
}

/// The options used below an `Array`, a `Map` or a `JSON` key. Halving `max_array_length` makes the
/// generated size grow subexponentially with the nesting depth.
GenerateRandomOptions nestedOptions(const GenerateRandomOptions & options)
{
    GenerateRandomOptions result = options;
    result.max_array_length /= 2;
    return result;
}

/// Probability that a scalar member of a self-describing type is wrapped into an array.
constexpr double ARRAY_PROBABILITY = 0.15;

/// The shape of a generated `JSON` document. These are deliberately constants and not settings: the
/// goal is one document stream that looks like a real one, not a knob for every dimension of it.
/// Only the three settings of the storage - `null_ratio`, `max_json_depth`,
/// `max_json_keys_per_object` - change it.

/// Probability that a key at depth `d` opens a nested object, if the depth budget allows it, is
/// `NESTED_OBJECT_PROBABILITY / d`: the deeper a level is, the less it branches.
constexpr double NESTED_OBJECT_PROBABILITY = 0.30;

/// Share of the keys that are sparse: present much more rarely than `null_ratio` alone would make them.
constexpr double SPARSE_KEY_SHARE = 0.30;
/// A sparse key is absent with the probability of `null_ratio` times a factor from this range.
constexpr UInt64 SPARSE_FACTOR_MIN = 4;
constexpr UInt64 SPARSE_FACTOR_MAX = 12;
/// Share of the arrays that hold objects instead of scalars, if the depth budget allows it.
constexpr double ARRAY_OF_OBJECTS_SHARE = 0.20;
/// Share of the arrays of scalars that are heterogeneous - `Array(Dynamic)` instead of `Array(T)`.
constexpr double MIXED_ARRAY_SHARE = 0.15;
/// Share of the scalar keys whose values drift between two types.
constexpr double TYPE_DRIFT_KEY_SHARE = 0.10;
/// Share of the present rows of a key with a secondary type that hold that secondary type.
constexpr double TYPE_DRIFT_ROW_SHARE = 0.10;
/// Hard cap on the number of generated paths of one `JSON` column, shared by the object trees of its
/// arrays of objects: a deep `max_json_depth` with many keys per object would otherwise let the
/// number of drawn schemas grow exponentially with the depth.
constexpr size_t MAX_GENERATED_JSON_PATHS = DataTypeObject::MAX_DYNAMIC_PATHS_LIMIT;

/// The names generated keys are drawn from. Real documents reuse a small vocabulary of short
/// lowercase names, and a generated stream is much easier to read when it does the same.
constexpr std::string_view JSON_KEY_VOCABULARY[] = {
    "id", "name", "type", "status", "user_id", "email", "created_at", "updated_at", "timestamp", "count", "value",
    "price", "amount", "currency", "tags", "url", "title", "description", "address", "city", "country", "zip", "lat",
    "lon", "items", "total", "enabled", "version", "message", "level", "source", "host", "region", "session", "event",
    "duration", "score", "rating", "comment", "parent", "children", "metadata", "attributes", "properties", "settings",
    "payload", "data", "result", "error", "code", "label", "category", "group", "owner", "author", "avatar", "image",
    "thumbnail", "width", "height", "size", "format", "mime", "hash", "token", "key", "secret", "phone", "first_name",
    "last_name", "birthday", "gender", "language", "locale", "timezone", "ip", "user_agent", "referrer", "path",
    "method", "query", "params", "headers", "body", "response", "request", "latency", "bytes", "retries", "priority",
    "state", "active", "deleted", "verified", "visible", "public", "order_id", "product_id", "sku", "quantity",
    "discount", "tax", "shipping", "subtotal", "notes", "reason", "role", "permissions", "scope", "expires_at",
    "started_at", "finished_at", "started", "finished", "progress", "stage", "step", "retry_count", "attempt",
    "cluster", "node", "shard", "replica", "table", "database", "column", "partition", "offset", "limit", "cursor",
    "page", "per_page", "sort", "filter", "search", "text", "summary", "content", "link", "links", "mentions", "likes",
    "shares", "views", "followers", "following",
};

/// A probability draw for schema decisions, with a resolution of 1/65536.
bool drawChance(pcg64 & rng, double probability)
{
    return rng() % 65536 < static_cast<UInt64>(std::lround(probability * 65536));
}

/// The scalar types a self-describing column can take, with their weights in percent. Both the set
/// and the proportions imitate what the JSON parser infers for real documents.
const std::vector<std::pair<DataTypePtr, UInt32>> & scalarTypePool()
{
    static const std::vector<std::pair<DataTypePtr, UInt32>> pool = {
        {std::make_shared<DataTypeString>(), 35},
        {std::make_shared<DataTypeInt64>(), 30},
        {std::make_shared<DataTypeFloat64>(), 10},
        {DataTypeFactory::instance().get("Bool"), 10},
        {std::make_shared<DataTypeDateTime>(), 5},
        {std::make_shared<DataTypeDate>(), 5},
        {std::make_shared<DataTypeUInt64>(), 5},
    };
    return pool;
}

/// Picks `count` distinct scalar types, each drawn by its weight among the types not picked yet. With
/// `wrap_into_arrays` some of them become `Array(Nullable(T))` - the shape the parser infers for a
/// homogeneous array. The result is distinct by type name, which is what `DataTypeVariant` deduplicates on.
DataTypes pickScalarTypes(pcg64 & rng, size_t count, bool wrap_into_arrays)
{
    const auto & pool = scalarTypePool();
    count = std::min(count, pool.size());

    std::vector<bool> used(pool.size(), false);
    UInt32 remaining_weight = 0;
    for (const auto & [_, weight] : pool)
        remaining_weight += weight;

    DataTypes result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        auto draw = static_cast<UInt32>(rng() % remaining_weight);
        std::optional<size_t> chosen;
        for (size_t j = 0; j < pool.size() && !chosen; ++j)
        {
            if (used[j])
                continue;
            if (draw < pool[j].second)
                chosen = j;
            else
                draw -= pool[j].second;
        }
        if (!chosen)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The weighted draw of a scalar type went out of range");

        used[*chosen] = true;
        remaining_weight -= pool[*chosen].second;
        const auto & scalar = pool[*chosen].first;
        result.push_back(
            wrap_into_arrays && drawChance(rng, ARRAY_PROBABILITY) ? std::make_shared<DataTypeArray>(makeNullable(scalar))
                                                                   : scalar);
    }
    return result;
}

/// The set of types one `Dynamic` column mixes, drawn once for its position in the type tree. Its
/// width does not depend on `max_dynamic_types`: that limit only decides how many of the types get a
/// variant of their own, the rest of them are encoded into the shared variant - the way
/// `ColumnDynamic` itself spills the types it meets once its limit is reached.
struct DynamicSchema
{
    DataTypes types;                          /// Distinct by name, at least one.
    size_t num_dedicated = 0;                 /// `min(max_dynamic_types, types.size())`: the leading types with a variant of their own.
    DataTypePtr variant_type;                 /// `Variant(types[0], ..., types[num_dedicated - 1], SharedVariant)`.
    std::vector<UInt8> global_discriminator;  /// The discriminator of `types[i]` inside `variant_type`; the shared one for a shared type.
    UInt8 shared_variant_discriminator = 0;
    size_t max_dynamic_types = 0;
};

/// Wraps an already chosen set of types into the variant a `Dynamic` column of them is built on: the
/// leading `max_dynamic_types` of them become variants of their own and the rest are left to the
/// shared variant, so the number of real variants never exceeds the limit of the type.
DynamicSchema makeDynamicSchema(DataTypes types, size_t max_dynamic_types)
{
    DynamicSchema schema;
    schema.types = std::move(types);
    schema.max_dynamic_types = max_dynamic_types;
    schema.num_dedicated = std::min(max_dynamic_types, schema.types.size());

    DataTypes variants(schema.types.begin(), schema.types.begin() + schema.num_dedicated);
    variants.push_back(ColumnDynamic::getSharedVariantDataType());
    schema.variant_type = std::make_shared<DataTypeVariant>(variants);

    /// `DataTypeVariant` sorts its variants by name, so never rely on the order the types were
    /// chosen in: resolve every one of them by name.
    const auto & variant_type = typeid_cast<const DataTypeVariant &>(*schema.variant_type);
    auto resolve = [&](const String & name)
    {
        auto discriminator = variant_type.tryGetVariantDiscriminator(name);
        if (!discriminator)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type {} is missing from the variant of a generated `Dynamic` column", name);
        return *discriminator;
    };

    schema.shared_variant_discriminator = resolve(ColumnDynamic::getSharedVariantTypeName());
    schema.global_discriminator.reserve(schema.types.size());
    for (size_t i = 0; i < schema.types.size(); ++i)
        schema.global_discriminator.push_back(
            i < schema.num_dedicated ? resolve(schema.types[i]->getName()) : schema.shared_variant_discriminator);

    return schema;
}

DynamicSchema buildDynamicSchema(UInt64 seed, size_t max_dynamic_types, bool array_element)
{
    pcg64 schema_rng(seed);

    DataTypes types;
    if (array_element)
    {
        /// A heterogeneous JSON array is inferred as `Array(Dynamic)` whose elements are strings
        /// mixed with one number type - the parser never produces a wider element set.
        types = {
            std::make_shared<DataTypeString>(),
            drawChance(schema_rng, 0.5) ? DataTypePtr(std::make_shared<DataTypeInt64>())
                                        : DataTypePtr(std::make_shared<DataTypeFloat64>())};
    }
    else
    {
        /// Between one and four types, whatever the limit of the column is: a low `max_dynamic_types`
        /// does not narrow the set of types the values take, it sends the excess to the shared variant.
        types = pickScalarTypes(schema_rng, 1 + schema_rng() % 4, /*wrap_into_arrays=*/true);
    }

    return makeDynamicSchema(std::move(types), max_dynamic_types);
}

/// Assembles a `ColumnVariant` out of one already filled column per alternative. `type_index[i]` is
/// the alternative of row `i`, or `value_columns.size()` when the row holds no value at all;
/// `global_discriminator[t]` is the discriminator of alternative `t` inside `variant_type`, whose
/// variants are sorted by type name.
MutableColumnPtr buildVariantColumn(
    const IDataType & variant_type,
    const std::vector<UInt8> & global_discriminator,
    const std::vector<UInt8> & type_index,
    MutableColumns && value_columns)
{
    const size_t num_types = value_columns.size();

    auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
    auto & discriminators = discriminators_column->getData();
    discriminators.resize(type_index.size());
    for (size_t i = 0; i < type_index.size(); ++i)
        discriminators[i] = type_index[i] == num_types ? ColumnVariant::NULL_DISCRIMINATOR : global_discriminator[type_index[i]];

    /// The variants go in global discriminator order; the ones no alternative maps to stay empty.
    const auto & variants = typeid_cast<const DataTypeVariant &>(variant_type).getVariants();
    MutableColumns columns(variants.size());
    for (size_t i = 0; i < variants.size(); ++i)
        columns[i] = variants[i]->createColumn();
    for (size_t i = 0; i < num_types; ++i)
        columns[global_discriminator[i]] = std::move(value_columns[i]);

    return ColumnVariant::create(std::move(discriminators_column), std::move(columns));
}

/// Assembles a `Dynamic` column out of one already filled column per schema type. `type_index[i]` is
/// the index in `schema.types` of the type of row `i`, or `schema.types.size()` when the row is NULL;
/// `value_columns[t]` holds the values of the rows of type `t`, in row order. The column of a
/// dedicated type becomes its variant as it is; the values of a shared type are encoded one by one,
/// in row order, into the shared variant, the way `ColumnDynamic` stores the types it has no variant
/// left for. Never silently promote a shared type to a real variant instead.
ColumnPtr buildDynamicColumn(const DynamicSchema & schema, const std::vector<UInt8> & type_index, MutableColumns && value_columns)
{
    const size_t num_types = schema.types.size();

    std::vector<SerializationPtr> serializations(num_types);
    for (size_t i = schema.num_dedicated; i < num_types; ++i)
        serializations[i] = schema.types[i]->getDefaultSerialization();

    auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
    auto & discriminators = discriminators_column->getData();
    discriminators.resize(type_index.size());

    /// `consumed[t]` is how many values of the shared type `t` are encoded already, so it is the
    /// position of the value of the next row of that type in `value_columns[t]`.
    std::vector<size_t> consumed(num_types, 0);
    auto shared_variant = ColumnString::create();
    for (size_t i = 0; i < type_index.size(); ++i)
    {
        const size_t chosen = type_index[i];
        if (chosen == num_types)
        {
            discriminators[i] = ColumnVariant::NULL_DISCRIMINATOR;
            continue;
        }

        discriminators[i] = schema.global_discriminator[chosen];
        if (chosen >= schema.num_dedicated)
        {
            ColumnDynamic::serializeValueIntoSharedVariant(
                *shared_variant, *value_columns[chosen], schema.types[chosen], serializations[chosen], consumed[chosen]);
            ++consumed[chosen];
        }
    }

    /// The variants go in global discriminator order; every slot is either a dedicated type or the
    /// shared variant, so none of them is left empty.
    const size_t num_variants = typeid_cast<const DataTypeVariant &>(*schema.variant_type).getVariants().size();
    MutableColumns columns(num_variants);
    for (size_t i = 0; i < schema.num_dedicated; ++i)
        columns[schema.global_discriminator[i]] = std::move(value_columns[i]);
    columns[schema.shared_variant_discriminator] = std::move(shared_variant);

    MutableColumnPtr variant_column = ColumnVariant::create(std::move(discriminators_column), std::move(columns));
    return ColumnDynamic::create(std::move(variant_column), schema.variant_type, schema.max_dynamic_types, schema.max_dynamic_types);
}

/// The JSON parser infers `UInt64` only for an integer above the `Int64` maximum - anything below it
/// comes back as `Int64` - so generated `UInt64` values of a `JSON` column are lifted into that range.
void raiseUInt64ValuesAboveInt64Max(IColumn & column)
{
    if (auto * array = typeid_cast<ColumnArray *>(&column))
    {
        raiseUInt64ValuesAboveInt64Max(array->getData());
    }
    else if (auto * nullable = typeid_cast<ColumnNullable *>(&column))
    {
        raiseUInt64ValuesAboveInt64Max(nullable->getNestedColumn());
    }
    else if (auto * numbers = typeid_cast<ColumnUInt64 *>(&column))
    {
        for (auto & value : numbers->getData())
            value |= UInt64(1) << 63;
    }
}

/// The drawn key set of one `JSON` column. A leaf is one generated path; the typed paths of the type
/// are not part of it, they are always present and filled from their declared type.
struct JSONSchema
{
    struct Leaf
    {
        String path;                  /// The full dotted path from the root of the object.
        DynamicSchema types;          /// One type, or two when the key drifts between types.
        UInt32 absent_threshold = 0;  /// Probability out of 65536 that the key is missing from a row.
    };

    /// The leading `num_dynamic_leaves` leaves become the dynamic paths of the column and the rest,
    /// sorted by path, its shared data. `unflattenAndInsertPaths` splits them in exactly this order.
    std::vector<Leaf> leaves;
    size_t num_dynamic_leaves = 0;
};

/// Draws the key set of one `JSON` column: a tree of objects whose leaves are the generated paths.
/// The result depends only on the seed of the column and on its declared type, so that every block
/// and every stream of one query produce the same document shape.
class JSONSchemaGenerator
{
public:
    JSONSchemaGenerator(
        const DataTypeObject & object_type_, UInt64 seed, const GenerateRandomOptions & options_, size_t & remaining_paths_)
        : object_type(object_type_), options(options_), rng(seed), remaining_paths(remaining_paths_)
    {
        /// The type has already validated them.
        for (const auto & regexp : object_type.getPathRegexpsToSkip())
            skip_regexps.push_back(std::make_unique<re2::RE2>(regexp));
    }

    JSONSchema generate()
    {
        generateObject("", 1);

        /// The most often present keys become the dynamic paths of the column and the rarer ones go
        /// to the shared data, which is where inserting such documents would put them as well.
        std::stable_sort(
            leaves.begin(),
            leaves.end(),
            [](const JSONSchema::Leaf & lhs, const JSONSchema::Leaf & rhs) { return lhs.absent_threshold < rhs.absent_threshold; });

        JSONSchema schema;
        schema.num_dynamic_leaves = std::min(leaves.size(), object_type.getMaxDynamicPaths());
        schema.leaves = std::move(leaves);
        std::sort(
            schema.leaves.begin() + schema.num_dynamic_leaves,
            schema.leaves.end(),
            [](const JSONSchema::Leaf & lhs, const JSONSchema::Leaf & rhs) { return lhs.path < rhs.path; });
        return schema;
    }

private:
    void generateObject(const String & prefix, size_t depth)
    {
        const size_t max_keys = options.max_json_keys_per_object;
        if (max_keys == 0)
            return;

        /// The root object is at least half full, so that documents do not degenerate to one key.
        const size_t min_keys = depth == 1 ? (max_keys + 1) / 2 : 1;
        const size_t num_keys = min_keys + rng() % (max_keys - min_keys + 1);

        std::unordered_set<String> used_names;
        for (size_t i = 0; i < num_keys && remaining_paths > 0; ++i)
            generateKey(prefix, depth, used_names);
    }

    void generateKey(const String & prefix, size_t depth, std::unordered_set<String> & used_names)
    {
        const String name = pickName(used_names);
        const String path = prefix.empty() ? name : prefix + "." + name;

        if (shouldSkipPath(path))
            return;

        const bool can_nest = depth < options.max_json_depth;

        /// A key that a typed path goes through can only be the object holding that typed path.
        if (hasTypedChildren(path))
        {
            if (can_nest)
                generateObject(path, depth + 1);
            return;
        }

        if (can_nest && drawChance(rng, NESTED_OBJECT_PROBABILITY / static_cast<double>(depth)))
        {
            generateObject(path, depth + 1);
            return;
        }

        JSONSchema::Leaf leaf;
        leaf.path = path;
        leaf.absent_threshold = drawAbsentThreshold();
        leaf.types = makeDynamicSchema(drawLeafTypes(can_nest), object_type.getMaxDynamicTypes());
        leaves.push_back(std::move(leaf));
        --remaining_paths;
    }

    /// The type of a generated key, or the two types its values drift between: a scalar, an array of
    /// scalars, an array of objects, or a heterogeneous array. The nested objects of an array of
    /// objects are a schema of their own, drawn when `RandomSchemas` walks the leaf type.
    DataTypes drawLeafTypes(bool can_nest)
    {
        if (drawChance(rng, ARRAY_PROBABILITY))
        {
            if (can_nest && drawChance(rng, ARRAY_OF_OBJECTS_SHARE))
                return {std::make_shared<DataTypeArray>(object_type.getTypeOfNestedObjects())};

            /// A heterogeneous array is inferred with the default `Dynamic` as its element type,
            /// whatever the limits of the object holding it are.
            if (drawChance(rng, MIXED_ARRAY_SHARE))
                return {std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>())};

            return {std::make_shared<DataTypeArray>(makeNullable(pickScalarTypes(rng, 1, /*wrap_into_arrays=*/false).front()))};
        }

        /// A key drifts whatever `max_dynamic_types` of the object is: a type its path column has no
        /// variant left for is encoded into the shared variant of that column, see `makeDynamicSchema`.
        const bool drifts = drawChance(rng, TYPE_DRIFT_KEY_SHARE);
        return pickScalarTypes(rng, drifts ? 2 : 1, /*wrap_into_arrays=*/false);
    }

    /// A name not used yet in the same object, drawn uniformly and then probed linearly.
    String pickName(std::unordered_set<String> & used_names)
    {
        const size_t vocabulary_size = std::size(JSON_KEY_VOCABULARY);
        if (used_names.size() < vocabulary_size)
        {
            const size_t start = rng() % vocabulary_size;
            for (size_t i = 0; i < vocabulary_size; ++i)
            {
                String name{JSON_KEY_VOCABULARY[(start + i) % vocabulary_size]};
                if (used_names.insert(name).second)
                    return name;
            }
        }

        /// The object has more keys than the vocabulary has words.
        String name = "word_" + std::to_string(used_names.size());
        used_names.insert(name);
        return name;
    }

    UInt32 drawAbsentThreshold()
    {
        const UInt64 base = options.nullThreshold();
        if (!drawChance(rng, SPARSE_KEY_SHARE))
            return static_cast<UInt32>(base);

        const UInt64 factor = SPARSE_FACTOR_MIN + rng() % (SPARSE_FACTOR_MAX - SPARSE_FACTOR_MIN + 1);

        /// A sparse key is still a key of the document: it must not become absent in every row.
        /// Saturating at 65536 would do exactly that for any `null_ratio` above 1/SPARSE_FACTOR_MAX,
        /// leaving a path that is never filled, so the cap keeps a share of the presence of an
        /// ordinary key instead.
        const UInt64 cap = 65536 - (65536 - base) / SPARSE_FACTOR_MAX;
        return static_cast<UInt32>(std::min(base * factor, cap));
    }

    /// Repeats `SerializationObject::shouldSkipPath`: a path that the serialization of the type would
    /// drop must not be generated at all, otherwise its values would silently disappear on insertion.
    bool shouldSkipPath(const String & path) const
    {
        if (object_type.getTypedPaths().contains(path))
            return true;

        for (const auto & skip : object_type.getPathsToSkip())
        {
            if (path.starts_with(skip))
                return true;
        }

        for (const auto & regexp : skip_regexps)
        {
            if (re2::RE2::FullMatch(path, *regexp))
                return true;
        }

        return false;
    }

    bool hasTypedChildren(const String & path) const
    {
        const String prefix = path + ".";
        for (const auto & [typed_path, _] : object_type.getTypedPaths())
        {
            if (typed_path.starts_with(prefix))
                return true;
        }
        return false;
    }

    const DataTypeObject & object_type;
    const GenerateRandomOptions & options;
    pcg64 rng;
    size_t & remaining_paths;  /// The budget of the whole column, shared with its nested schemas.
    std::vector<std::unique_ptr<re2::RE2>> skip_regexps;
    std::vector<JSONSchema::Leaf> leaves;
};

/// The drawn shapes of all self-describing types of one query, keyed by the seed of their position
/// in the type tree. Built once in `read` and shared, immutable, by all streams.
class RandomSchemas
{
public:
    static std::shared_ptr<const RandomSchemas> build(
        const NamesAndTypesList & columns, UInt64 storage_seed, const GenerateRandomOptions & options)
    {
        auto schemas = std::make_shared<RandomSchemas>();
        for (const auto & column : columns)
            schemas->walkColumn(column.type, deriveSeed(storage_seed, column.name), options);
        return schemas;
    }

    /// One column whose seed is already known, because its name is not observable from the outside.
    static std::shared_ptr<const RandomSchemas> buildForColumn(
        const DataTypePtr & type, UInt64 column_seed, const GenerateRandomOptions & options)
    {
        auto schemas = std::make_shared<RandomSchemas>();
        schemas->walkColumn(type, column_seed, options);
        return schemas;
    }

    const DynamicSchema & dynamic(UInt64 seed) const
    {
        auto it = dynamic_schemas.find(seed);
        if (it == dynamic_schemas.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No `Dynamic` schema was drawn for seed {}", seed);
        return it->second;
    }

    const JSONSchema & json(UInt64 seed) const
    {
        auto it = json_schemas.find(seed);
        if (it == json_schemas.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No `JSON` schema was drawn for seed {}", seed);
        return it->second;
    }

private:
    /// The path budget is per column: the arrays of objects of one `JSON` column share it.
    void walkColumn(const DataTypePtr & type, UInt64 column_seed, const GenerateRandomOptions & options)
    {
        remaining_json_paths = MAX_GENERATED_JSON_PATHS;
        walk(type, column_seed, options, false);
    }

    /// Repeats the child seed derivation of `fillColumnWithRandomData` exactly, so that every
    /// self-describing position it reaches is registered here under the very same seed.
    /// `array_element` says whether the type is the element type of an `Array`.
    void walk(const DataTypePtr & type, UInt64 seed, const GenerateRandomOptions & options, bool array_element)
    {
        switch (type->getTypeId())
        {
            case TypeIndex::Array:
                walk(typeid_cast<const DataTypeArray &>(*type).getNestedType(), deriveSeed(seed, 0), options, true);
                return;

            case TypeIndex::Map:
                walk(typeid_cast<const DataTypeMap &>(*type).getNestedType(), deriveSeed(seed, 0), options, false);
                return;

            case TypeIndex::Tuple:
            {
                const auto & elements = typeid_cast<const DataTypeTuple &>(*type).getElements();
                for (size_t i = 0; i < elements.size(); ++i)
                    walk(elements[i], deriveSeed(seed, i), options, false);
                return;
            }

            case TypeIndex::Nullable:
                walk(typeid_cast<const DataTypeNullable &>(*type).getNestedType(), seed, options, array_element);
                return;

            case TypeIndex::LowCardinality:
                walk(typeid_cast<const DataTypeLowCardinality &>(*type).getDictionaryType(), seed, options, array_element);
                return;

            case TypeIndex::Variant:
            {
                const auto & variants = typeid_cast<const DataTypeVariant &>(*type).getVariants();
                for (size_t i = 0; i < variants.size(); ++i)
                    walk(variants[i], deriveSeed(seed, i), options, false);
                return;
            }

            case TypeIndex::Dynamic:
            {
                const size_t max_dynamic_types = typeid_cast<const DataTypeDynamic &>(*type).getMaxDynamicTypes();
                const auto & schema
                    = dynamic_schemas.emplace(seed, buildDynamicSchema(seed, max_dynamic_types, array_element)).first->second;
                for (size_t i = 0; i < schema.types.size(); ++i)
                    walk(schema.types[i], deriveSeed(seed, i), options, false);
                return;
            }

            case TypeIndex::Object:
            {
                const auto & object_type = typeid_cast<const DataTypeObject &>(*type);
                const GenerateRandomOptions nested_options = nestedOptions(options);

                /// Sorted, because a typed path spends the shared path budget of the column: the
                /// iteration order of an unordered map must not decide which of them gets the rest of it.
                std::vector<String> typed_paths;
                typed_paths.reserve(object_type.getTypedPaths().size());
                for (const auto & [path, _] : object_type.getTypedPaths())
                    typed_paths.push_back(path);
                std::sort(typed_paths.begin(), typed_paths.end());

                for (const auto & path : typed_paths)
                    walk(object_type.getTypedPaths().at(path), deriveSeed(seed, path), nested_options, false);

                /// The depth and the key count of the document are not halved on the way down: they
                /// are the budget of the whole column, spent by the generator itself.
                const auto & schema
                    = json_schemas.emplace(seed, JSONSchemaGenerator(object_type, seed, options, remaining_json_paths).generate())
                          .first->second;
                for (const auto & leaf : schema.leaves)
                {
                    /// The root object is at depth 1, so a path with `d - 1` dots names a key at depth `d`.
                    const size_t leaf_depth = 1 + std::count(leaf.path.begin(), leaf.path.end(), '.');
                    if (leaf_depth > options.max_json_depth)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "The generated `JSON` path `{}` is deeper than the allowed {} levels",
                            leaf.path,
                            options.max_json_depth);

                    /// An array of objects at this leaf starts an object tree of its own, and the
                    /// levels this leaf sits at are already spent.
                    GenerateRandomOptions leaf_options = nested_options;
                    leaf_options.max_json_depth = options.max_json_depth - leaf_depth;

                    const UInt64 leaf_seed = deriveSeed(seed, leaf.path);
                    for (size_t i = 0; i < leaf.types.types.size(); ++i)
                        walk(leaf.types.types[i], deriveSeed(leaf_seed, i), leaf_options, false);
                }
                return;
            }

            default:
                /// Every other type has a fixed shape.
                return;
        }
    }

    std::unordered_map<UInt64, DynamicSchema> dynamic_schemas;
    std::unordered_map<UInt64, JSONSchema> json_schemas;
    /// How many paths the `JSON` column being walked may still generate.
    size_t remaining_json_paths = MAX_GENERATED_JSON_PATHS;
};

/// Everything the recursive generator needs besides the type and the number of rows.
struct GenerateRandomContext
{
    GenerateRandomOptions options;
    const RandomSchemas & schemas;   /// Immutable, shared by all streams of one query.
    pcg64 & rng;                     /// The value generator of one stream.
    UInt64 schema_seed = 0;          /// Identifies the position in the type tree.

    GenerateRandomContext child(UInt64 salt) const
    {
        return GenerateRandomContext{options, schemas, rng, deriveSeed(schema_seed, salt)};
    }

    GenerateRandomContext nestedChild(UInt64 salt) const
    {
        return GenerateRandomContext{nestedOptions(options), schemas, rng, deriveSeed(schema_seed, salt)};
    }

    GenerateRandomContext nestedChild(std::string_view name) const
    {
        return GenerateRandomContext{nestedOptions(options), schemas, rng, deriveSeed(schema_seed, name)};
    }
};

/// The threshold is `null_ratio` out of 65536. Comparing the bit-reversed low 16 bits of the draw
/// keeps the legacy `rng() % 16 == 0` decision for the default ratio 1/16 - a reversed value below
/// 4096 means the four lowest bits of the draw are zero - so that existing seeded output does not
/// change, while any other ratio works with a resolution of 1/65536. Exactly one draw per row for
/// every threshold.
bool drawNull(pcg64 & rng, UInt32 threshold)
{
    return __builtin_bitreverse16(static_cast<UInt16>(rng())) < threshold;
}

/// Which of `num_types` alternatives - the variants of a `Variant`, the types of a `Dynamic`, the
/// types of a generated `JSON` key - holds the value of each row. `type_index[i]` is that
/// alternative, or `num_types` when the row holds no value at all: a NULL, or an absent key. There
/// are at most 255 alternatives, so `num_types` never collides with one of them. `counts[t]` is how
/// many values alternative `t` needs; the rows of one alternative keep their relative order, which
/// is the order a `ColumnVariant` consumes its values in.
struct RowTypes
{
    std::vector<UInt8> type_index;
    std::vector<UInt64> counts;
};

/// `pick_type` draws the alternative of a row that holds a value, and is called once for such a row.
template <typename PickType>
RowTypes drawRowTypes(pcg64 & rng, UInt64 limit, UInt32 null_threshold, size_t num_types, PickType && pick_type)
{
    RowTypes result;
    result.type_index.resize(limit);
    result.counts.assign(num_types, 0);

    for (UInt64 i = 0; i < limit; ++i)
    {
        if (drawNull(rng, null_threshold))
        {
            result.type_index[i] = static_cast<UInt8>(num_types);
        }
        else
        {
            const UInt8 index = pick_type();
            result.type_index[i] = index;
            ++result.counts[index];
        }
    }

    return result;
}

size_t estimateValueSize(
    const DataTypePtr & type, const GenerateRandomOptions & options, const RandomSchemas & schemas, UInt64 schema_seed);

/// The widest of the alternatives a `Dynamic`-like position can hold in a row - the types of a
/// `Dynamic`, the variants of a `Variant`, the types of a generated `JSON` key. The `i`-th
/// alternative is seeded with the `i`-th child seed, the way `fillColumnWithRandomData` seeds it.
size_t estimateWidestAlternative(
    const DataTypes & alternatives, const GenerateRandomOptions & options, const RandomSchemas & schemas, UInt64 schema_seed)
{
    size_t res = 0;
    for (size_t i = 0; i < alternatives.size(); ++i)
        res = std::max(res, estimateValueSize(alternatives[i], options, schemas, deriveSeed(schema_seed, i)));
    return res;
}

/// What a value of a type without a variant of its own costs on top of the value itself: the offset
/// of the encoded value inside the shared variant and the binary encoding of its type, which is never
/// longer than the name of the type. Zero when every type of the schema has a variant of its own.
size_t estimateSharedVariantOverhead(const DynamicSchema & schema)
{
    size_t res = 0;
    for (size_t i = schema.num_dedicated; i < schema.types.size(); ++i)
        res = std::max(res, sizeof(UInt64) + schema.types[i]->getName().size());
    return res;
}

size_t estimateValueSize(
    const DataTypePtr & type, const GenerateRandomOptions & options, const RandomSchemas & schemas, UInt64 schema_seed)
{
    if (type->haveMaximumSizeOfValue())
        return type->getMaximumSizeOfValueInMemory();

    TypeIndex idx = type->getTypeId();

    switch (idx)
    {
        case TypeIndex::String:
        {
            return options.max_string_length + sizeof(UInt64);
        }

        /// The logic in this function should reflect the logic of fillColumnWithRandomData.
        case TypeIndex::Array:
        {
            auto nested_type = typeid_cast<const DataTypeArray &>(*type).getNestedType();
            return sizeof(size_t) + estimateValueSize(nested_type, nestedOptions(options), schemas, deriveSeed(schema_seed, 0));
        }

        case TypeIndex::Map:
        {
            const DataTypePtr & nested_type = typeid_cast<const DataTypeMap &>(*type).getNestedType();
            return sizeof(size_t) + estimateValueSize(nested_type, nestedOptions(options), schemas, deriveSeed(schema_seed, 0));
        }

        case TypeIndex::Tuple:
        {
            auto elements = typeid_cast<const DataTypeTuple *>(type.get())->getElements();
            const size_t tuple_size = elements.size();
            size_t res = 0;

            for (size_t i = 0; i < tuple_size; ++i)
                res += estimateValueSize(elements[i], options, schemas, deriveSeed(schema_seed, i));

            return res;
        }

        case TypeIndex::Nullable:
        {
            auto nested_type = typeid_cast<const DataTypeNullable &>(*type).getNestedType();
            return 1 + estimateValueSize(nested_type, options, schemas, schema_seed);
        }

        case TypeIndex::LowCardinality:
        {
            auto nested_type = typeid_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
            return sizeof(size_t) + estimateValueSize(nested_type, options, schemas, schema_seed);
        }

        case TypeIndex::Dynamic:
        {
            /// One discriminator byte and one offset per row on top of the widest type.
            const auto & schema = schemas.dynamic(schema_seed);
            return 1 + sizeof(UInt64) + estimateWidestAlternative(schema.types, options, schemas, schema_seed)
                + estimateSharedVariantOverhead(schema);
        }

        case TypeIndex::Variant:
        {
            /// One discriminator byte and one offset per row on top of the widest variant.
            const DataTypes & variants = typeid_cast<const DataTypeVariant &>(*type).getVariants();
            return 1 + sizeof(UInt64) + estimateWidestAlternative(variants, options, schemas, schema_seed);
        }

        case TypeIndex::Object:
        {
            const auto & object_type = typeid_cast<const DataTypeObject &>(*type);
            const GenerateRandomOptions nested_options = nestedOptions(options);

            size_t res = 0;
            for (const auto & [path, path_type] : object_type.getTypedPaths())
                res += estimateValueSize(path_type, nested_options, schemas, deriveSeed(schema_seed, path));

            /// A dynamic path costs a discriminator and an offset per row; a shared one stores its
            /// name and the offsets of the name and of the value instead.
            const auto & schema = schemas.json(schema_seed);
            for (size_t i = 0; i < schema.leaves.size(); ++i)
            {
                const auto & leaf = schema.leaves[i];
                const size_t overhead
                    = i < schema.num_dynamic_leaves ? 1 + sizeof(UInt64) : leaf.path.size() + 2 * sizeof(UInt64);
                res += overhead
                    + estimateWidestAlternative(leaf.types.types, nested_options, schemas, deriveSeed(schema_seed, leaf.path))
                    + estimateSharedVariantOverhead(leaf.types);
            }
            return res;
        }

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'GenerateRandom' is not implemented for type {}", type->getName());
    }
}

ColumnPtr fillColumnWithRandomData(const DataTypePtr & type, UInt64 limit, const GenerateRandomContext & ctx)
{
    pcg64 & rng = ctx.rng;
    const UInt64 max_array_length = ctx.options.max_array_length;
    const UInt64 max_string_length = ctx.options.max_string_length;
    const bool fuzzy = ctx.options.fuzzy;

    TypeIndex idx = type->getTypeId();

    switch (idx)
    {
        case TypeIndex::String:
        {
            /// Mostly the same as the implementation of randomPrintableASCII function.

            auto column = ColumnString::create();
            ColumnString::Chars & data_to = column->getChars();
            ColumnString::Offsets & offsets_to = column->getOffsets();
            offsets_to.resize(limit);

            IColumn::Offset offset = 0;
            for (size_t row_num = 0; row_num < limit; ++row_num)
            {
                if (fuzzy)
                {
                    appendFuzzyRandomString(data_to, max_string_length, rng);
                    offset = data_to.size();
                }
                else
                {
                    size_t length = rng() % (max_string_length + 1);    /// Slow

                    IColumn::Offset next_offset = offset + length;
                    data_to.resize(next_offset);
                    fillBufferWithRandomPrintableASCIIBytes(reinterpret_cast<char *>(data_to.data() + offset), length, rng);
                    offset = next_offset;
                }
                offsets_to[row_num] = offset;
            }

            return column;
        }

        case TypeIndex::Enum8:
        {
            auto column = ColumnVector<Int8>::create();
            auto values = typeid_cast<const DataTypeEnum<Int8> *>(type.get())->getValues();
            auto & data = column->getData();
            data.resize(limit);

            size_t enum_size = values.size();
            for (UInt64 i = 0; i < limit; ++i)
            {
                size_t off = rng() % enum_size;
                data[i] = values[off].second;
            }

            return column;
        }

        case TypeIndex::Enum16:
        {
            auto column = ColumnVector<Int16>::create();
            auto values = typeid_cast<const DataTypeEnum<Int16> *>(type.get())->getValues();
            auto & data = column->getData();
            data.resize(limit);

            size_t enum_size = values.size();
            for (UInt64 i = 0; i < limit; ++i)
            {
                size_t off = rng() % enum_size;
                data[i] = values[off].second;
            }

            return column;
        }

        case TypeIndex::Array:
        {
            auto nested_type = typeid_cast<const DataTypeArray &>(*type).getNestedType();

            auto offsets_column = ColumnVector<ColumnArray::Offset>::create();
            auto & offsets = offsets_column->getData();

            UInt64 offset = 0;
            offsets.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
            {
                offset += static_cast<UInt64>(rng()) % (max_array_length + 1);
                offsets[i] = offset;
            }

            auto data_column = fillColumnWithRandomData(nested_type, offset, ctx.nestedChild(0));

            return ColumnArray::create(data_column, std::move(offsets_column));
        }

        case TypeIndex::Map:
        {
            const DataTypePtr & nested_type = typeid_cast<const DataTypeMap &>(*type).getNestedType();
            auto nested_column = fillColumnWithRandomData(nested_type, limit, ctx.nestedChild(0));
            return ColumnMap::create(nested_column);
        }

        case TypeIndex::Tuple:
        {
            auto elements = typeid_cast<const DataTypeTuple *>(type.get())->getElements();
            if (elements.empty())
                return ColumnTuple::create(limit);

            const size_t tuple_size = elements.size();
            Columns tuple_columns(tuple_size);

            for (size_t i = 0; i < tuple_size; ++i)
                tuple_columns[i] = fillColumnWithRandomData(elements[i], limit, ctx.child(i));

            return ColumnTuple::create(std::move(tuple_columns));
        }

        case TypeIndex::Nullable:
        {
            auto nested_type = typeid_cast<const DataTypeNullable &>(*type).getNestedType();
            auto nested_column = fillColumnWithRandomData(nested_type, limit, ctx);

            const UInt32 null_threshold = ctx.options.nullThreshold();
            auto null_map_column = ColumnUInt8::create();
            auto & null_map = null_map_column->getData();
            null_map.resize(limit);
            for (UInt64 i = 0; i < limit; ++i)
                null_map[i] = drawNull(rng, null_threshold);

            return ColumnNullable::create(nested_column, std::move(null_map_column));
        }

        case TypeIndex::UInt8:
        {
            auto column = ColumnUInt8::create();
            auto & data = column->getData();
            data.resize(limit);
            if (isBool(type))
            {
                for (size_t i = 0; i < limit; ++i)
                    data[i] = rng() % 2;
            }
            else
            {
                fillBufferWithRandomNumbers<UInt8>(reinterpret_cast<char *>(data.data()), limit, rng, fuzzy);
            }
            return column;
        }
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::Date:
        {
            auto column = ColumnUInt16::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt16>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Date32:
        {
            auto column = ColumnInt32::create();
            column->getData().resize(limit);

            if (fuzzy)
                /// Ignore range because out-of-range Date32 values can appear in practice, e.g. `toDate(0) + 2000000000`.
                fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            else
                for (size_t i = 0; i < limit; ++i)
                    column->getData()[i] = (rng() % static_cast<UInt64>(DATE_LUT_SIZE)) - DAYNUM_OFFSET_EPOCH;

            return column;
        }
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::DateTime:
        {
            auto column = ColumnUInt32::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::UInt64:
        {
            auto column = ColumnUInt64::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt64>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::UInt128:
        {
            auto column = ColumnUInt128::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt128>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::UInt256:
        {
            auto column = ColumnUInt256::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt256>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::UUID:
        {
            auto column = ColumnUUID::create();
            column->getData().resize(limit);
            /// NOTE This is slightly incorrect as random UUIDs should have fixed version 4.
            fillBufferWithRandomNumbers<UInt128>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int8:
        {
            auto column = ColumnInt8::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt8>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int16:
        {
            auto column = ColumnInt16::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt16>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int32:
        {
            auto column = ColumnInt32::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int64:
        {
            auto column = ColumnInt64::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt64>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int128:
        {
            auto column = ColumnInt128::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt128>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Int256:
        {
            auto column = ColumnInt256::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt256>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Float32:
        {
            auto column = ColumnFloat32::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::Float64:
        {
            auto column = ColumnFloat64::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt64>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }

        case TypeIndex::Decimal32:
        {
            const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal32> &>(*type);
            auto column = decimal_type.createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal32> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// Generate numbers from range [-10^P + 1, 10^P - 1]
            Int32 range = common::exp10_i32(decimal_type.getPrecision());
            fillRandomDecimals<Int32>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }
        case TypeIndex::Decimal64:
        {
            const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal64> &>(*type);
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal64> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// Generate numbers from range [-10^P + 1, 10^P - 1]
            Int64 range = common::exp10_i64(decimal_type.getPrecision());
            fillRandomDecimals<Int64>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }
        case TypeIndex::Decimal128:
        {
            const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal128> &>(*type);
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal128> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// Generate numbers from range [-10^P + 1, 10^P - 1]
            Int128 range = common::exp10_i128(decimal_type.getPrecision());
            fillRandomDecimals<Int128>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }
        case TypeIndex::Decimal256:
        {
            const auto & decimal_type = assert_cast<const DataTypeDecimal<Decimal256> &>(*type);
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Decimal256> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// Generate numbers from range [-10^P + 1, 10^P - 1]
            Int256 range = common::exp10_i256(decimal_type.getPrecision());
            fillRandomDecimals<Int256>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }
        case TypeIndex::FixedString:
        {
            size_t n = typeid_cast<const DataTypeFixedString &>(*type).getN();
            auto column = ColumnFixedString::create(n);
            column->getChars().resize_fill(limit * n);
            if (fuzzy)
            {
                ColumnString::Chars temp;
                for (size_t row_num = 0; row_num < limit; ++row_num)
                {
                    temp.clear();
                    appendFuzzyRandomString(temp, n, rng);
                    chassert(temp.size() <= n);
                    memcpy(column->getChars().data() + row_num * n, temp.data(), temp.size());
                }
            }
            else
            {
                fillBufferWithRandomBytes(reinterpret_cast<char *>(column->getChars().data()), limit * n, rng);
            }
            return column;
        }
        case TypeIndex::DateTime64:
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<DateTime64> &>(*column);
            column_concrete.getData().resize(limit);
            UInt64 range = (1ULL << 32) * intExp10(typeid_cast<const DataTypeDateTime64 &>(*type).getScale());
            if (fuzzy)
                fillRandomDecimals<Int64>(reinterpret_cast<char *>(column_concrete.getData().data()), limit, static_cast<Int64>(range), rng, fuzzy);
            else
            {
                /// Keep the old behavior for non-fuzzy mode: use UInt64 arithmetic
                for (size_t i = 0; i < limit; ++i)
                    column_concrete.getData()[i] = rng() % range;
            }
            return column;
        }
        case TypeIndex::LowCardinality:
        {
            /// We are generating the values using the same random distribution as for full columns
            /// so it's not in fact "low cardinality",
            /// but it's ok for testing purposes, because the LowCardinality data type supports high cardinality data as well.

            auto nested_type = typeid_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
            auto nested_column = fillColumnWithRandomData(nested_type, limit, ctx);

            auto column = type->createColumn();
            typeid_cast<ColumnLowCardinality &>(*column).insertRangeFromFullColumn(*nested_column, 0, limit);

            return column;
        }
        case TypeIndex::IPv4:
        {
            auto column = ColumnIPv4::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt32>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }
        case TypeIndex::IPv6:
        {
            auto column = ColumnIPv6::create();
            column->getData().resize(limit);
            /// IPv6 is always stored as big-endian in memory, so we don't use fillBufferWithRandomNumbers here.
            fillBufferWithRandomBytes(reinterpret_cast<char *>(column->getData().data()), limit * sizeof(IPv6), rng);
            return column;
        }
        case TypeIndex::MacAddress:
        {
            auto column = ColumnVector<MacAddress>::create();
            auto & data = column->getData();
            data.resize(limit);
            fillBufferWithRandomNumbers<UInt64>(reinterpret_cast<char *>(data.data()), limit, rng, fuzzy);
            /// Random 64-bit values break the invariant that the upper 16 bits are zero, so mask them off.
            for (size_t i = 0; i < limit; ++i)
                data[i] = MacAddress(data[i].toUnderType());
            return column;
        }

        case TypeIndex::BFloat16:
        {
            auto column = ColumnBFloat16::create();
            column->getData().resize(limit);
            fillBufferWithRandomNumbers<UInt16>(reinterpret_cast<char *>(column->getData().data()), limit, rng, fuzzy);
            return column;
        }

        case TypeIndex::Time:
        {
            auto column = ColumnInt32::create();
            auto & data = column->getData();
            data.resize(limit);
            /// Generate values from range [-3599999, 3599999]; out-of-range values would only
            /// saturate to that boundary on text output. In `fuzzy` mode the range is ignored,
            /// the same way it is for `Date32`.
            fillRandomDecimals<Int32>(
                reinterpret_cast<char *>(data.data()), limit, static_cast<Int32>(MAX_TIME_SECONDS + 1), rng, fuzzy);
            return column;
        }

        case TypeIndex::Time64:
        {
            auto column = type->createColumn();
            auto & column_concrete = typeid_cast<ColumnDecimal<Time64> &>(*column);
            auto & data = column_concrete.getData();
            data.resize(limit);
            /// `Time64` is stored as `Int64` ticks: seconds scaled by 10^scale.
            /// The largest value fits `Int64` for every legal scale (up to 9).
            Int64 range
                = (MAX_TIME_SECONDS + 1) * static_cast<Int64>(intExp10(typeid_cast<const DataTypeTime64 &>(*type).getScale()));
            fillRandomDecimals<Int64>(reinterpret_cast<char *>(data.data()), limit, range, rng, fuzzy);
            return column;
        }

        case TypeIndex::Variant:
        {
            const auto & variants = typeid_cast<const DataTypeVariant &>(*type).getVariants();
            const size_t num_variants = variants.size();

            /// A `Variant` has no shape to draw: every variant is equally likely, and one extra
            /// outcome produces an untyped NULL, so that state is exercised too.
            auto [type_index, counts] = drawRowTypes(
                rng, limit, ctx.options.nullThreshold(), num_variants, [&] { return static_cast<UInt8>(rng() % num_variants); });

            /// Each variant is filled in one call; `ColumnVariant` consumes its values in row order,
            /// so the k-th row with discriminator `d` gets the k-th value of variant `d`.
            MutableColumns variant_columns(num_variants);
            for (size_t i = 0; i < num_variants; ++i)
                variant_columns[i] = fillColumnWithRandomData(variants[i], counts[i], ctx.child(i))->assumeMutable();

            /// `getVariants` is already in global discriminator order, so a variant is its own discriminator.
            std::vector<UInt8> global_discriminator(num_variants);
            for (size_t i = 0; i < num_variants; ++i)
                global_discriminator[i] = static_cast<UInt8>(i);

            return buildVariantColumn(*type, global_discriminator, type_index, std::move(variant_columns));
        }

        case TypeIndex::Dynamic:
        {
            const auto & schema = ctx.schemas.dynamic(ctx.schema_seed);
            const size_t num_types = schema.types.size();

            auto [type_index, counts] = drawRowTypes(
                rng, limit, ctx.options.nullThreshold(), num_types, [&] { return static_cast<UInt8>(rng() % num_types); });

            MutableColumns type_columns(num_types);
            for (size_t i = 0; i < num_types; ++i)
                type_columns[i] = fillColumnWithRandomData(schema.types[i], counts[i], ctx.child(i))->assumeMutable();

            return buildDynamicColumn(schema, type_index, std::move(type_columns));
        }

        case TypeIndex::Object:
        {
            const auto & object_type = typeid_cast<const DataTypeObject &>(*type);
            const auto & schema = ctx.schemas.json(ctx.schema_seed);

            /// The typed paths of the type are always present. They are filled in sorted order so
            /// that the values do not depend on the iteration order of an unordered map.
            std::vector<String> typed_paths;
            typed_paths.reserve(object_type.getTypedPaths().size());
            for (const auto & [path, _] : object_type.getTypedPaths())
                typed_paths.push_back(path);
            std::sort(typed_paths.begin(), typed_paths.end());

            UnorderedMapWithMemoryTracking<String, MutableColumnPtr> typed_path_columns;
            for (const auto & path : typed_paths)
                typed_path_columns[path]
                    = fillColumnWithRandomData(object_type.getTypedPaths().at(path), limit, ctx.nestedChild(path))->assumeMutable();

            /// Every generated path becomes a `Dynamic` column of `limit` rows; a NULL there means
            /// that the key is absent from that row.
            std::vector<String> leaf_paths;
            MutableColumns leaf_columns;
            leaf_paths.reserve(schema.leaves.size());
            leaf_columns.reserve(schema.leaves.size());

            for (const auto & leaf : schema.leaves)
            {
                const size_t num_types = leaf.types.types.size();

                /// A leaf has at most two types, and the second one - the drift - is rare.
                auto [type_index, counts] = drawRowTypes(
                    rng,
                    limit,
                    leaf.absent_threshold,
                    num_types,
                    [&] { return static_cast<UInt8>(num_types > 1 && drawChance(rng, TYPE_DRIFT_ROW_SHARE) ? 1 : 0); });

                auto leaf_ctx = ctx.nestedChild(leaf.path);
                MutableColumns value_columns(num_types);
                for (size_t i = 0; i < num_types; ++i)
                {
                    value_columns[i] = fillColumnWithRandomData(leaf.types.types[i], counts[i], leaf_ctx.child(i))->assumeMutable();
                    raiseUInt64ValuesAboveInt64Max(*value_columns[i]);
                }

                leaf_paths.push_back(leaf.path);
                leaf_columns.push_back(buildDynamicColumn(leaf.types, type_index, std::move(value_columns))->assumeMutable());
            }

            /// `max_dynamic_paths`, `max_dynamic_paths_upper_bound` and `global_max_dynamic_paths` are
            /// all the limit of the type, the way `DataTypeObject::createColumn` initialises them.
            const size_t max_dynamic_paths = object_type.getMaxDynamicPaths();
            UnorderedMapWithMemoryTracking<String, MutableColumnPtr> no_dynamic_paths;
            auto object = ColumnObject::create(
                std::move(typed_path_columns),
                std::move(no_dynamic_paths),
                DataTypeObject::getTypeOfSharedData()->createColumn(),
                max_dynamic_paths,
                max_dynamic_paths,
                max_dynamic_paths,
                object_type.getMaxDynamicTypes());

            /// Takes the leading `max_dynamic_paths` leaves as dynamic paths of the column and
            /// serialises the rest into the shared data, sorted by path, one offset per row.
            unflattenAndInsertPaths(leaf_paths, std::move(leaf_columns), *object, limit);

            if (object->size() != limit)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Generated a `JSON` column of {} rows instead of {}", object->size(), limit);
            object->validateDynamicPathsSizes();

            return object;
        }

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'GenerateRandom' is not implemented for type {}", type->getName());
    }
}

/// To support `Nested` types, we will collect them to a single `Array` of `Tuple`.
Block prepareBlockToFill(const Block & block)
{
    Block res;
    for (const auto & column : Nested::collect(block.getNamesAndTypesList()))
        res.insert(ColumnWithTypeAndName(column.type, column.name));
    return res;
}

}

ColumnPtr fillColumnWithRandomData(
    DataTypePtr type, UInt64 limit, UInt64 max_array_length, UInt64 max_string_length, pcg64 & rng, bool fuzzy)
{
    GenerateRandomOptions options;
    options.max_array_length = max_array_length;
    options.max_string_length = max_string_length;
    options.fuzzy = fuzzy;

    /// There is a single column here and its name is not observable from the outside, so any stable
    /// name will do. Its schema is seeded from one draw of the value generator.
    const UInt64 column_seed = deriveSeed(rng(), "x");
    auto schemas = RandomSchemas::buildForColumn(type, column_seed, options);

    GenerateRandomContext ctx{options, *schemas, rng, column_seed};
    return fillColumnWithRandomData(type, limit, ctx);
}

namespace
{

class GenerateSource final : public ISource
{
public:
    GenerateSource(
        UInt64 block_size_,
        const GenerateRandomOptions & options_,
        std::shared_ptr<const RandomSchemas> schemas_,
        UInt64 storage_seed_,
        UInt64 stream_seed_,
        Block block_to_fill_,
        GenerateRandomStatePtr state_)
        : ISource(std::make_shared<const Block>(Nested::flattenNested(block_to_fill_)))
        , block_size(block_size_)
        , options(options_)
        , schemas(std::move(schemas_))
        , storage_seed(storage_seed_)
        , block_to_fill(std::move(block_to_fill_))
        , rng(stream_seed_)
        , shared_state(state_)
    {
    }

    String getName() const override { return "GenerateRandom"; }

protected:
    Chunk generate() override
    {
        Columns columns;
        columns.reserve(block_to_fill.columns());

        GenerateRandomContext ctx{options, *schemas, rng, 0};
        for (const auto & elem : block_to_fill)
        {
            ctx.schema_seed = deriveSeed(storage_seed, elem.name);
            columns.emplace_back(fillColumnWithRandomData(elem.type, block_size, ctx));
        }

        columns = Nested::flattenNested(block_to_fill.cloneWithColumns(columns)).getColumns();

        UInt64 total_rows = shared_state->add_total_rows.fetch_and(0);
        if (total_rows)
            addTotalRowsApprox(total_rows);

        auto chunk = Chunk{std::move(columns), block_size};
        progress(chunk.getNumRows(), chunk.bytes());

        return chunk;
    }

private:
    UInt64 block_size;
    GenerateRandomOptions options;
    std::shared_ptr<const RandomSchemas> schemas;
    UInt64 storage_seed;
    Block block_to_fill;

    pcg64 rng;

    GenerateRandomStatePtr shared_state;
};

}


StorageGenerateRandom::StorageGenerateRandom(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const String & comment,
    const GenerateRandomOptions & options_,
    const std::optional<UInt64> & random_seed_)
    : StorageWithCommonVirtualColumns(table_id_), options(options_)
{
    static constexpr size_t MAX_ARRAY_SIZE = 1 << 30;
    static constexpr size_t MAX_STRING_SIZE = 1 << 30;

    if (options.max_array_length > MAX_ARRAY_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in GenerateRandom: {}, maximum: {}",
                        options.max_array_length, MAX_ARRAY_SIZE);
    if (options.max_string_length > MAX_STRING_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size in GenerateRandom: {}, maximum: {}",
                        options.max_string_length, MAX_STRING_SIZE);

    random_seed = random_seed_ ? sipHash64(*random_seed_) : randomSeed();
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageGenerateRandom::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


void registerStorageGenerateRandom(StorageFactory & factory);
void registerStorageGenerateRandom(StorageFactory & factory)
{
    factory.registerStorage("GenerateRandom", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage GenerateRandom requires at most three arguments: "
                            "random_seed, max_string_length, max_array_length.");

        std::optional<UInt64> random_seed;
        GenerateRandomOptions options;

        if (!engine_args.empty())
        {
            engine_args[0] = evaluateConstantExpressionAsLiteral(engine_args[0], args.getLocalContext());
            random_seed = checkAndGetLiteralArgument<UInt64>(engine_args[0], "random_seed");
        }

        if (engine_args.size() >= 2)
        {
            engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.getLocalContext());
            options.max_string_length = checkAndGetLiteralArgument<UInt64>(engine_args[1], "max_string_length");
        }

        if (engine_args.size() == 3)
        {
            engine_args[2] = evaluateConstantExpressionAsLiteral(engine_args[2], args.getLocalContext());
            options.max_array_length = checkAndGetLiteralArgument<UInt64>(engine_args[2], "max_array_length");
        }

        GenerateRandomSettings settings;
        settings.loadFromQuery(*args.storage_def);
        settings.sanityCheck();

        options.null_ratio = settings[GenerateRandomSetting::null_ratio];
        options.max_json_depth = settings[GenerateRandomSetting::max_json_depth];
        options.max_json_keys_per_object = settings[GenerateRandomSetting::max_json_keys_per_object];

        return std::make_shared<StorageGenerateRandom>(args.table_id, args.columns, args.comment, options, random_seed);
    },
    {
        .supports_settings = true,
        .has_builtin_setting_fn = GenerateRandomSettings::hasBuiltin,
    },
    Documentation{
        .description = R"DOCS_MD(
The GenerateRandom table engine produces random data for given table schema.

Usage examples:

- Use in test to populate reproducible large table.
- Generate random input for fuzzing tests.

## Usage in ClickHouse Server {#usage-in-clickhouse-server}

```sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
[SETTINGS null_ratio = ..., max_json_depth = ..., max_json_keys_per_object = ...]
```

The `max_array_length` and `max_string_length` parameters specify maximum length of all
array or map columns and strings correspondingly in generated data.

The `SETTINGS` clause controls how `Nullable`, `Variant`, `Dynamic` and `JSON` values are generated:
`null_ratio` (default `0.0625`) is the probability that a value is `NULL` and the base probability
that a `JSON` key is absent from a row - a minority of sparse keys are absent several times more
often, `max_json_depth` (default `3`) bounds the nesting depth of generated `JSON` objects,
and `max_json_keys_per_object` (default `8`) bounds the number of generated keys on one level of an
object. The [`generateRandom`](/reference/functions/table-functions/generate) table function accepts
the same settings.

Generate table engine supports only `SELECT` queries.

It supports all [DataTypes](/reference/data-types/index) that can be stored in a table,
except `AggregateFunction`, `Interval`, `Nothing` and `QBit`.

## Example {#example}

**1.** Set up the `generate_engine_table` table:

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Query the data:

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## Details of Implementation {#details-of-implementation}

- Not supported:
  - `ALTER`
  - `SELECT ... SAMPLE`
  - `INSERT`
  - Indices
  - Replication
)DOCS_MD",
        .syntax = "ENGINE = GenerateRandom([random_seed[, max_string_length[, max_array_length]]]) "
                  "[SETTINGS null_ratio = ..., max_json_depth = ..., max_json_keys_per_object = ...]",
        .related = {"FuzzJSON", "FuzzQuery"}});
}

Pipe StorageGenerateRandom::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    const ColumnsDescription & our_columns = storage_snapshot->metadata->getColumns();
    Block block_header;
    for (const auto & name : column_names)
    {
        const auto & name_type = our_columns.get(name);
        MutableColumnPtr column = name_type.type->createColumn();
        block_header.insert({std::move(column), name_type.type, name_type.name});
    }

    /// `Nested` columns are generated as a single `Array` of `Tuple` and flattened afterwards, so
    /// both the seeds and the size estimate below refer to the collected block.
    Block block_to_fill = prepareBlockToFill(block_header);

    auto schemas = RandomSchemas::build(block_to_fill.getNamesAndTypesList(), random_seed, options);

    /// Correction of block size for wide tables.
    size_t preferred_block_size_bytes = context->getSettingsRef()[Setting::preferred_block_size_bytes];
    if (preferred_block_size_bytes)
    {
        size_t estimated_row_size_bytes = 0;
        for (const auto & elem : block_to_fill)
            estimated_row_size_bytes += estimateValueSize(elem.type, options, *schemas, deriveSeed(random_seed, elem.name));

        size_t estimated_block_size_bytes = 0;
        if (common::mulOverflow(max_block_size, estimated_row_size_bytes, estimated_block_size_bytes))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large estimated block size in GenerateRandom table: its estimation leads to 64bit overflow");

        if (estimated_block_size_bytes > preferred_block_size_bytes)
        {
            max_block_size = static_cast<size_t>(static_cast<double>(max_block_size) * (static_cast<double>(preferred_block_size_bytes) / static_cast<double>(estimated_block_size_bytes)));
            if (max_block_size == 0)
                max_block_size = 1;
        }
    }

    UInt64 query_limit = query_info.trivial_limit;
    if (query_limit && num_streams * max_block_size > query_limit)
    {
        /// We want to avoid spawning more streams than necessary
        num_streams = std::min(
            num_streams, static_cast<size_t>(query_limit / max_block_size + (query_limit % max_block_size != 0)));
    }

    /// This engine generates its data, so only a trivial `LIMIT` bounds the number of sources.
    static constexpr size_t max_sources = 65536;
    if (num_streams > max_sources)
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Too many streams for a `GenerateRandom` table read (the maximum is {}). "
            "Lower `max_streams_to_max_threads_ratio` or `max_threads`",
            max_sources);

    Pipes pipes;
    pipes.reserve(num_streams);

    /// Will create more seed values for each source from initial seed.
    pcg64 generate(random_seed);

    auto shared_state = std::make_shared<GenerateRandomState>(query_info.trivial_limit);

    for (UInt64 i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<GenerateSource>(
            max_block_size, options, schemas, random_seed, generate(), block_to_fill, shared_state);
        pipes.emplace_back(std::move(source));
    }

    return Pipe::unitePipes(std::move(pipes));
}

}
