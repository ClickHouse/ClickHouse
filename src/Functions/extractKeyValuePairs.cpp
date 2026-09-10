#include <Functions/extractKeyValuePairs.h>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <base/EnumReflection.h>

#include <bit>
#include <cstring>

#if defined(__SSSE3__)
#    include <tmmintrin.h>
#endif
#if defined(__aarch64__)
#    include <arm_neon.h>
#endif


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 extract_key_value_pairs_max_pairs_per_row;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LIMIT_EXCEEDED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

constexpr char ESCAPE_CHARACTER = '\\';

#if defined(__SSSE3__) || defined(__aarch64__)

/// GCC/Clang vector extensions lower each lane operation to one SSE/NEON instruction.
/// Only the byte permutation has no portable spelling.
using UInt8x16 = UInt8 __attribute__((vector_size(16)));
using UInt64x2 = UInt64 __attribute__((vector_size(16)));

/// Returns a vector with `table[indexes[i]]` in lane `i`; every index must be less than 16.
inline UInt8x16 lookupBytes(UInt8x16 table, UInt8x16 indexes)
{
#if defined(__SSSE3__)
    return std::bit_cast<UInt8x16>(_mm_shuffle_epi8(std::bit_cast<__m128i>(table), std::bit_cast<__m128i>(indexes)));
#else
    return std::bit_cast<UInt8x16>(vqtbl1q_u8(std::bit_cast<uint8x16_t>(table), std::bit_cast<uint8x16_t>(indexes)));
#endif
}

#endif

/// A set of bytes with a vectorized search for the first byte that is (or is not) in the set.
/// The lookup tables are built once per set, so a search has no setup cost.
class ByteSet
{
public:
    void add(char c)
    {
        auto byte = static_cast<UInt8>(c);
        if (table[byte])
            return;

        table[byte] = true;

        /// Tables for the vectorized search, which can only look up
        /// 16-entry tables and therefore classifies a byte by its two nibbles:
        ///
        /// - Every high nibble that occurs in the set gets its own bit (`high_nibble_bit`).
        /// - `high_nibble_table[high]` is that bit, or 0 if no member has this high nibble.
        /// - `low_nibble_table[low]` is the union of the bits of all high nibbles that are
        ///   combined with this low nibble in some member.
        /// - A byte is in the set iff the two lookups share a bit:
        ///   `low_nibble_table[low] & high_nibble_table[high] != 0`.
        ///
        /// The bits fit in one byte, so at most 8 distinct high nibbles are supported. That is
        /// enough for any set of ASCII characters; a set that needs more is searched by the
        /// scalar loop only.
        UInt8 high = byte >> 4;
        UInt8 low = byte & 0x0F;

        if (!high_nibble_bit[high])
        {
            if (num_high_nibbles == 8)
            {
                vectorized = false;
                return;
            }

            high_nibble_bit[high] = static_cast<UInt8>(1u << num_high_nibbles);
            ++num_high_nibbles;
        }

        low_nibble_table[low] |= high_nibble_bit[high];
        high_nibble_table[high] = high_nibble_bit[high];
    }

    ALWAYS_INLINE bool contains(char c) const
    {
        return table[static_cast<UInt8>(c)];
    }

    /// Returns the first byte in [begin, end) whose membership in the set equals `positive`, or `end`.
    template <bool positive>
    const char * find(const char * begin, const char * end) const
    {
        const char * pos = begin;

        /// Most keys and values are short, so the first bytes are checked one by one
        /// before paying for a vector iteration that would mostly look past the token.
        const char * scalar_end = end - pos > SCALAR_PREFIX ? pos + SCALAR_PREFIX : end;
        for (; pos < scalar_end; ++pos)
        {
            if (contains(*pos) == positive)
                return pos;
        }

        if (vectorized)
            pos = findVectorized<positive>(pos, end);

        for (; pos < end; ++pos)
        {
            if (contains(*pos) == positive)
                return pos;
        }

        return end;
    }

private:
    static constexpr ptrdiff_t SCALAR_PREFIX = 16;
    static constexpr ptrdiff_t VECTOR_SIZE = 16;

    /// Scans whole 16-byte blocks. Returns the position of the first byte matching the search,
    /// or the position from which fewer than 16 bytes remain.
    template <bool positive>
    const char * findVectorized(const char * pos, const char * end) const
    {
#if defined(__SSSE3__) || defined(__aarch64__)
        const auto low_table = std::bit_cast<UInt8x16>(low_nibble_table);
        const auto high_table = std::bit_cast<UInt8x16>(high_nibble_table);

        for (; end - pos >= VECTOR_SIZE; pos += VECTOR_SIZE)
        {
            UInt8x16 bytes;
            memcpy(&bytes, pos, VECTOR_SIZE);

            UInt8x16 low = lookupBytes(low_table, bytes & 0x0F);
            UInt8x16 high = lookupBytes(high_table, bytes >> 4);

            /// 0xFF in the lanes of the bytes that match the search, 0x00 in the others.
            auto match = (low & high) != UInt8x16{};
            if constexpr (!positive)
                match = ~match;

            /// The first matching byte is the lowest non-zero byte, little-endian.
            const auto halves = std::bit_cast<UInt64x2>(match);
            if (halves[0])
                return pos + std::countr_zero(halves[0]) / 8;
            if (halves[1])
                return pos + 8 + std::countr_zero(halves[1]) / 8;
        }
#endif

        return pos;
    }

    bool table[256]{};
    UInt8 low_nibble_table[16]{};
    UInt8 high_nibble_table[16]{};
    UInt8 high_nibble_bit[16]{};
    size_t num_high_nibbles = 0;
    bool vectorized = true;
};

/// A key or a value being read. Without escape sequences it is a view into the input;
/// after the first decoded escape sequence it is accumulated in `buffer`.
class Token
{
public:
    explicit Token(std::string & buffer_) : buffer(buffer_) {}

    void start(const char * pos)
    {
        chunk_begin = pos;
        in_buffer = false;
    }

    /// Decodes the escape sequence at `pos` (the backslash) and moves `pos` past it.
    /// Returns false if the sequence is invalid; `pos` is still moved past the consumed bytes.
    bool consumeEscapeSequence(const char *& pos, const char * end)
    {
        if (!in_buffer)
        {
            buffer.clear();
            in_buffer = true;
        }

        buffer.append(chunk_begin, pos);

        ReadBufferFromMemory in(pos, end - pos);
        bool ok = parseComplexEscapeSequence(buffer, in);
        pos = in.position();
        chunk_begin = pos;
        return ok;
    }

    /// Returns the token read so far, ending at `pos`.
    std::string_view finish(const char * pos)
    {
        if (!in_buffer)
            return {chunk_begin, pos};

        buffer.append(chunk_begin, pos);
        chunk_begin = pos;
        return buffer;
    }

private:
    std::string & buffer;
    const char * chunk_begin = nullptr;
    bool in_buffer = false;
};

/// What ended the reading of a token.
enum class Stop
{
    KeyValueDelimiter,
    PairDelimiter,
    QuotingCharacter,
    End,
    InvalidEscapeSequence,
};

/// Outcome of reading a key or a value.
enum class ReadResult
{
    /// The token was read: for a key `pos` is at the start of the value, for a value the pair is complete.
    Ok,
    /// The token is invalid, go back to waiting for a key.
    Discard,
    /// The input is over.
    End,
};

}

struct KeyValuePairExtractor::Impl
{
    Configuration configuration;

    /// Bytes skipped while waiting for a key: the delimiters, and the escape character with escaping.
    ByteSet waiting_key_bytes;
    /// Bytes that end an unquoted key: both delimiters, the quoting character
    /// unless the strategy is `ACCEPT`, and the escape character with escaping.
    ByteSet key_stop_bytes;
    /// Bytes that end an unquoted value: same as for a key, except the key-value delimiter is a regular byte.
    ByteSet value_stop_bytes;
    /// Bytes that end a quoted key or value: the quoting character, and the escape character with escaping.
    ByteSet quoted_stop_bytes;
    /// Bytes that end a pair: the pair delimiters.
    ByteSet pair_delimiters;

    explicit Impl(const Configuration & configuration_)
        : configuration(configuration_)
    {
        validate();

        waiting_key_bytes.add(configuration.key_value_delimiter);
        key_stop_bytes.add(configuration.key_value_delimiter);

        for (char c : configuration.pair_delimiters)
        {
            waiting_key_bytes.add(c);
            key_stop_bytes.add(c);
            value_stop_bytes.add(c);
            pair_delimiters.add(c);
        }

        if (configuration.unexpected_quoting_character_strategy != UnexpectedQuotingCharacterStrategy::ACCEPT)
        {
            key_stop_bytes.add(configuration.quoting_character);
            value_stop_bytes.add(configuration.quoting_character);
        }

        quoted_stop_bytes.add(configuration.quoting_character);

        if (configuration.with_escaping)
        {
            waiting_key_bytes.add(ESCAPE_CHARACTER);
            key_stop_bytes.add(ESCAPE_CHARACTER);
            value_stop_bytes.add(ESCAPE_CHARACTER);
            quoted_stop_bytes.add(ESCAPE_CHARACTER);
        }
    }

    void validate() const
    {
        const auto & pair_delimiters_str = configuration.pair_delimiters;

        if (configuration.key_value_delimiter == configuration.quoting_character)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, key_value_delimiter and quoting_character can not be the same");

        if (pair_delimiters_str.size() > MAX_NUMBER_OF_PAIR_DELIMITERS)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, pair delimiters can contain at most {} characters", MAX_NUMBER_OF_PAIR_DELIMITERS);

        if (pair_delimiters_str.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, pair delimiters list is empty");

        if (pair_delimiters_str.contains(configuration.key_value_delimiter))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, key_value_delimiter conflicts with pair delimiters");

        if (pair_delimiters_str.contains(configuration.quoting_character))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, quoting_character conflicts with pair delimiters");

        if (configuration.with_escaping)
        {
            bool used_escape_character = configuration.key_value_delimiter == ESCAPE_CHARACTER
                || configuration.quoting_character == ESCAPE_CHARACTER
                || pair_delimiters_str.contains(ESCAPE_CHARACTER);

            if (used_escape_character)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid arguments, {} is reserved for the escaping character", ESCAPE_CHARACTER);
        }
    }

    /// Reads a token starting at `pos` until a byte from `stop_set` and moves `pos` past that byte
    /// (or to `end`). With escaping, escape sequences are decoded into the token.
    template <bool with_escaping>
    Stop readToken(const ByteSet & stop_bytes, Token & token, std::string_view & result, const char *& pos, const char * end) const
    {
        token.start(pos);

        while (true)
        {
            pos = stop_bytes.find<true>(pos, end);
            if (pos == end)
            {
                result = token.finish(pos);
                return Stop::End;
            }

            char c = *pos;
            if constexpr (with_escaping)
            {
                if (c == ESCAPE_CHARACTER)
                {
                    if (token.consumeEscapeSequence(pos, end))
                        continue;

                    result = token.finish(pos);
                    return Stop::InvalidEscapeSequence;
                }
            }

            result = token.finish(pos);
            ++pos;

            if (c == configuration.key_value_delimiter)
                return Stop::KeyValueDelimiter;

            if (c == configuration.quoting_character)
                return Stop::QuotingCharacter;

            return Stop::PairDelimiter;
        }
    }

    /// Reads an unquoted key starting at `pos`. Depending on the strategy, an unexpected quoting
    /// character either discards the key or restarts it as a quoted key.
    template <bool with_escaping>
    ReadResult readUnquotedKey(Token & key, std::string_view & key_view, const char *& pos, const char * end) const
    {
        Stop stop = readToken<with_escaping>(key_stop_bytes, key, key_view, pos, end);
        switch (stop)
        {
            case Stop::End:
                return ReadResult::End;
            case Stop::KeyValueDelimiter:
                return ReadResult::Ok;
            case Stop::PairDelimiter:
            case Stop::InvalidEscapeSequence:
                return ReadResult::Discard;
            case Stop::QuotingCharacter:
            {
                if (configuration.unexpected_quoting_character_strategy == UnexpectedQuotingCharacterStrategy::INVALID)
                    return ReadResult::Discard;

                return readQuotedKey<with_escaping>(key, key_view, pos, end);
            }
        }
    }

    /// Reads a quoted key, `pos` is right after the opening quote. The key must be non-empty
    /// and the closing quote must be followed by the key-value delimiter.
    template <bool with_escaping>
    ReadResult readQuotedKey(Token & key, std::string_view & key_view, const char *& pos, const char * end) const
    {
        Stop stop = readToken<with_escaping>(quoted_stop_bytes, key, key_view, pos, end);
        if (stop == Stop::End)
            return ReadResult::End;

        if (stop == Stop::InvalidEscapeSequence || key_view.empty())
            return ReadResult::Discard;

        if (pos == end || *pos != configuration.key_value_delimiter)
            return ReadResult::Discard;

        ++pos;
        return ReadResult::Ok;
    }

    /// Reads an unquoted value starting at `pos`. It ends at a pair delimiter or at the end of the input;
    /// an invalid escape sequence ends it as well and what was read so far is kept. Depending on the
    /// strategy, an unexpected quoting character either discards the value or restarts it as a quoted value.
    template <bool with_escaping>
    ReadResult readUnquotedValue(Token & value, std::string_view & value_view, const char *& pos, const char * end) const
    {
        if constexpr (with_escaping)
        {
            /// A value cannot start with an escape sequence.
            if (pos != end && *pos == ESCAPE_CHARACTER)
                return ReadResult::Discard;
        }

        Stop stop = readToken<with_escaping>(value_stop_bytes, value, value_view, pos, end);
        if (stop != Stop::QuotingCharacter)
            return ReadResult::Ok;

        if (configuration.unexpected_quoting_character_strategy == UnexpectedQuotingCharacterStrategy::INVALID)
            return ReadResult::Discard;

        return readQuotedValue<with_escaping>(value, value_view, pos, end);
    }

    /// Reads a quoted value, `pos` is right after the opening quote. Only a pair delimiter may follow
    /// the closing quote, everything up to it is skipped.
    template <bool with_escaping>
    ReadResult readQuotedValue(Token & value, std::string_view & value_view, const char *& pos, const char * end) const
    {
        Stop stop = readToken<with_escaping>(quoted_stop_bytes, value, value_view, pos, end);
        if (stop == Stop::End)
            return ReadResult::End;

        if (stop == Stop::InvalidEscapeSequence)
            return ReadResult::Discard;

        pos = pair_delimiters.find<true>(pos, end);
        if (pos != end)
            ++pos;

        return ReadResult::Ok;
    }

    template <bool with_escaping, typename OnPair>
    size_t extractImpl(std::string_view data, OnPair && on_pair) const
    {
        const char * pos = data.data();
        const char * const end = pos + data.size();
        size_t num_pairs = 0;

        /// Used only when escape sequences are decoded, otherwise keys and values are views into `data`.
        std::string key_buffer;
        std::string value_buffer;
        Token key(key_buffer);
        Token value(value_buffer);
        std::string_view key_view;
        std::string_view value_view;

        auto commit = [&]
        {
            ++num_pairs;

            if (configuration.max_number_of_pairs && num_pairs > configuration.max_number_of_pairs)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Number of pairs produced exceeded the limit of {}", configuration.max_number_of_pairs);

            on_pair(key_view, value_view);
        };

        while (true)
        {
            /// Waiting for a key.
            pos = waiting_key_bytes.find<false>(pos, end);
            if (pos == end)
                return num_pairs;

            ReadResult key_result{};
            if (*pos == configuration.quoting_character)
            {
                ++pos;
                key_result = readQuotedKey<with_escaping>(key, key_view, pos, end);
            }
            else
            {
                key_result = readUnquotedKey<with_escaping>(key, key_view, pos, end);
            }

            if (key_result == ReadResult::End)
                return num_pairs;

            if (key_result == ReadResult::Discard)
                continue;

            /// Waiting for a value.
            ReadResult value_result{};
            if (pos != end && *pos == configuration.quoting_character)
            {
                ++pos;
                value_result = readQuotedValue<with_escaping>(value, value_view, pos, end);
            }
            else
            {
                value_result = readUnquotedValue<with_escaping>(value, value_view, pos, end);
            }

            if (value_result == ReadResult::End)
                return num_pairs;

            if (value_result == ReadResult::Discard)
                continue;

            commit();
            if (pos == end)
                return num_pairs;
        }
    }

    template <typename OnPair>
    size_t extract(std::string_view data, OnPair && on_pair) const
    {
        return configuration.with_escaping ? extractImpl<true>(data, on_pair) : extractImpl<false>(data, on_pair);
    }

    static constexpr size_t MAX_NUMBER_OF_PAIR_DELIMITERS = 8;
};

KeyValuePairExtractor::KeyValuePairExtractor(const Configuration & configuration_)
    : impl(std::make_unique<const Impl>(configuration_))
{
}

KeyValuePairExtractor::~KeyValuePairExtractor() = default;

size_t KeyValuePairExtractor::extract(std::string_view data, ColumnString & keys, ColumnString & values) const
{
    return impl->extract(data, [&](std::string_view key, std::string_view value)
    {
        keys.insertData(key.data(), key.size());
        values.insertData(value.data(), value.size());
    });
}

size_t KeyValuePairExtractor::forEachPair(std::string_view data, const PairCallback & on_pair) const
{
    return impl->extract(data, on_pair);
}

namespace
{

class ExtractKeyValuePairs final : public IFunction
{
public:
    ExtractKeyValuePairs(ContextPtr context, String name_, bool with_escaping_)
        : max_number_of_pairs(context->getSettingsRef()[Setting::extract_key_value_pairs_max_pairs_per_row])
        , name(std::move(name_))
        , with_escaping(with_escaping_)
    {
    }

    static FunctionPtr create(ContextPtr context, String name, bool with_escaping)
    {
        return std::make_shared<ExtractKeyValuePairs>(context, std::move(name), with_escaping);
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3, 4}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > MAX_NUMBER_OF_ARGUMENTS)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires at least 1 argument and at most {}. {} was provided",
                getName(), MAX_NUMBER_OF_ARGUMENTS, arguments.size());
        }

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (!isStringOrFixedString(arguments[i].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument {}. Must be String.", arguments[i].type, ARGUMENT_NAMES[i]);
        }

        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        KeyValuePairExtractor extractor(getConfiguration(arguments));
        const auto & data_column = *arguments[0].column;

        auto keys = ColumnString::create();
        auto values = ColumnString::create();
        auto offsets = ColumnUInt64::create();
        auto & offsets_data = offsets->getData();
        offsets_data.reserve(input_rows_count);

        UInt64 offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            offset += extractor.extract(data_column.getDataAt(i), *keys, *values);
            offsets_data.push_back(offset);
        }

        return ColumnMap::create(ColumnPtr(std::move(keys)), ColumnPtr(std::move(values)), ColumnPtr(std::move(offsets)));
    }

private:
    static constexpr size_t MAX_NUMBER_OF_ARGUMENTS = 5;
    static constexpr std::array<std::string_view, MAX_NUMBER_OF_ARGUMENTS> ARGUMENT_NAMES =
    {
        "data_column",
        "key_value_delimiter",
        "pair_delimiters",
        "quoting_character",
        "unexpected_quoting_character_strategy"
    };

    KeyValuePairExtractor::Configuration getConfiguration(const ColumnsWithTypeAndName & arguments) const
    {
        KeyValuePairExtractor::Configuration configuration;
        configuration.with_escaping = with_escaping;
        configuration.max_number_of_pairs = max_number_of_pairs;

        /// An empty argument keeps the default.
        if (arguments.size() > 1)
        {
            if (auto c = getCharacterArgument(arguments[1]))
                configuration.key_value_delimiter = *c;
        }

        if (arguments.size() > 2)
        {
            if (auto pair_delimiters = arguments[2].column->getDataAt(0); !pair_delimiters.empty())
                configuration.pair_delimiters = pair_delimiters;
        }

        if (arguments.size() > 3)
        {
            if (auto c = getCharacterArgument(arguments[3]))
                configuration.quoting_character = *c;
        }

        if (arguments.size() > 4)
        {
            auto strategy_name = arguments[4].column->getDataAt(0);
            auto strategy = magic_enum::enum_cast<KeyValuePairExtractor::UnexpectedQuotingCharacterStrategy>(strategy_name, magic_enum::case_insensitive);

            if (!strategy)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid unexpected_quoting_character_strategy argument: {}", strategy_name);

            configuration.unexpected_quoting_character_strategy = *strategy;
        }

        return configuration;
    }

    static std::optional<char> getCharacterArgument(const ColumnWithTypeAndName & argument)
    {
        auto value = argument.column->getDataAt(0);
        if (value.empty())
            return {};

        if (value.size() == 1)
            return value.front();

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Control character argument must either be empty or contain exactly 1 character");
    }

    const UInt64 max_number_of_pairs;
    const String name;
    const bool with_escaping;
};

}

REGISTER_FUNCTION(ExtractKeyValuePairs)
{
    factory.registerFunction("extractKeyValuePairs", [](ContextPtr ctx){ return ExtractKeyValuePairs::create(ctx, "extractKeyValuePairs", false); },
        FunctionDocumentation{
            .description=R"(Extracts key-value pairs from any string. The string does not need to be 100% structured in a key value pair format;

It can contain noise (e.g. log files). The key-value pair format to be interpreted should be specified via function arguments.

A key-value pair consists of a key followed by a `key_value_delimiter` and a value. Quoted keys and values are also supported. Key value pairs must be separated by pair delimiters.

**Syntax**
```sql
extractKeyValuePairs(data, [key_value_delimiter], [pair_delimiter], [quoting_character], [unexpected_quoting_character_strategy])
```

**Arguments**
- `data` - String to extract key-value pairs from. [String](/reference/data-types/string) or [FixedString](/reference/data-types/fixedstring).
- `key_value_delimiter` - Character to be used as delimiter between the key and the value. Defaults to `:`. [String](/reference/data-types/string) or [FixedString](/reference/data-types/fixedstring).
- `pair_delimiters` - Set of character to be used as delimiters between pairs. Defaults to `\space`, `,` and `;`. [String](/reference/data-types/string) or [FixedString](/reference/data-types/fixedstring).
- `quoting_character` - Character to be used as quoting character. Defaults to `"`. [String](/reference/data-types/string) or [FixedString](/reference/data-types/fixedstring).
- `unexpected_quoting_character_strategy` - Strategy to handle quoting characters in unexpected places during `read_key` and `read_value` phase. Possible values: `invalid`, `accept` and `promote`. Invalid will discard key/value and transition back to `WAITING_KEY` state. Accept will treat it as a normal character. Promote will transition to `READ_QUOTED_{KEY/VALUE}` state and start from next character. The default value is `PROMOTE`

**Returned values**
- The extracted key-value pairs in a Map(String, String).

**Examples**

Query:

**Simple case**
```sql
arthur :) select extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv

SELECT extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv

Query id: f9e0ca6f-3178-4ee2-aa2c-a5517abb9cee

┌─kv──────────────────────────────────────────────────────────────────────┐
│ {'name':'neymar','age':'31','team':'psg','nationality':'brazil'}        │
└─────────────────────────────────────────────────────────────────────────┘
```

**Single quote as quoting character**
```sql
arthur :) select extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'') as kv

SELECT extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'') as kv

Query id: 0e22bf6b-9844-414a-99dc-32bf647abd5e

┌─kv───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {'name':'neymar','age':'31','team':'psg','nationality':'brazil','last_key':'last_value'}                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

unexpected_quoting_character_strategy examples:

unexpected_quoting_character_strategy=invalid

```sql
SELECT extractKeyValuePairs('name"abc:5', ':', ' ,;', '\"', 'INVALID') as kv;
```

```text
┌─kv────────────────┐
│ {'abc':'5'}       │
└───────────────────┘
```

```sql
SELECT extractKeyValuePairs('name"abc":5', ':', ' ,;', '\"', 'INVALID') as kv;
```

```text
┌─kv──┐
│ {}  │
└─────┘
```

unexpected_quoting_character_strategy=accept

```sql
SELECT extractKeyValuePairs('name"abc:5', ':', ' ,;', '\"', 'ACCEPT') as kv;
```

```text
┌─kv────────────────┐
│ {'name"abc':'5'}  │
└───────────────────┘
```

```sql
SELECT extractKeyValuePairs('name"abc":5', ':', ' ,;', '\"', 'ACCEPT') as kv;
```

```text
┌─kv─────────────────┐
│ {'name"abc"':'5'}  │
└────────────────────┘
```

unexpected_quoting_character_strategy=promote

```sql
SELECT extractKeyValuePairs('name"abc:5', ':', ' ,;', '\"', 'PROMOTE') as kv;
```

```text
┌─kv──┐
│ {}  │
└─────┘
```

```sql
SELECT extractKeyValuePairs('name"abc":5', ':', ' ,;', '\"', 'PROMOTE') as kv;
```

```text
┌─kv───────────┐
│ {'abc':'5'}  │
└──────────────┘
```

**Escape sequences without escape sequences support**
```sql
arthur :) select extractKeyValuePairs('age:a\\x0A\\n\\0') as kv

SELECT extractKeyValuePairs('age:a\\x0A\\n\\0') AS kv

Query id: e9fd26ee-b41f-4a11-b17f-25af6fd5d356

┌─kv─────────────────────┐
│ {'age':'a\\x0A\\n\\0'} │
└────────────────────────┘
```)",
            .syntax = "extractKeyValuePairs(data, [key_value_delimiter], [pair_delimiter], [quoting_character], [unexpected_quoting_character_strategy])",
            .introduced_in = {23, 4},
            .category = FunctionDocumentation::Category::Map
        }
    );

    factory.registerFunction("extractKeyValuePairsWithEscaping", [](ContextPtr ctx){ return ExtractKeyValuePairs::create(ctx, "extractKeyValuePairsWithEscaping", true); },
        FunctionDocumentation{
            .description=R"(Same as `extractKeyValuePairs` but with escaping support.

Escape sequences supported: `\x`, `\N`, `\a`, `\b`, `\e`, `\f`, `\n`, `\r`, `\t`, `\v` and `\0`.
Non standard escape sequences are returned as it is (including the backslash) unless they are one of the following:
`\\`, `'`, `"`, `backtick`, `/`, `=` or ASCII control characters (`c <= 31`).

This function will satisfy the use case where pre-escaping and post-escaping are not suitable. For instance, consider the following
input string: `a: "aaaa\"bbb"`. The expected output is: `a: aaaa\"bbbb`.
- Pre-escaping: Pre-escaping it will output: `a: "aaaa"bbb"` and `extractKeyValuePairs` will then output: `a: aaaa`
- Post-escaping: `extractKeyValuePairs` will output `a: aaaa\` and post-escaping will keep it as it is.

Leading escape sequences will be skipped in keys and will be considered invalid for values.

**Escape sequences with escape sequence support turned on**
```sql
arthur :) select extractKeyValuePairsWithEscaping('age:a\\x0A\\n\\0') as kv

SELECT extractKeyValuePairsWithEscaping('age:a\\x0A\\n\\0') AS kv

Query id: 44c114f0-5658-4c75-ab87-4574de3a1645

┌─kv────────────────┐
│ {'age':'a\n\n\0'} │
└───────────────────┘
```)",
            .syntax = "extractKeyValuePairsWithEscaping(data, [key_value_delimiter], [pair_delimiter], [quoting_character], [unexpected_quoting_character_strategy])",
            .introduced_in = {23, 4},
            .category = FunctionDocumentation::Category::Map
        }
    );
    factory.registerAlias("str_to_map", "extractKeyValuePairs", FunctionFactory::Case::Insensitive);
    factory.registerAlias("mapFromString", "extractKeyValuePairs");
}

}
