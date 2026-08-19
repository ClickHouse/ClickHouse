#pragma once

#include <Functions/keyvaluepair/impl/Configuration.h>
#include <Functions/keyvaluepair/impl/StateHandler.h>
#include <Functions/keyvaluepair/impl/NeedleFactory.h>
#include <Functions/keyvaluepair/impl/DuplicateKeyFoundException.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Columns/ColumnString.h>
#include <base/find_symbols.h>

#include <string_view>
#include <string>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace extractKV
{


/*
 * Handles (almost) all states present in `StateHandler::State`. The description of each state responsibility can be found in
 * `StateHandler::State`. Advanced & optimized string search algorithms are used to search for control characters and form key value pairs.
 * Each method returns a `StateHandler::NextState` object which contains the next state itself and the number of characters consumed by the previous state.
 *
 * The class is templated with a boolean that controls escaping support. As of now, there are two specializations:
 * `NoEscapingStateHandler` and `InlineEscapingStateHandler`.
 * */
template <bool WITH_ESCAPING>
class StateHandlerImpl : public StateHandler
{
public:
    explicit StateHandlerImpl(Configuration configuration_)
        : configuration(std::move(configuration_))
    {
        /* SearchNeedles do not change throughout the algorithm. Therefore, they are created only once in the constructor
         * to avoid unnecessary copies.
         * */
        NeedleFactory<WITH_ESCAPING> needle_factory;

        wait_key_needles = needle_factory.getWaitKeyNeedles(configuration);
        read_key_needles = needle_factory.getReadKeyNeedles(configuration);
        read_value_needles = needle_factory.getReadValueNeedles(configuration);
        read_quoted_needles = needle_factory.getReadQuotedNeedles(configuration);
        wait_pair_delimiter_needles = needle_factory.getWaitPairDelimiterNeedles(configuration);
    }

    /*
     * Find first character that is considered a valid key character and proceeds to READING_KEY like states.
     * */
    [[nodiscard]] NextState waitKey(std::string_view file) const
    {
        if (const auto * p = find_first_not_symbols_or_null(file, wait_key_needles))
        {
            const size_t character_position = p - file.data();
            if (isQuotingCharacter(*p))
            {
                // +1 to skip quoting character
                return {character_position + 1u, State::READING_QUOTED_KEY};
            }

            return {character_position, State::READING_KEY};
        }

        return {file.size(), State::END};
    }

    /*
     * Find first delimiter of interest (`read_needles`). Valid symbols are either `key_value_delimiter` and `escape_character` if escaping
     * support is on. If it finds a pair delimiter, it discards the key.
     * */
    [[nodiscard]] NextState readKey(std::string_view file, auto & pair_writer) const
    {
        pair_writer.resetKey();

        size_t pos = 0;

        while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_key_needles))
        {
            auto character_position = p - file.data();
            size_t next_pos = character_position + 1u;

            if (WITH_ESCAPING && isEscapeCharacter(*p))
            {
                if constexpr (WITH_ESCAPING)
                {
                    auto [parsed_successfully, escape_sequence_length] = consumeWithEscapeSequence<true>(file, pos, character_position, pair_writer);
                    next_pos = character_position + escape_sequence_length;

                    if (!parsed_successfully)
                    {
                        return {next_pos, State::WAITING_KEY};
                    }
                }
            }
            else if (isKeyValueDelimiter(*p))
            {
                pair_writer.appendKey(file.data() + pos, file.data() + character_position);

                return {next_pos, State::WAITING_VALUE};
            }
            else if (isPairDelimiter(*p))
            {
                return {next_pos, State::WAITING_KEY};
            }
            else if (isQuotingCharacter(*p))
            {
                switch (configuration.unexpected_quoting_character_strategy)
                {
                    case Configuration::UnexpectedQuotingCharacterStrategy::INVALID:
                        return {next_pos, State::WAITING_KEY};
                    case Configuration::UnexpectedQuotingCharacterStrategy::PROMOTE:
                        return {next_pos, State::READING_QUOTED_KEY};
                    case Configuration::UnexpectedQuotingCharacterStrategy::ACCEPT:
                        // The quoting character should not be added to the search symbols list in case strategy = Configuration::UnexpectedQuotingCharacterStrategy::ACCEPT
                        // See `NeedleFactory`
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to handle unexpected quoting character. This is a bug");
                }
            }

            pos = next_pos;
        }

        return {file.size(), State::END};
    }

    /*
     * Search for closing quoting character and process escape sequences along the way (if escaping support is turned on).
     * */
    [[nodiscard]] NextState readQuotedKey(std::string_view file, auto & pair_writer) const
    {
        pair_writer.resetKey();

        size_t pos = 0;

        while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
        {
            size_t character_position = p - file.data();
            size_t next_pos = character_position + 1u;

            if (WITH_ESCAPING && isEscapeCharacter(*p))
            {
                if constexpr (WITH_ESCAPING)
                {
                    auto [parsed_successfully, escape_sequence_length] = consumeWithEscapeSequence<true>(file, pos, character_position, pair_writer);
                    next_pos = character_position + escape_sequence_length;

                    if (!parsed_successfully)
                    {
                        return {next_pos, State::WAITING_KEY};
                    }
                }
            }
            else if (isQuotingCharacter(*p))
            {
                pair_writer.appendKey(file.data() + pos, file.data() + character_position);

                if (pair_writer.isKeyEmpty())
                {
                    return {next_pos, State::WAITING_KEY};
                }

                return {next_pos, State::READING_KV_DELIMITER};
            }

            pos = next_pos;
        }

        return {file.size(), State::END};
    }

    /*
     * Validate expected key-value-delimiter is in place.
     * */
    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file) const
    {
        if (!file.empty())
        {
            const auto current_character = file[0];

            if (isKeyValueDelimiter(current_character))
            {
                return {1, WAITING_VALUE};
            }
        }

        return {0, State::WAITING_KEY};
    }

    /*
     * Check if next character is a valid value character and jumps to read-like states. Caveat here is that a pair delimiter must also lead to
     * read-like states because it indicates empty values.
     * */
    [[nodiscard]] NextState waitValue(std::string_view file) const
    {
        size_t pos = 0;

        if (!file.empty())
        {
            const auto current_character = file[pos];

            if (isQuotingCharacter(current_character))
            {
                return {pos + 1u, State::READING_QUOTED_VALUE};
            }

            if constexpr (WITH_ESCAPING)
            {
                if (isEscapeCharacter(current_character))
                {
                    return {pos, State::WAITING_KEY};
                }
            }
        }

        return {pos, State::READING_VALUE};
    }

    /*
     * Finds next delimiter of interest (`read_needles`). Valid symbols are either `pair_delimiter` and `escape_character` if escaping
     * support is on. If it finds a `key_value_delimiter`, it discards the value.
     * */
    [[nodiscard]] NextState readValue(std::string_view file, auto & pair_writer) const
    {
        pair_writer.resetValue();

        size_t pos = 0;

        while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_value_needles))
        {
            const size_t character_position = p - file.data();
            size_t next_pos = character_position + 1u;

            if (WITH_ESCAPING && isEscapeCharacter(*p))
            {
                if constexpr (WITH_ESCAPING)
                {
                    auto [parsed_successfully, escape_sequence_length] = consumeWithEscapeSequence<false>(file, pos, character_position, pair_writer);
                    next_pos = character_position + escape_sequence_length;

                    if (!parsed_successfully)
                    {
                        // Perform best-effort parsing and ignore invalid escape sequences at the end
                        return {next_pos, State::FLUSH_PAIR};
                    }
                }
            }
            else if (isPairDelimiter(*p))
            {
                pair_writer.appendValue(file.data() + pos, file.data() + character_position);

                return {next_pos, State::FLUSH_PAIR};
            }
            else if (isQuotingCharacter(*p))
            {
                switch (configuration.unexpected_quoting_character_strategy)
                {
                    case Configuration::UnexpectedQuotingCharacterStrategy::INVALID:
                        return {next_pos, State::WAITING_KEY};
                    case Configuration::UnexpectedQuotingCharacterStrategy::PROMOTE:
                        return {next_pos, State::READING_QUOTED_VALUE};
                    case Configuration::UnexpectedQuotingCharacterStrategy::ACCEPT:
                        // The quoting character should not be added to the search symbols list in case strategy = Configuration::UnexpectedQuotingCharacterStrategy::ACCEPT
                        // See `NeedleFactory`
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to handle unexpected quoting character. This is a bug");
                }
            }

            pos = next_pos;
        }

        // Reached end of input, consume rest of the file as value and make sure KV pair is produced.
        pair_writer.appendValue(file.data() + pos, file.data() + file.size());
        return {file.size(), State::FLUSH_PAIR};
    }

    /*
     * Search for closing quoting character and process escape sequences along the way (if escaping support is turned on).
     * */
    [[nodiscard]] NextState readQuotedValue(std::string_view file, auto & pair_writer) const
    {
        size_t pos = 0;

        pair_writer.resetValue();

        while (const auto * p = find_first_symbols_or_null({file.begin() + pos, file.end()}, read_quoted_needles))
        {
            const size_t character_position = p - file.data();
            size_t next_pos = character_position + 1u;

            if (WITH_ESCAPING && isEscapeCharacter(*p))
            {
                if constexpr (WITH_ESCAPING)
                {
                    auto [parsed_successfully, escape_sequence_length] = consumeWithEscapeSequence<false>(file, pos, character_position, pair_writer);
                    next_pos = character_position + escape_sequence_length;

                    if (!parsed_successfully)
                    {
                        return {next_pos, State::WAITING_KEY};
                    }
                }
            }
            else if (isQuotingCharacter(*p))
            {
                pair_writer.appendValue(file.data() + pos, file.data() + character_position);

                return {next_pos, State::FLUSH_PAIR_AFTER_QUOTED_VALUE};
            }

            pos = next_pos;
        }

        return {file.size(), State::END};
    }

    [[nodiscard]] NextState flushPair(std::string_view file, auto & pair_writer) const
    {
        pair_writer.commitKey();
        pair_writer.commitValue();

        return {0, file.empty() ? State::END : State::WAITING_KEY};
    }

    [[nodiscard]] NextState flushPairAfterQuotedValue(std::string_view file, auto & pair_writer) const
    {
        pair_writer.commitKey();
        pair_writer.commitValue();

        return {0, file.empty() ? State::END : State::WAITING_PAIR_DELIMITER};
    }

    [[nodiscard]] NextState waitPairDelimiter(std::string_view file) const
    {
        if (const auto * p = find_first_symbols_or_null(file, wait_pair_delimiter_needles))
        {
            const size_t character_position = p - file.data();
            size_t next_pos = character_position + 1u;

            return {next_pos, State::WAITING_KEY};
        }

        return {file.size(), State::END};
    }

    const Configuration configuration;

private:
    SearchSymbols wait_key_needles;
    SearchSymbols read_key_needles;
    SearchSymbols read_value_needles;
    SearchSymbols read_quoted_needles;
    SearchSymbols wait_pair_delimiter_needles;

    /*
     * Helper method to copy bytes until `character_pos` and process possible escape sequence. Returns a pair containing a boolean
     * that indicates success and a std::size_t that contains the number of bytes read/ consumed.
     * */
    template <bool isKey>
    std::pair<bool, std::size_t> consumeWithEscapeSequence(std::string_view file, size_t start_pos, size_t character_pos, auto & output) const
    {
        std::string escaped_sequence;
        DB::ReadBufferFromMemory buf(file.data() + character_pos, file.size() - character_pos);

        if constexpr (isKey)
        {
            output.appendKey(file.data() + start_pos, file.data() + character_pos);
        }
        else
        {
            output.appendValue(file.data() + start_pos, file.data() + character_pos);
        }

        if (DB::parseComplexEscapeSequence(escaped_sequence, buf))
        {
            if constexpr (isKey)
            {
                output.appendKey(escaped_sequence);
            }
            else
            {
                output.appendValue(escaped_sequence);
            }


            return {true, buf.getPosition()};
        }

        return {false, buf.getPosition()};
    }

    bool isKeyValueDelimiter(char character) const
    {
        return configuration.key_value_delimiter == character;
    }

    bool isPairDelimiter(char character) const
    {
        const auto & pair_delimiters = configuration.pair_delimiters;
        return std::find(pair_delimiters.begin(), pair_delimiters.end(), character) != pair_delimiters.end();
    }

    bool isQuotingCharacter(char character) const
    {
        return configuration.quoting_character == character;
    }

    bool isEscapeCharacter(char character) const
    {
        return character == '\\';
    }
};

struct NoEscapingStateHandler : public StateHandlerImpl<false>
{
    /*
     * View based PairWriter, no temporary copies are used.
     * */
    class PairWriter
    {
        ColumnString & key_col;
        ColumnString & value_col;

        std::string_view key;
        std::string_view value;

    public:
        PairWriter(ColumnString & key_col_, ColumnString & value_col_)
            : key_col(key_col_), value_col(value_col_)
        {}

        ~PairWriter()
        {
            // Make sure that ColumnString invariants are not broken.
            if (!isKeyEmpty())
            {
                resetKey();
            }

            if (!isValueEmpty())
            {
                resetValue();
            }
        }

        void appendKey(std::string_view new_data)
        {
            key = new_data;
        }

        template <typename T>
        void appendKey(const T * begin, const T * end)
        {
            appendKey({begin, end});
        }

        void appendValue(std::string_view new_data)
        {
            value = new_data;
        }

        template <typename T>
        void appendValue(const T * begin, const T * end)
        {
            appendValue({begin, end});
        }

        void resetKey()
        {
            key = {};
        }

        void resetValue()
        {
            value = {};
        }

        bool isKeyEmpty() const
        {
            return key.empty();
        }

        bool isValueEmpty() const
        {
            return value.empty();
        }

        void commitKey()
        {
            key_col.insertData(key.data(), key.size());
            resetKey();
        }

        void commitValue()
        {
            value_col.insertData(value.data(), value.size());
            resetValue();
        }


        std::string_view uncommittedKeyChunk() const
        {
            return key;
        }

        std::string_view uncommittedValueChunk() const
        {
            return value;
        }
    };

    template <typename ... Args>
    explicit NoEscapingStateHandler(Args && ... args)
    : StateHandlerImpl<false>(std::forward<Args>(args)...) {}
};

struct InlineEscapingStateHandler : public StateHandlerImpl<true>
{
    class PairWriter
    {
        ColumnString & key_col;
        ColumnString::Chars & key_chars;
        UInt64 key_prev_commit_pos;

        ColumnString & value_col;
        ColumnString::Chars & value_chars;
        UInt64 value_prev_commit_pos;

    public:
        PairWriter(ColumnString & key_col_, ColumnString & value_col_)
            : key_col(key_col_),
            key_chars(key_col.getChars()),
            key_prev_commit_pos(key_chars.size()),
            value_col(value_col_),
            value_chars(value_col.getChars()),
            value_prev_commit_pos(value_chars.size())
        {}

        ~PairWriter()
        {
            // Make sure that ColumnString invariants are not broken.
            if (!isKeyEmpty())
            {
                resetKey();
           }

            if (!isValueEmpty())
            {
                resetValue();
            }
        }

        void appendKey(std::string_view new_data)
        {
            key_chars.insert(new_data.begin(), new_data.end());
        }

        template <typename T>
        void appendKey(const T * begin, const T * end)
        {
            key_chars.insert(begin, end);
        }

        void appendValue(std::string_view new_data)
        {
            value_chars.insert(new_data.begin(), new_data.end());
        }

        template <typename T>
        void appendValue(const T * begin, const T * end)
        {
            value_chars.insert(begin, end);
        }

        void resetKey()
        {
            key_chars.resize_assume_reserved(key_prev_commit_pos);
        }

        void resetValue()
        {
            value_chars.resize_assume_reserved(value_prev_commit_pos);
        }

        bool isKeyEmpty() const
        {
            return key_chars.size() == key_prev_commit_pos;
        }

        bool isValueEmpty() const
        {
            return value_chars.size() == value_prev_commit_pos;
        }

        void commitKey()
        {
            key_col.insertData(nullptr, 0);
            key_prev_commit_pos = key_chars.size();
        }

        void commitValue()
        {
            value_col.insertData(nullptr, 0);
            value_prev_commit_pos = value_chars.size();
        }

        std::string_view uncommittedKeyChunk() const
        {
            return std::string_view(key_chars.raw_data() + key_prev_commit_pos, key_chars.raw_data() + key_chars.size());
        }

        std::string_view uncommittedValueChunk() const
        {
            return std::string_view(value_chars.raw_data() + value_prev_commit_pos, value_chars.raw_data() + value_chars.size());
        }
    };

    template <typename ... Args>
    explicit InlineEscapingStateHandler(Args && ... args)
        : StateHandlerImpl<true>(std::forward<Args>(args)...) {}
};

struct ReferencesMapStateHandler : public StateHandlerImpl<false>
{
    /*
     * View based PairWriter, no copies at all
     * */
    class PairWriter
    {
        std::map<std::string_view, std::string_view> & map;

        std::string_view key;
        std::string_view value;

    public:
        explicit PairWriter(std::map<std::string_view, std::string_view> & map_)
            : map(map_)
        {}

        ~PairWriter()
        {
            // Make sure that ColumnString invariants are not broken.
            if (!isKeyEmpty())
            {
                resetKey();
            }

            if (!isValueEmpty())
            {
                resetValue();
            }
        }

        void appendKey(std::string_view new_data)
        {
            key = new_data;
        }

        template <typename T>
        void appendKey(const T * begin, const T * end)
        {
            appendKey({begin, end});
        }

        void appendValue(std::string_view new_data)
        {
            value = new_data;
        }

        template <typename T>
        void appendValue(const T * begin, const T * end)
        {
            appendValue({begin, end});
        }

        void resetKey()
        {
            key = {};
        }

        void resetValue()
        {
            value = {};
        }

        bool isKeyEmpty() const
        {
            return key.empty();
        }

        bool isValueEmpty() const
        {
            return value.empty();
        }

        void commitKey()
        {
            // don't do anything
        }

        void commitValue()
        {
            if (map.contains(key) && value != map[key])
            {
                throw DuplicateKeyFoundException(key);
            }

            map[key] = value;

            resetValue();
            resetKey();
        }

        std::string_view uncommittedKeyChunk() const
        {
            return key;
        }

        std::string_view uncommittedValueChunk() const
        {
            return value;
        }
    };

    template <typename ... Args>
    explicit ReferencesMapStateHandler(Args && ... args)
    : StateHandlerImpl<false>(std::forward<Args>(args)...) {}
};


}

}
