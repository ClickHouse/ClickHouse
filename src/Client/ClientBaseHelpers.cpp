#include <Client/ClientBaseHelpers.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/LocalDate.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <base/find_symbols.h>
#include <Poco/String.h>
#include <string_view>


namespace DB
{

namespace Setting
{
    extern const SettingsBool implicit_select;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

/// Should we celebrate a bit?
bool isNewYearMode()
{
    time_t current_time = time(nullptr);

    /// It's bad to be intrusive.
    if (current_time % 3 != 0)
        return false;

    LocalDate now(current_time);
    return (now.month() == 12 && now.day() >= 20) || (now.month() == 1 && now.day() <= 5);
}

bool isChineseNewYearMode(const String & local_tz)
{
    /// Days of Dec. 20 in Chinese calendar starting from year 2019 to year 2105
    static constexpr UInt16 chineseNewYearIndicators[]
        = {18275, 18659, 19014, 19368, 19752, 20107, 20491, 20845, 21199, 21583, 21937, 22292, 22676, 23030, 23414, 23768, 24122, 24506,
            24860, 25215, 25599, 25954, 26308, 26692, 27046, 27430, 27784, 28138, 28522, 28877, 29232, 29616, 29970, 30354, 30708, 31062,
            31446, 31800, 32155, 32539, 32894, 33248, 33632, 33986, 34369, 34724, 35078, 35462, 35817, 36171, 36555, 36909, 37293, 37647,
            38002, 38386, 38740, 39095, 39479, 39833, 40187, 40571, 40925, 41309, 41664, 42018, 42402, 42757, 43111, 43495, 43849, 44233,
            44587, 44942, 45326, 45680, 46035, 46418, 46772, 47126, 47510, 47865, 48249, 48604, 48958, 49342};

    /// All time zone names are acquired from https://www.iana.org/time-zones
    static constexpr const char * chineseNewYearTimeZoneIndicators[] = {
        /// Time zones celebrating Chinese new year.
        "Asia/Shanghai",
        "Asia/Chongqing",
        "Asia/Harbin",
        "Asia/Urumqi",
        "Asia/Hong_Kong",
        "Asia/Chungking",
        "Asia/Macao",
        "Asia/Macau",
        "Asia/Taipei",
        "Asia/Singapore",

        /// Time zones celebrating Chinese new year but with different festival names. Let's not print the message for now.
        // "Asia/Brunei",
        // "Asia/Ho_Chi_Minh",
        // "Asia/Hovd",
        // "Asia/Jakarta",
        // "Asia/Jayapura",
        // "Asia/Kashgar",
        // "Asia/Kuala_Lumpur",
        // "Asia/Kuching",
        // "Asia/Makassar",
        // "Asia/Pontianak",
        // "Asia/Pyongyang",
        // "Asia/Saigon",
        // "Asia/Seoul",
        // "Asia/Ujung_Pandang",
        // "Asia/Ulaanbaatar",
        // "Asia/Ulan_Bator",
    };
    static constexpr size_t M = sizeof(chineseNewYearTimeZoneIndicators) / sizeof(chineseNewYearTimeZoneIndicators[0]);

    time_t current_time = time(nullptr);

    if (chineseNewYearTimeZoneIndicators + M
        == std::find_if(chineseNewYearTimeZoneIndicators, chineseNewYearTimeZoneIndicators + M, [&local_tz](const char * tz)
                        {
                            return tz == local_tz;
                        }))
        return false;

    /// It's bad to be intrusive.
    if (current_time % 3 != 0)
        return false;

    auto days = DateLUT::instance().toDayNum(current_time).toUnderType();
    for (auto d : chineseNewYearIndicators)
    {
        /// Let's celebrate until Lantern Festival
        if (d <= days && d + 25 >= days)
            return true;
        if (d > days)
            return false;
    }
    return false;
}

std::string getChineseZodiac()
{
    time_t current_time = time(nullptr);
    int year = DateLUT::instance().toYear(current_time);

    // Traditional Chinese Zodiac
    static constexpr const char * zodiacs[12] = {
        "鼠", "牛", "虎", "兔", "龙", "蛇",
        "马", "羊", "猴", "鸡", "狗", "猪"
    };

    //2020 is Rat
    int offset = (year - 2020) % 12;
    if (offset < 0)
        offset += 12;

    return zodiacs[offset];
}

#if USE_REPLXX
void highlight(const String & query, std::vector<replxx::Replxx::Color> & colors, const Context & context, int cursor_position)
{
    using namespace replxx;

    /// The `colors` array maps to a Unicode code point position in a string into a color.
    /// A color is set for every position individually (not for a range).

    /// Empty input.
    if (colors.empty())
        return;

    /// The colors should be legible (and look gorgeous) in both dark and light themes.
    /// When modifying this, check it in both themes.

    static const std::unordered_map<Highlight, Replxx::Color> type_to_color =
    {
        {Highlight::keyword, replxx::color::bold(Replxx::Color::DEFAULT)},
        {Highlight::identifier, Replxx::Color::CYAN},
        {Highlight::function, Replxx::Color::BROWN},
        {Highlight::alias, replxx::color::rgb666(0, 4, 4)},
        {Highlight::substitution, Replxx::Color::MAGENTA},
        {Highlight::number, replxx::color::rgb666(0, 4, 0)},
        {Highlight::string, Replxx::Color::GREEN},
    };

    /// We set reasonably small limits for size/depth, because we don't want the CLI to be slow.
    /// While syntax highlighting is unneeded for long queries, which the user couldn't read anyway.

    const char * begin = query.data();
    const char * end = begin + query.size();
    Tokens tokens(begin, end, 10000, true);
    IParser::Pos token_iterator(
        tokens,
        static_cast<uint32_t>(context.getSettingsRef()[Setting::max_parser_depth]),
        static_cast<uint32_t>(context.getSettingsRef()[Setting::max_parser_backtracks])
    );
    Expected expected;
    expected.enable_highlighting = true;

    /// We don't do highlighting for foreign dialects, such as PRQL and Kusto.
    /// Only normal ClickHouse SQL queries are highlighted.

    ParserQuery parser(end, false, context.getSettingsRef()[Setting::implicit_select]);
    ASTPtr ast;
    bool parse_res = false;

    try
    {
        while (!token_iterator->isEnd())
        {
            parse_res = parser.parse(token_iterator, ast, expected);
            if (!parse_res)
                break;

            if (!token_iterator->isEnd() && token_iterator->type != TokenType::Semicolon)
            {
                parse_res = false;
                break;
            }

            while (token_iterator->type == TokenType::Semicolon)
                ++token_iterator;
        }
    }
    catch (...)
    {
        /// Skip highlighting in the case of exceptions during parsing.
        /// It is ok to ignore unknown exceptions here.
        return;
    }

    size_t pos = 0;
    const char * prev = begin;
    for (const auto & range : expected.highlights)
    {
        auto it = type_to_color.find(range.highlight);
        if (it != type_to_color.end())
        {
            /// We have to map from byte positions to Unicode positions.
            pos += UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(prev), range.begin - prev);
            size_t utf8_len = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(range.begin), range.end - range.begin);

            for (size_t code_point_index = 0; code_point_index < utf8_len; ++code_point_index)
                colors[pos + code_point_index] = it->second;

            pos += utf8_len;
            prev = range.end;
        }
    }

    // Pride flag colors.
    static const std::array<Replxx::Color, 8> default_colormap =
    {
        replxx::color::rgb666(4, 2, 3), // Soft pink
        replxx::color::rgb666(4, 1, 1), // Red
        replxx::color::rgb666(4, 3, 1), // Gold-orange
        replxx::color::rgb666(4, 4, 1), // Yellow
        replxx::color::rgb666(1, 4, 1), // Green
        replxx::color::rgb666(1, 4, 4), // Teal
        replxx::color::rgb666(2, 1, 4), // Indigo
        replxx::color::rgb666(4, 1, 4)  // Violet
    };

    static const std::unordered_map<Replxx::Color, Replxx::Color> bright_colormap =
    {
        {replxx::color::rgb666(4, 2, 3), replxx::color::rgb666(5, 3, 4)}, // Soft pink
        {replxx::color::rgb666(4, 1, 1), replxx::color::rgb666(5, 2, 2)}, // Red
        {replxx::color::rgb666(4, 3, 1), replxx::color::rgb666(5, 4, 2)}, // Gold-orange
        {replxx::color::rgb666(4, 4, 1), replxx::color::rgb666(5, 5, 2)}, // Yellow
        {replxx::color::rgb666(1, 4, 1), replxx::color::rgb666(2, 5, 2)}, // Green
        {replxx::color::rgb666(1, 4, 4), replxx::color::rgb666(2, 5, 5)}, // Teal
        {replxx::color::rgb666(2, 1, 4), replxx::color::rgb666(3, 2, 5)}, // Indigo
        {replxx::color::rgb666(4, 1, 4), replxx::color::rgb666(5, 2, 5)}  // Violet
    };

    size_t current_color = 0;
    std::vector<Replxx::Color> color_stack;
    std::vector<Token> brace_stack;
    std::optional<std::tuple<size_t, size_t>> active_matching_brace;

    IParser::Pos highlight_token_iterator(
        tokens,
        static_cast<uint32_t>(context.getSettingsRef()[Setting::max_parser_depth]),
        static_cast<uint32_t>(context.getSettingsRef()[Setting::max_parser_backtracks])
    );

    try
    {
        while (!highlight_token_iterator->isEnd())
        {
            if (highlight_token_iterator->isError())
                break;

            if (highlight_token_iterator->type == TokenType::OpeningRoundBracket)
            {
                /// On opening round bracket, remember the color we use for it.
                color_stack.push_back(default_colormap[current_color % default_colormap.size()]);
                brace_stack.push_back(*highlight_token_iterator);
                current_color++;

                /// Highlight the opening round bracket and advance.
                auto highlight_pos = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(begin), highlight_token_iterator->begin - begin);
                if (highlight_pos < colors.size())
                    colors[highlight_pos] = color_stack.back();

                ++highlight_token_iterator;
                continue;
            }

            if (highlight_token_iterator->type == TokenType::ClosingRoundBracket)
            {
                /// Closing round bracket should match the last opening round bracket.
                /// If there is no opening round bracket, advance.
                if (color_stack.empty() || brace_stack.empty())
                {
                    ++highlight_token_iterator;
                    continue;
                }

                /// Highlight the closing round bracket.
                auto highlight_pos = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(begin), highlight_token_iterator->begin - begin);
                if (highlight_pos < colors.size())
                    colors[highlight_pos] = color_stack.back();

                /// If there is no matching opening round bracket, advance.
                auto matching_brace_pos = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(begin), brace_stack.back().begin - begin);
                auto mapped_cursor_position = static_cast<uint64_t>(cursor_position);

                auto cursor_on_current_brace = mapped_cursor_position == highlight_pos;
                auto cursor_on_matching_brace = mapped_cursor_position == matching_brace_pos;

                if (cursor_position < 0 || (cursor_on_current_brace || cursor_on_matching_brace))
                {
                    ++highlight_token_iterator;
                    color_stack.pop_back();
                    brace_stack.pop_back();
                    continue;
                }

                /// If the cursor is on one of the round braces,
                /// highlight both the opening and closing round braces with a brighter color.
                auto bright_color = bright_colormap.at(color_stack.back());
                colors[highlight_pos] = bright_color;
                colors[matching_brace_pos] = bright_color;
                active_matching_brace = std::make_tuple(highlight_pos, matching_brace_pos);

                /// Remove the last opening round brace from the stack and advance.
                color_stack.pop_back();
                brace_stack.pop_back();
                ++highlight_token_iterator;
                continue;
            }

            ++highlight_token_iterator;

            while (highlight_token_iterator->type == TokenType::Semicolon)
                ++highlight_token_iterator;
        }
    }
    catch (...)
    {
        /// Skip highlighting in the case of exceptions during parsing.
        /// It is ok to ignore unknown exceptions here.
        return;
    }

    Token last_token = token_iterator.max();
    /// Raw data in INSERT queries, which is not necessarily tokenized.
    const char * insert_data = ast ? getInsertData(ast) : nullptr;

    /// Highlight the last error in red. If the parser failed or the lexer found an invalid token,
    /// or if it didn't parse all the data (except, the data for INSERT query, which is legitimately unparsed)
    if ((!parse_res || last_token.isError())
        && !(insert_data && expected.max_parsed_pos >= insert_data)
        && expected.max_parsed_pos >= prev)
    {
        pos += UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(prev), expected.max_parsed_pos - prev);

        if (pos >= colors.size())
            pos = colors.size() - 1;

        colors[pos] = Replxx::Color::BRIGHTRED;

        if (active_matching_brace)
        {
            const auto & [highlight_pos, matching_brace_pos] = *active_matching_brace;

            const auto opening_brace_pos = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(begin), highlight_pos);
            const auto closing_brace_pos = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(begin), matching_brace_pos);

            /// If the cursor is on one of the round brackets marked as an error,
            /// highlight both with a brighter color.
            if (pos == closing_brace_pos || pos == opening_brace_pos)
            {
                colors[closing_brace_pos] = replxx::color::rgb666(5, 0, 1);
                colors[opening_brace_pos] = replxx::color::rgb666(5, 0, 1);
            }
        }
    }

    /// This is a callback for the client/local app to better find query end. Note: this is a kludge, remove it.
    if (last_token.type == TokenType::Semicolon || last_token.type == TokenType::VerticalDelimiter
        || query.ends_with(';') || query.ends_with("\\G"))  /// This is for raw data in INSERT queries, which is not necessarily tokenized.
    {
        ReplxxLineReader::setLastIsDelimiter(true);
    }
    else if (last_token.type != TokenType::Whitespace)
    {
        ReplxxLineReader::setLastIsDelimiter(false);
    }
}
#endif

void skipSpacesAndComments(const char*& pos, const char* end, std::function<void(std::string_view)> comment_callback)
{
    do
    {
        /// skip spaces to avoid throw exception after last query
        while (pos != end && std::isspace(*pos))
            ++pos;

        const char * comment_begin = pos;
        /// for skip comment after the last query and to not throw exception
        if (end - pos > 2 && *pos == '-' && *(pos + 1) == '-')
        {
            pos += 2;
            /// skip until the end of the line
            while (pos != end && *pos != '\n')
                ++pos;
            if (comment_callback)
                comment_callback(std::string_view(comment_begin, pos - comment_begin));
        }
        /// need to parse next sql
        else
            break;
    } while (pos != end);
}

String formatQuery(String query)
{
    const unsigned max_parser_depth = DBMS_DEFAULT_MAX_PARSER_DEPTH;
    const unsigned max_parser_backtracks = DBMS_DEFAULT_MAX_PARSER_BACKTRACKS;

    ParserQuery parser(query.data() + query.size(), /*allow_settings_after_format_in_insert_=*/ false, /*implicit_select_=*/ false);

    String res;
    res.reserve(query.size());

    auto comments_callback = [&](std::string_view comment)
    {
        res += comment;
        res += '\n';
    };

    const char * begin = query.data();
    const char * pos = begin;
    const char * end = begin + query.size();

    skipSpacesAndComments(pos, end, comments_callback);

    size_t queries = 0;
    while (pos < end)
    {
        const char * query_start = pos;
        const ASTPtr ast = parseQueryAndMovePosition(parser, pos, end, "query in editor", /*allow_multi_statements=*/ true, /*max_query_size=*/ 0, max_parser_depth, max_parser_backtracks);

        std::string_view insert_query_payload;
        if (auto * insert_ast = ast->as<ASTInsertQuery>(); insert_ast && insert_ast->data)
        {
            if (Poco::toLower(insert_ast->format) == "values")
            {
                /// Reset format to default to have `INSERT INTO table VALUES` instead of `INSERT INTO table VALUES FORMAT Values`
                insert_ast->format.clear();

                /// We assume that data ends with a newline character (same as in other places)
                const char * this_query_end = find_first_symbols<'\n'>(insert_ast->data, end);
                insert_ast->end = this_query_end;
                pos = this_query_end;

                /// Remove semicolon from the INSERT query payload, since it will be added explicitly below
                if (*insert_ast->end == '\n')
                {
                    while (insert_ast->end > insert_ast->data && std::isspace(*insert_ast->end))
                        --insert_ast->end;
                }
            }
            else
                pos = insert_ast->end;

            /// No need to use getReadBufferFromASTInsertQuery() here, since it does extra things that we do not need here (i.e. handle INFILE)
            insert_query_payload = std::string_view(insert_ast->data, insert_ast->end);
        }

        bool multiline_query = std::string_view(query_start, pos).contains('\n');
        if (multiline_query)
            res += ast->formatWithSecretsMultiLine();
        else
            res += ast->formatWithSecretsOneLine();

        if (!insert_query_payload.empty())
        {
            res += ' ';
            res += insert_query_payload;
        }

        bool need_query_delimiter = pos != end || queries > 0;
        if (need_query_delimiter)
        {
            res += ';';
            res += '\n';
        }

        ++queries;
        skipSpacesAndComments(pos, end, comments_callback);
    }

    return res;
}

}
