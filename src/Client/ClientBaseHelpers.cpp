#include "ClientBaseHelpers.h"

#include <Common/DateLUT.h>
#include <Common/LocalDate.h>
#include <Parsers/ParserQuery.h>
#include <Common/UTF8Helpers.h>


namespace DB
{

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
        else if (d > days)
            return false;
    }
    return false;
}

#if USE_REPLXX
void highlight(const String & query, std::vector<replxx::Replxx::Color> & colors)
{
    using namespace replxx;

    if (colors.empty())
        return;

    static const std::unordered_map<Highlight, Replxx::Color> type_to_color =
    {
        {Highlight::keyword, replxx::color::bold(Replxx::Color::DEFAULT)},
        {Highlight::identifier, Replxx::Color::CYAN},
        {Highlight::function, Replxx::Color::BROWN},
        {Highlight::alias, Replxx::Color::MAGENTA},
        {Highlight::substitution, Replxx::Color::MAGENTA},
        {Highlight::number, Replxx::Color::BRIGHTGREEN},
        {Highlight::string, Replxx::Color::GREEN},
    };

    const char * begin = query.data();
    const char * end = begin + query.size();
    Tokens tokens(begin, end, 1000, true);
    IParser::Pos token_iterator(tokens, static_cast<uint32_t>(1000), static_cast<uint32_t>(10000));
    Expected expected;
    ParserQuery parser(end);
    ASTPtr ast;
    bool parse_res = false;

    try
    {
        parse_res = parser.parse(token_iterator, ast, expected);
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
            pos += UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(prev), range.begin - prev);
            size_t utf8_len = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(range.begin), range.end - range.begin);

            for (size_t code_point_index = 0; code_point_index < utf8_len; ++code_point_index)
                colors[pos + code_point_index] = it->second;

            pos += utf8_len;
            prev = range.end;
        }
    }

    Token last_token = token_iterator.max();

    if (!parse_res || last_token.isError() || (!token_iterator->isEnd() && token_iterator->type != TokenType::Semicolon))
    {
        pos += UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(prev), expected.max_parsed_pos - prev);

        if (pos >= colors.size())
            pos = colors.size() - 1;

        colors[pos] = Replxx::Color::BRIGHTRED;
    }

    if (last_token.type == TokenType::Semicolon || last_token.type == TokenType::VerticalDelimiter)
        ReplxxLineReader::setLastIsDelimiter(true);
    else if (last_token.type != TokenType::Whitespace)
        ReplxxLineReader::setLastIsDelimiter(false);
}
#endif

}
