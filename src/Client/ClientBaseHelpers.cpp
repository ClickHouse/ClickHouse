#include "ClientBaseHelpers.h"


#include <Common/DateLUT.h>
#include <Common/LocalDate.h>
#include <Parsers/Lexer.h>
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

    static const std::unordered_map<TokenType, Replxx::Color> token_to_color
        = {{TokenType::Whitespace, Replxx::Color::DEFAULT},
            {TokenType::Comment, Replxx::Color::GRAY},
            {TokenType::BareWord, Replxx::Color::DEFAULT},
            {TokenType::Number, Replxx::Color::GREEN},
            {TokenType::StringLiteral, Replxx::Color::CYAN},
            {TokenType::QuotedIdentifier, Replxx::Color::MAGENTA},
            {TokenType::OpeningRoundBracket, Replxx::Color::BROWN},
            {TokenType::ClosingRoundBracket, Replxx::Color::BROWN},
            {TokenType::OpeningSquareBracket, Replxx::Color::BROWN},
            {TokenType::ClosingSquareBracket, Replxx::Color::BROWN},
            {TokenType::DoubleColon, Replxx::Color::BROWN},
            {TokenType::OpeningCurlyBrace, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::ClosingCurlyBrace, replxx::color::bold(Replxx::Color::DEFAULT)},

            {TokenType::Comma, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Semicolon, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::VerticalDelimiter, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Dot, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Asterisk, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::HereDoc, Replxx::Color::CYAN},
            {TokenType::Plus, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Minus, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Slash, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Percent, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Arrow, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::QuestionMark, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Colon, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Equals, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::NotEquals, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Less, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Greater, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::LessOrEquals, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::GreaterOrEquals, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::Concatenation, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::At, replxx::color::bold(Replxx::Color::DEFAULT)},
            {TokenType::DoubleAt, Replxx::Color::MAGENTA},

            {TokenType::EndOfStream, Replxx::Color::DEFAULT},

            {TokenType::Error, Replxx::Color::RED},
            {TokenType::ErrorMultilineCommentIsNotClosed, Replxx::Color::RED},
            {TokenType::ErrorSingleQuoteIsNotClosed, Replxx::Color::RED},
            {TokenType::ErrorDoubleQuoteIsNotClosed, Replxx::Color::RED},
            {TokenType::ErrorSinglePipeMark, Replxx::Color::RED},
            {TokenType::ErrorWrongNumber, Replxx::Color::RED},
            {TokenType::ErrorMaxQuerySizeExceeded, Replxx::Color::RED}};

    const Replxx::Color unknown_token_color = Replxx::Color::RED;

    Lexer lexer(query.data(), query.data() + query.size());
    size_t pos = 0;

    for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
    {
        if (token.type == TokenType::Semicolon || token.type == TokenType::VerticalDelimiter)
            ReplxxLineReader::setLastIsDelimiter(true);
        else if (token.type != TokenType::Whitespace)
            ReplxxLineReader::setLastIsDelimiter(false);

        size_t utf8_len = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(token.begin), token.size());
        for (size_t code_point_index = 0; code_point_index < utf8_len; ++code_point_index)
        {
            if (token_to_color.find(token.type) != token_to_color.end())
                colors[pos + code_point_index] = token_to_color.at(token.type);
            else
                colors[pos + code_point_index] = unknown_token_color;
        }

        pos += utf8_len;
    }
}
#endif

}
