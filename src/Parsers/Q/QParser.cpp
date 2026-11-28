#include <Parsers/Q/QParser.h>

#include <Parsers/Q/QLexer.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>
#include <base/arithmeticOverflow.h>

#include <charconv>
#include <cmath>
#include <limits>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_DEEP_RECURSION;
}

char qTypeChar(QType type)
{
    switch (type)
    {
        case QType::Mixed: return ' ';
        case QType::Boolean: return 'b';
        case QType::Guid: return 'g';
        case QType::Byte: return 'x';
        case QType::Short: return 'h';
        case QType::Int: return 'i';
        case QType::Long: return 'j';
        case QType::Real: return 'e';
        case QType::Float: return 'f';
        case QType::Char: return 'c';
        case QType::Symbol: return 's';
        case QType::Timestamp: return 'p';
        case QType::Month: return 'm';
        case QType::Date: return 'd';
        case QType::Datetime: return 'z';
        case QType::Timespan: return 'n';
        case QType::Minute: return 'u';
        case QType::Second: return 'v';
        case QType::Time: return 't';
    }
    return ' ';
}

bool isQInfixKeyword(std::string_view name)
{
    static const std::unordered_set<std::string_view> keywords = {
        "and", "or", "mod", "div", "xbar", "xexp", "xlog", "in", "like", "within", "except", "inter", "union", "cross",
        "each", "peach", "over", "scan", "prior", "xasc", "xdesc", "xcol", "xcols", "xkey", "xgroup", "xrank", "xprev",
        "lj", "ljf", "ij", "ijf", "uj", "ujf", "pj", "asof", "fby", "wavg", "wsum", "mavg", "msum", "mmin", "mmax",
        "mcount", "mdev", "ema", "cor", "cov", "scov", "sublist", "ss", "sv", "vs", "set", "upsert", "insert", "bin",
        "binr", "rotate", "cut", "mmu", "lsq", "dsave", "setm", "rank_by", "ssr_dummy",
    };
    return keywords.contains(name);
}

namespace
{

/// The number of days from 1970-01-01 to the given date in the proleptic Gregorian calendar.
Int64 daysFromCivil(Int64 year, Int64 month, Int64 day)
{
    year -= month <= 2;
    const Int64 era = (year >= 0 ? year : year - 399) / 400;
    const Int64 year_of_era = year - era * 400;
    const Int64 day_of_year = (153 * (month > 2 ? month - 3 : month + 9) + 2) / 5 + day - 1;
    const Int64 day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    return era * 146097 + day_of_era - 719468;
}

constexpr Int64 days_1970_to_2000 = 10957;
constexpr Int64 nanoseconds_in_day = 86'400'000'000'000;
constexpr Int64 milliseconds_in_day = 86'400'000;

/// The dates 0001.01.01 and 9999.12.31 in days since 2000.01.01, and the months 0001.01 and 9999.12 in months since 2000.01:
/// the range of the dates in q.
constexpr Int64 min_date = -730119;
constexpr Int64 max_date = 2921939;
constexpr Int64 min_month = (1 - 2000) * 12;
constexpr Int64 max_month = (9999 - 2000) * 12 + 11;

[[noreturn]] void throwBadLiteral(std::string_view text)
{
    throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid literal '{}' in the q program", text);
}

[[noreturn]] void throwOutOfRange(std::string_view text)
{
    throw Exception(ErrorCodes::SYNTAX_ERROR, "The literal '{}' is out of range in the q program", text);
}

/// Checks that a temporal value is in the range of its type in q, so that the conversions of it between the units
/// (here and in the compiler) do not overflow: a date within the years 0001..9999, and a minute, a second, or a time
/// within 32 bits. Timestamps and timespans are counts of nanoseconds, which take all 64 bits.
void checkTemporalRange(QType type, Int64 value, std::string_view text)
{
    Int64 min = 0;
    Int64 max = 0;
    switch (type)
    {
        case QType::Date:
            min = min_date;
            max = max_date;
            break;
        case QType::Month:
            min = min_month;
            max = max_month;
            break;
        case QType::Datetime:
            min = min_date * milliseconds_in_day;
            max = (max_date + 1) * milliseconds_in_day - 1;
            break;
        case QType::Minute:
        case QType::Second:
        case QType::Time:
            min = -std::numeric_limits<Int32>::max();
            max = std::numeric_limits<Int32>::max();
            break;
        default:
            return;
    }
    if (value < min || value > max)
        throwOutOfRange(text);
}

bool parseUnsigned(std::string_view text, Int64 & out)
{
    if (text.empty())
        return false;
    for (char c : text)
        if (!isNumericASCII(c))
            return false;
    auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), out);
    return ec == std::errc() && ptr == text.data() + text.size();
}

/// Parses `yyyy.mm.dd` into the number of days since 2000.01.01.
bool parseDate(std::string_view text, Int64 & days)
{
    if (text.size() != 10 || text[4] != '.' || text[7] != '.')
        return false;
    Int64 year = 0;
    Int64 month = 0;
    Int64 day = 0;
    if (!parseUnsigned(text.substr(0, 4), year) || !parseUnsigned(text.substr(5, 2), month) || !parseUnsigned(text.substr(8, 2), day))
        return false;
    if (month < 1 || month > 12 || day < 1 || day > 31)
        return false;
    days = daysFromCivil(year, month, day) - days_1970_to_2000;
    return true;
}

/// Parses the fractional part of seconds, like `123` in `10:00:00.123`, into a count of units of `10^-digits` seconds.
bool parseFraction(std::string_view text, size_t digits, Int64 & out)
{
    if (text.size() > digits)
        return false;
    Int64 value = 0;
    if (!text.empty() && !parseUnsigned(text, value))
        return false;
    for (size_t i = text.size(); i < digits; ++i)
        value *= 10;
    out = value;
    return true;
}

/// Parses a time of day or a duration `hh[:mm[:ss[.fffffffff]]]` into nanoseconds.
/// `fraction_digits` receives the number of the digits after the point (or -1 when there are no seconds).
bool parseClock(std::string_view text, Int64 & nanoseconds, int & components, int & fraction_digits)
{
    components = 0;
    fraction_digits = -1;
    Int64 hours = 0;
    Int64 minutes = 0;
    Int64 seconds = 0;
    Int64 fraction = 0;

    size_t point = text.find('.');
    std::string_view clock = text.substr(0, point);
    if (point != std::string_view::npos)
    {
        std::string_view frac = text.substr(point + 1);
        if (!parseFraction(frac, 9, fraction))
            return false;
        fraction_digits = static_cast<int>(frac.size());
    }

    size_t first = clock.find(':');
    if (first == std::string_view::npos)
    {
        if (!parseUnsigned(clock, hours))
            return false;
        components = 1;
    }
    else
    {
        if (!parseUnsigned(clock.substr(0, first), hours))
            return false;
        std::string_view rest = clock.substr(first + 1);
        size_t second = rest.find(':');
        if (second == std::string_view::npos)
        {
            if (rest.size() != 2 || !parseUnsigned(rest, minutes))
                return false;
            components = 2;
        }
        else
        {
            if (second != 2 || rest.size() != 5 || !parseUnsigned(rest.substr(0, 2), minutes) || !parseUnsigned(rest.substr(3), seconds))
                return false;
            components = 3;
        }
    }
    if (point != std::string_view::npos && components != 3)
        return false;
    if (minutes >= 60 || seconds >= 60 || hours > 1'000'000)
        return false;

    nanoseconds = ((hours * 60 + minutes) * 60 + seconds) * 1'000'000'000 + fraction;
    return true;
}

struct DecodedNumber
{
    QType type = QType::Long;
    std::vector<QAtom> atoms;
    /// `101b` and `0x0102` are lists even though they are written as one token.
    bool is_list = false;
    /// Whether the type was given explicitly, by a suffix or by the form of the literal.
    bool explicit_type = false;
};

QType typeFromSuffix(char c)
{
    switch (c)
    {
        case 'b': return QType::Boolean;
        case 'g': return QType::Guid;
        case 'x': return QType::Byte;
        case 'h': return QType::Short;
        case 'i': return QType::Int;
        case 'j': return QType::Long;
        case 'e': return QType::Real;
        case 'f': return QType::Float;
        case 'c': return QType::Char;
        case 's': return QType::Symbol;
        case 'p': return QType::Timestamp;
        case 'm': return QType::Month;
        case 'd': return QType::Date;
        case 'z': return QType::Datetime;
        case 'n': return QType::Timespan;
        case 'u': return QType::Minute;
        case 'v': return QType::Second;
        case 't': return QType::Time;
        default: return QType::Mixed;
    }
}

bool isIntegral(QType type)
{
    return type == QType::Boolean || type == QType::Byte || type == QType::Short || type == QType::Int || type == QType::Long;
}

bool isFloating(QType type)
{
    return type == QType::Real || type == QType::Float;
}

/// Converts a clock value in nanoseconds to the unit of a time type.
Int64 clockToUnit(Int64 nanoseconds, QType type)
{
    switch (type)
    {
        case QType::Minute: return nanoseconds / 60'000'000'000;
        case QType::Second: return nanoseconds / 1'000'000'000;
        case QType::Time: return nanoseconds / 1'000'000;
        default: return nanoseconds;
    }
}

DecodedNumber decodeNumberImpl(std::string_view text);

/// Decodes a single number token, like `42`, `-1.5e3`, `0Nd`, `2024.01.15D10:30`, `101b`.
DecodedNumber decodeNumber(std::string_view text)
{
    DecodedNumber result = decodeNumberImpl(text);
    for (const auto & atom : result.atoms)
        if (!atom.is_null && !atom.infinity && atom.value.getType() == Field::Types::Int64)
            checkTemporalRange(result.type, atom.value.safeGet<Int64>(), text);
    return result;
}

DecodedNumber decodeNumberImpl(std::string_view text)
{
    DecodedNumber result;
    QAtom atom;

    bool negative = !text.empty() && text[0] == '-';
    std::string_view body = negative ? text.substr(1) : text;
    if (body.empty())
        throwBadLiteral(text);

    /// Bytes: 0x0a or 0x0a0b.
    if (body.starts_with("0x"))
    {
        std::string_view hex = body.substr(2);
        if (negative || hex.size() % 2 != 0)
            throwBadLiteral(text);
        result.type = QType::Byte;
        result.explicit_type = true;
        result.is_list = hex.size() != 2;
        for (size_t i = 0; i < hex.size(); i += 2)
        {
            unsigned value = 0;
            auto [ptr, ec] = std::from_chars(hex.data() + i, hex.data() + i + 2, value, 16);
            if (ec != std::errc() || ptr != hex.data() + i + 2)
                throwBadLiteral(text);
            result.atoms.push_back(QAtom{Field(static_cast<Int64>(value)), false, 0});
        }
        return result;
    }

    /// Nulls and infinities: 0N, 0n, 0Nd, 0W, 0w, -0W, 0Wj.
    if (body.size() >= 2 && body[0] == '0' && (body[1] == 'N' || body[1] == 'n' || body[1] == 'W' || body[1] == 'w'))
    {
        bool is_null = body[1] == 'N' || body[1] == 'n';
        QType type = (body[1] == 'n' || body[1] == 'w') ? QType::Float : QType::Long;
        if (body.size() == 3)
        {
            type = typeFromSuffix(body[2]);
            if (type == QType::Mixed || (!is_null && (type == QType::Symbol || type == QType::Char || type == QType::Boolean)))
                throwBadLiteral(text);
        }
        else if (body.size() != 2)
            throwBadLiteral(text);
        if (is_null && negative)
            throwBadLiteral(text);
        result.type = type;
        result.explicit_type = body.size() == 3;
        atom.is_null = is_null;
        atom.infinity = is_null ? 0 : (negative ? -1 : 1);
        result.atoms.push_back(std::move(atom));
        return result;
    }

    /// Booleans: 1b, 0101b.
    if (body.size() >= 2 && body.back() == 'b' && !negative)
    {
        std::string_view bits = body.substr(0, body.size() - 1);
        bool all_bits = true;
        for (char c : bits)
            all_bits &= (c == '0' || c == '1');
        if (all_bits)
        {
            result.type = QType::Boolean;
            result.explicit_type = true;
            result.is_list = bits.size() != 1;
            for (char c : bits)
                result.atoms.push_back(QAtom{Field(static_cast<Int64>(c - '0')), false, 0});
            return result;
        }
    }

    /// Temporal literals with a date part: 2024.01.15, 2024.01m, 2024.01.15D10:30:00, 2024.01.15T10:30:00.000.
    if (body.size() >= 7 && body[4] == '.' && isNumericASCII(body[0]) && isNumericASCII(body[5]))
    {
        if (negative)
            throwBadLiteral(text);
        if (body.size() == 8 && body[7] == 'm')
        {
            Int64 year = 0;
            Int64 month = 0;
            if (!parseUnsigned(body.substr(0, 4), year) || !parseUnsigned(body.substr(5, 2), month) || month < 1 || month > 12)
                throwBadLiteral(text);
            result.type = QType::Month;
            atom.value = Field((year - 2000) * 12 + (month - 1));
        }
        else
        {
            Int64 days = 0;
            if (body.size() < 10 || !parseDate(body.substr(0, 10), days))
                throwBadLiteral(text);
            std::string_view rest = body.substr(10);
            if (rest.empty() || rest == "d")
            {
                result.type = QType::Date;
                atom.value = Field(days);
            }
            else if (rest[0] == 'D' || rest[0] == 'p' || rest[0] == 'T' || rest[0] == 'z')
            {
                bool is_datetime = rest[0] == 'T' || rest[0] == 'z';
                std::string_view clock = rest.substr(1);
                Int64 nanoseconds = 0;
                int components = 0;
                int fraction_digits = 0;
                if (!clock.empty() && !parseClock(clock, nanoseconds, components, fraction_digits))
                    throwBadLiteral(text);
                if (nanoseconds >= nanoseconds_in_day)
                    throwBadLiteral(text);
                if (is_datetime)
                {
                    /// In milliseconds at once: the count of nanoseconds of a far date does not fit into 64 bits.
                    result.type = QType::Datetime;
                    atom.value = Field(days * milliseconds_in_day + nanoseconds / 1'000'000);
                }
                else
                {
                    /// A timestamp is a count of nanoseconds, which covers the years 1707..2292.
                    Int64 total = 0;
                    if (common::mulOverflow(days, nanoseconds_in_day, total) || common::addOverflow(total, nanoseconds, total))
                        throwOutOfRange(text);
                    result.type = QType::Timestamp;
                    atom.value = Field(total);
                }
            }
            else
                throwBadLiteral(text);
        }
        result.explicit_type = true;
        result.atoms.push_back(std::move(atom));
        return result;
    }

    /// Timespans: 0D01:15:00, 1D, 00:00:00.000000001.
    size_t day_mark = body.find('D');
    if (day_mark != std::string_view::npos)
    {
        Int64 days = 0;
        if (!parseUnsigned(body.substr(0, day_mark), days))
            throwBadLiteral(text);
        std::string_view clock = body.substr(day_mark + 1);
        if (!clock.empty() && clock.back() == 'n')
            clock.remove_suffix(1);
        Int64 nanoseconds = 0;
        int components = 0;
        int fraction_digits = 0;
        if (!clock.empty() && !parseClock(clock, nanoseconds, components, fraction_digits))
            throwBadLiteral(text);
        /// A timespan is a count of nanoseconds: `106751D23:47:16.854775807` is the last one that fits.
        Int64 total = 0;
        if (common::mulOverflow(days, nanoseconds_in_day, total) || common::addOverflow(total, nanoseconds, total))
            throwOutOfRange(text);
        result.type = QType::Timespan;
        result.explicit_type = true;
        atom.value = Field(negative ? -total : total);
        result.atoms.push_back(std::move(atom));
        return result;
    }

    /// Times of day: 10:30 (minute), 10:30:00 (second), 10:30:00.000 (time), 10:30:00.000000000 (timespan).
    if (body.contains(':'))
    {
        std::string_view clock = body;
        QType suffix = QType::Mixed;
        if (isAlphaASCII(clock.back()))
        {
            suffix = typeFromSuffix(clock.back());
            clock.remove_suffix(1);
            if (suffix != QType::Minute && suffix != QType::Second && suffix != QType::Time && suffix != QType::Timespan)
                throwBadLiteral(text);
        }
        Int64 nanoseconds = 0;
        int components = 0;
        int fraction_digits = 0;
        if (!parseClock(clock, nanoseconds, components, fraction_digits) || components < 2)
            throwBadLiteral(text);
        QType type = suffix;
        if (type == QType::Mixed)
        {
            if (components == 2)
                type = QType::Minute;
            else if (fraction_digits < 0)
                type = QType::Second;
            else if (fraction_digits <= 3)
                type = QType::Time;
            else
                type = QType::Timespan;
        }
        result.type = type;
        result.explicit_type = true;
        Int64 value = clockToUnit(nanoseconds, type);
        atom.value = Field(negative ? -value : value);
        result.atoms.push_back(std::move(atom));
        return result;
    }

    /// Plain numbers with an optional type suffix: 42, 42i, 1.5, 1e-3, 2f, .5e.
    std::string_view digits = body;
    QType suffix = QType::Mixed;
    if (isAlphaASCII(digits.back()))
    {
        suffix = typeFromSuffix(digits.back());
        /// `1e` is a real, but `1e5` is a float with an exponent.
        if (suffix == QType::Mixed || !(isIntegral(suffix) || isFloating(suffix) || suffix == QType::Timespan
                || suffix == QType::Timestamp || suffix == QType::Date || suffix == QType::Month
                || suffix == QType::Minute || suffix == QType::Second || suffix == QType::Time))
            throwBadLiteral(text);
        digits.remove_suffix(1);
    }

    bool is_float = digits.find_first_of(".eE") != std::string_view::npos;
    if (digits.empty())
        throwBadLiteral(text);

    if (is_float || isFloating(suffix))
    {
        if (suffix != QType::Mixed && !isFloating(suffix))
            throwBadLiteral(text);
        double value = 0;
        auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), value);
        if (ec != std::errc() || ptr != digits.data() + digits.size())
            throwBadLiteral(text);
        result.type = suffix == QType::Mixed ? QType::Float : suffix;
        result.explicit_type = suffix != QType::Mixed;
        atom.value = Field(negative ? -value : value);
    }
    else
    {
        Int64 value = 0;
        if (!parseUnsigned(digits, value))
            throwBadLiteral(text);
        result.type = suffix == QType::Mixed ? QType::Long : suffix;
        result.explicit_type = suffix != QType::Mixed;
        if (result.type == QType::Boolean && value > 1)
            throwBadLiteral(text);
        atom.value = Field(negative ? -value : value);
    }
    result.atoms.push_back(std::move(atom));
    return result;
}

void convertAtom(QAtom & atom, QType from, QType to)
{
    if (from == to || atom.is_null || atom.infinity)
        return;
    if (isFloating(to) && isIntegral(from))
        atom.value = Field(static_cast<Float64>(atom.value.safeGet<Int64>()));
}

/// Merges the numbers of a space-separated run like `1 2 3f` into one list.
std::shared_ptr<QNode> makeNumberList(const std::vector<const QToken *> & run)
{
    std::vector<DecodedNumber> decoded;
    decoded.reserve(run.size());
    for (const auto * token : run)
        decoded.push_back(decodeNumber(token->text));

    auto node = std::make_shared<QNode>(QNodeKind::Literal, run.front()->offset);
    if (decoded.size() == 1)
    {
        node->type = decoded[0].type;
        node->is_list = decoded[0].is_list;
        node->atoms = std::move(decoded[0].atoms);
        return node;
    }

    /// The type of the list: the explicit type of the last item, or float when any item is a float, or the common type.
    QType type = decoded.back().type;
    bool explicit_type = decoded.back().explicit_type;
    if (!explicit_type)
    {
        for (const auto & item : decoded)
            if (item.type == QType::Float)
                type = QType::Float;
    }

    node->type = type;
    node->is_list = true;
    for (size_t i = 0; i < decoded.size(); ++i)
    {
        auto & item = decoded[i];
        bool compatible = item.type == type
            || (isFloating(type) && isIntegral(item.type) && !item.explicit_type)
            || (isIntegral(type) && item.type == QType::Long && !item.explicit_type)
            || (item.atoms.size() == 1 && (item.atoms[0].is_null || item.atoms[0].infinity) && !item.explicit_type);
        if (!compatible || item.is_list)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Items of different types in the list literal '{}' in the q program", run[i]->text);
        for (auto & atom : item.atoms)
        {
            convertAtom(atom, item.type, type);
            if (isIntegral(type) && type != QType::Long && !atom.is_null && !atom.infinity)
            {
                Int64 value = atom.value.safeGet<Int64>();
                Int64 max = type == QType::Boolean ? 1 : (type == QType::Byte ? 255 : (type == QType::Short ? 32767 : 2147483647));
                if (value > max || value < (type == QType::Boolean || type == QType::Byte ? 0 : -max))
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "The literal '{}' is out of range in the q program", run[i]->text);
            }
            node->atoms.push_back(std::move(atom));
        }
    }
    return node;
}

class QParser
{
public:
    QParser(std::string_view text_, size_t max_depth_)
        : text(text_), tokens(tokenizeQ(text_)), max_depth(max_depth_)
    {
    }

    QProgram parseProgram()
    {
        QProgram program;
        while (true)
        {
            QNodePtr statement = parseExpression(Stop{});
            program.statements.push_back(statement);
            const QToken & token = peek();
            if (token.type == QTokenType::End)
                break;
            if (token.type == QTokenType::Semicolon || token.type == QTokenType::StatementBreak)
            {
                ++pos;
                continue;
            }
            throwUnexpected(token);
        }
        return program;
    }

private:
    std::string_view text;
    QTokens tokens;
    size_t pos = 0;
    size_t max_depth;
    size_t depth = 0;

    /// What ends an expression, in addition to `;`, a statement break, closing brackets, and the end.
    struct Stop
    {
        /// A top-level `,`, which separates the phrases of a q-sql template.
        bool comma = false;
        /// The q-sql keywords `by`, `from`, `where`.
        bool by = false;
        bool from = false;
        bool where = false;
    };

    struct DepthGuard
    {
        QParser & parser;
        explicit DepthGuard(QParser & parser_) : parser(parser_)
        {
            checkStackSize();
            ++parser.depth;
            if (parser.max_depth && parser.depth > parser.max_depth)
                throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "The q program is too deeply nested (the maximum depth is {})", parser.max_depth);
        }
        ~DepthGuard() { --parser.depth; }
    };

    struct Item
    {
        QNodePtr node;
        bool is_verb = false;
    };

    [[nodiscard]] const QToken & peek(size_t ahead = 0) const
    {
        return tokens[std::min(pos + ahead, tokens.size() - 1)];
    }

    [[noreturn]] void throwUnexpected(const QToken & token) const
    {
        if (token.type == QTokenType::End)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected end of the q program");
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected '{}' at position {} of the q program", token.text, token.offset + 1);
    }

    void expect(QTokenType type, std::string_view what)
    {
        const QToken & token = peek();
        if (token.type != type)
        {
            if (token.type == QTokenType::End)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected '{}', but the q program has ended", what);
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected '{}' at position {} of the q program, got '{}'", what, token.offset + 1, token.text);
        }
        ++pos;
    }

    static bool isKeyword(const QToken & token, std::string_view keyword)
    {
        return token.type == QTokenType::Name && token.text == keyword;
    }

    [[nodiscard]] bool isStop(const QToken & token, const Stop & stop) const
    {
        switch (token.type)
        {
            case QTokenType::Semicolon:
            case QTokenType::StatementBreak:
            case QTokenType::CloseParen:
            case QTokenType::CloseBracket:
            case QTokenType::CloseBrace:
            case QTokenType::End:
                return true;
            case QTokenType::Verb:
                return stop.comma && token.text == ",";
            case QTokenType::Name:
                return (stop.by && token.text == "by") || (stop.from && token.text == "from") || (stop.where && token.text == "where");
            default:
                return false;
        }
    }

    static std::shared_ptr<QNode> makeNode(QNodeKind kind, size_t offset)
    {
        return std::make_shared<QNode>(kind, offset);
    }

    /// Sets the height of a node from its parts, and checks it against the limit. The nesting of the brackets is limited
    /// by `DepthGuard`, but a sequence of operators (`1+1+1+...`) or of brackets and adverbs after an item (`f[1][2][3]...`)
    /// makes a tree as deep as the sequence is long with no nesting in the text, and the tree is processed recursively.
    void finish(QNode & node) const
    {
        size_t height = 0;
        auto visit = [&](const QNodePtr & part)
        {
            if (part)
                height = std::max(height, part->height);
        };
        for (const auto & child : node.children)
            visit(child);
        for (const auto * columns : {&node.key_columns, &node.columns})
            for (const auto & column : *columns)
                visit(column.expression);
        if (node.sql)
        {
            for (const auto * columns : {&node.sql->columns, &node.sql->by})
                for (const auto & column : *columns)
                    visit(column.expression);
            visit(node.sql->from);
            visit(node.sql->limit);
            for (const auto & constraint : node.sql->where)
                visit(constraint);
        }
        node.height = height + 1;
        if (max_depth && node.height > max_depth)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "The q program is too deeply nested (the maximum depth is {})", max_depth);
    }

    /// Parses an expression up to a stop token, without consuming it. Returns `nullptr` for an empty expression.
    QNodePtr parseExpression(const Stop & stop)
    {
        DepthGuard guard(*this);
        std::vector<Item> items;

        while (!isStop(peek(), stop))
        {
            const QToken & token = peek();

            /// An assignment: `name:value` or `name::value`. The value is the rest of the expression.
            if (token.type == QTokenType::Name && (peek(1).type == QTokenType::Colon || peek(1).type == QTokenType::DoubleColon)
                && !isSQLKeyword(token.text))
            {
                pos += 2;
                auto node = makeNode(QNodeKind::Assign, token.offset);
                node->name = String(token.text);
                QNodePtr value = parseExpression(stop);
                if (!value)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing the value of the assignment to '{}' in the q program", token.text);
                node->children.push_back(value);
                finish(*node);
                items.push_back(Item{node, false});
                break;
            }

            if (token.type == QTokenType::Name && isSQLKeyword(token.text))
            {
                items.push_back(Item{parseTemplate(), false});
                break;
            }

            Item item = parseItem();
            parsePostfix(item);
            items.push_back(std::move(item));
        }

        return reduce(items);
    }

    static bool isSQLKeyword(std::string_view name)
    {
        return name == "select" || name == "exec" || name == "update" || name == "delete";
    }

    /// Reduces a sequence of nouns and verbs right to left.
    QNodePtr reduce(std::vector<Item> & items)
    {
        if (items.empty())
            return nullptr;

        size_t i = items.size() - 1;
        QNodePtr result = items[i].node;
        while (i > 0)
        {
            --i;
            const Item & item = items[i];
            if (item.is_verb)
            {
                if (i > 0 && !items[i - 1].is_verb)
                {
                    auto node = makeNode(QNodeKind::Apply, item.node->offset);
                    node->children = {item.node, items[i - 1].node, result};
                    node->infix = true;
                    finish(*node);
                    result = node;
                    --i;
                }
                else
                {
                    auto node = makeNode(QNodeKind::Apply, item.node->offset);
                    node->children = {item.node, result};
                    node->juxtaposition = true;
                    finish(*node);
                    result = node;
                }
            }
            else
            {
                auto node = makeNode(QNodeKind::Apply, item.node->offset);
                node->children = {item.node, result};
                node->juxtaposition = true;
                finish(*node);
                result = node;
            }
        }
        return result;
    }

    Item parseItem()
    {
        const QToken & token = peek();
        switch (token.type)
        {
            case QTokenType::Number:
            {
                std::vector<const QToken *> run{&token};
                ++pos;
                while (peek().type == QTokenType::Number && peek().space_before)
                {
                    run.push_back(&peek());
                    ++pos;
                }
                return Item{makeNumberList(run), false};
            }
            case QTokenType::Symbol:
            {
                auto node = makeNode(QNodeKind::Literal, token.offset);
                node->type = QType::Symbol;
                node->atoms.push_back(QAtom{Field(token.value), false, 0});
                ++pos;
                while (peek().type == QTokenType::Symbol && !peek().space_before)
                {
                    node->atoms.push_back(QAtom{Field(peek().value), false, 0});
                    node->is_list = true;
                    ++pos;
                }
                return Item{node, false};
            }
            case QTokenType::String:
            {
                auto node = makeNode(QNodeKind::Literal, token.offset);
                node->type = QType::Char;
                node->is_list = token.value.size() != 1;
                node->atoms.push_back(QAtom{Field(token.value), false, 0});
                ++pos;
                return Item{node, false};
            }
            case QTokenType::Name:
            {
                auto node = makeNode(QNodeKind::Name, token.offset);
                node->name = String(token.text);
                ++pos;
                bool is_verb = isQInfixKeyword(token.text) && peek().type != QTokenType::OpenBracket;
                return Item{node, is_verb};
            }
            case QTokenType::Verb:
            case QTokenType::Colon:
            {
                auto node = makeNode(QNodeKind::Verb, token.offset);
                node->name = String(token.text);
                ++pos;
                return Item{node, true};
            }
            case QTokenType::DoubleColon:
            {
                auto node = makeNode(QNodeKind::Name, token.offset);
                node->name = "::";
                ++pos;
                return Item{node, false};
            }
            case QTokenType::OpenParen:
                return Item{parseParen(), false};
            case QTokenType::OpenBrace:
                return Item{parseLambda(), false};
            case QTokenType::OpenBracket:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Expression blocks in brackets (at position {}) are not supported in the q dialect", token.offset + 1);
            case QTokenType::Adverb:
                throw Exception(ErrorCodes::SYNTAX_ERROR, "The adverb '{}' at position {} of the q program has no function to modify", token.text, token.offset + 1);
            default:
                throwUnexpected(token);
        }
    }

    /// Parses the brackets of an application and the adverbs that follow an item.
    void parsePostfix(Item & item)
    {
        while (true)
        {
            const QToken & token = peek();
            if (token.type == QTokenType::OpenBracket && !token.space_before)
            {
                size_t offset = token.offset;
                ++pos;
                auto node = makeNode(QNodeKind::Apply, offset);
                node->children.push_back(item.node);
                parseBracketArguments(node->children);
                finish(*node);
                item = Item{node, false};
            }
            else if (token.type == QTokenType::Adverb && !token.space_before)
            {
                auto node = makeNode(QNodeKind::Derived, token.offset);
                node->name = String(token.text);
                node->children.push_back(item.node);
                finish(*node);
                ++pos;
                item = Item{node, true};
            }
            else
                break;
        }
    }

    /// Parses `a;b;c]` after an opening bracket. An empty argument is a `nullptr`; `f[]` has one empty argument.
    void parseBracketArguments(std::vector<QNodePtr> & out)
    {
        DepthGuard guard(*this);
        while (true)
        {
            out.push_back(parseExpression(Stop{}));
            const QToken & token = peek();
            if (token.type == QTokenType::Semicolon)
            {
                ++pos;
                continue;
            }
            if (token.type == QTokenType::StatementBreak)
            {
                ++pos;
                continue;
            }
            expect(QTokenType::CloseBracket, "]");
            break;
        }
    }

    QNodePtr parseParen()
    {
        DepthGuard guard(*this);
        size_t offset = peek().offset;
        ++pos;

        if (peek().type == QTokenType::OpenBracket)
            return parseTableLiteral(offset);

        if (peek().type == QTokenType::CloseParen)
        {
            ++pos;
            return makeNode(QNodeKind::List, offset);
        }

        std::vector<QNodePtr> items;
        while (true)
        {
            items.push_back(parseExpression(Stop{}));
            if (peek().type == QTokenType::Semicolon)
            {
                ++pos;
                continue;
            }
            expect(QTokenType::CloseParen, ")");
            break;
        }

        if (items.size() == 1)
        {
            if (!items[0])
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Empty parentheses at position {} of the q program", offset + 1);
            return items[0];
        }

        auto node = makeNode(QNodeKind::List, offset);
        for (const auto & item : items)
        {
            if (!item)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "An empty item in the list at position {} of the q program", offset + 1);
            node->children.push_back(item);
        }
        finish(*node);
        return node;
    }

    /// Parses a named column of a table literal or a q-sql phrase: `name:expr` or `expr`.
    QColumn parseColumn(const Stop & stop)
    {
        QColumn column;
        if (peek().type == QTokenType::Name && peek(1).type == QTokenType::Colon && !isSQLKeyword(peek().text))
        {
            column.name = String(peek().text);
            pos += 2;
        }
        column.expression = parseExpression(stop);
        if (!column.expression)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "An empty column at position {} of the q program", peek().offset + 1);
        return column;
    }

    /// Parses `[keys] columns)` after `(`.
    QNodePtr parseTableLiteral(size_t offset)
    {
        auto node = makeNode(QNodeKind::Table, offset);
        ++pos;
        if (peek().type != QTokenType::CloseBracket)
        {
            while (true)
            {
                node->key_columns.push_back(parseColumn(Stop{}));
                if (peek().type == QTokenType::Semicolon)
                {
                    ++pos;
                    continue;
                }
                break;
            }
        }
        expect(QTokenType::CloseBracket, "]");

        if (peek().type != QTokenType::CloseParen)
        {
            while (true)
            {
                node->columns.push_back(parseColumn(Stop{}));
                if (peek().type == QTokenType::Semicolon)
                {
                    ++pos;
                    continue;
                }
                break;
            }
        }
        expect(QTokenType::CloseParen, ")");
        finish(*node);
        return node;
    }

    static void collectImplicitParams(const QNodePtr & node, bool & has_x, bool & has_y, bool & has_z)
    {
        checkStackSize();
        if (!node || node->kind == QNodeKind::Lambda)
            return;
        if (node->kind == QNodeKind::Name)
        {
            has_x |= node->name == "x";
            has_y |= node->name == "y";
            has_z |= node->name == "z";
        }
        for (const auto & child : node->children)
            collectImplicitParams(child, has_x, has_y, has_z);
        for (const auto * columns : {&node->columns, &node->key_columns})
            for (const auto & column : *columns)
                collectImplicitParams(column.expression, has_x, has_y, has_z);
        if (node->sql)
        {
            for (const auto * columns : {&node->sql->columns, &node->sql->by})
                for (const auto & column : *columns)
                    collectImplicitParams(column.expression, has_x, has_y, has_z);
            collectImplicitParams(node->sql->from, has_x, has_y, has_z);
            collectImplicitParams(node->sql->limit, has_x, has_y, has_z);
            for (const auto & constraint : node->sql->where)
                collectImplicitParams(constraint, has_x, has_y, has_z);
        }
    }

    QNodePtr parseLambda()
    {
        DepthGuard guard(*this);
        const QToken & open = peek();
        size_t offset = open.offset;
        ++pos;

        auto node = makeNode(QNodeKind::Lambda, offset);
        bool explicit_params = false;
        if (peek().type == QTokenType::OpenBracket)
        {
            explicit_params = true;
            ++pos;
            while (peek().type != QTokenType::CloseBracket)
            {
                if (peek().type != QTokenType::Name)
                    throwUnexpected(peek());
                node->params.push_back(String(peek().text));
                ++pos;
                if (peek().type == QTokenType::Semicolon)
                    ++pos;
                else if (peek().type != QTokenType::CloseBracket)
                    throwUnexpected(peek());
            }
            ++pos;
            if (node->params.size() > 8)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "A lambda at position {} of the q program has more than 8 parameters", offset + 1);
        }

        while (true)
        {
            node->children.push_back(parseExpression(Stop{}));
            if (peek().type == QTokenType::Semicolon || peek().type == QTokenType::StatementBreak)
            {
                ++pos;
                continue;
            }
            break;
        }
        const QToken & close = peek();
        expect(QTokenType::CloseBrace, "}");
        node->source = String(text.substr(offset, close.offset + 1 - offset));
        finish(*node);

        if (!explicit_params)
        {
            bool has_x = false;
            bool has_y = false;
            bool has_z = false;
            for (const auto & statement : node->children)
                collectImplicitParams(statement, has_x, has_y, has_z);
            if (has_z)
                node->params = {"x", "y", "z"};
            else if (has_y)
                node->params = {"x", "y"};
            else
                node->params = {"x"};
        }
        return node;
    }

    /// Parses a comma-separated list of phrases of a q-sql template.
    void parsePhrases(QColumns & out, Stop stop, bool allow_names)
    {
        stop.comma = true;
        while (true)
        {
            if (allow_names)
                out.push_back(parseColumn(stop));
            else
            {
                QNodePtr expression = parseExpression(stop);
                if (!expression)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "An empty phrase at position {} of the q program", peek().offset + 1);
                out.push_back(QColumn{"", expression});
            }
            if (peek().type == QTokenType::Verb && peek().text == ",")
            {
                ++pos;
                continue;
            }
            break;
        }
    }

    QNodePtr parseTemplate()
    {
        DepthGuard guard(*this);
        const QToken & keyword = peek();
        auto node = makeNode(QNodeKind::SQL, keyword.offset);
        auto sql = std::make_shared<QSQLTemplate>();
        if (keyword.text == "select")
            sql->kind = QSQLTemplate::Kind::Select;
        else if (keyword.text == "exec")
            sql->kind = QSQLTemplate::Kind::Exec;
        else if (keyword.text == "update")
            sql->kind = QSQLTemplate::Kind::Update;
        else
            sql->kind = QSQLTemplate::Kind::Delete;
        ++pos;

        /// select[n], select[n;>col], select[>col], select[offset count].
        if (sql->kind == QSQLTemplate::Kind::Select && peek().type == QTokenType::OpenBracket && !peek().space_before)
        {
            ++pos;
            while (true)
            {
                if (peek().type == QTokenType::Verb && (peek().text == "<" || peek().text == ">") && peek(1).type == QTokenType::Name)
                {
                    sql->order_descending = peek().text == ">";
                    sql->order_column = String(peek(1).text);
                    pos += 2;
                }
                else
                {
                    sql->limit = parseExpression(Stop{});
                    if (!sql->limit)
                        throwUnexpected(peek());
                }
                if (peek().type == QTokenType::Semicolon)
                {
                    ++pos;
                    continue;
                }
                break;
            }
            expect(QTokenType::CloseBracket, "]");
        }

        if (sql->kind == QSQLTemplate::Kind::Select && isKeyword(peek(), "distinct"))
        {
            sql->distinct = true;
            ++pos;
        }

        Stop phrase_stop;
        phrase_stop.by = sql->kind != QSQLTemplate::Kind::Delete;
        phrase_stop.from = true;

        if (!isKeyword(peek(), "by") && !isKeyword(peek(), "from"))
            parsePhrases(sql->columns, phrase_stop, sql->kind != QSQLTemplate::Kind::Delete);

        if (isKeyword(peek(), "by"))
        {
            if (sql->kind == QSQLTemplate::Kind::Delete)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "'delete' has no by phrase in the q program");
            ++pos;
            sql->has_by = true;
            Stop by_stop;
            by_stop.from = true;
            if (!isKeyword(peek(), "from"))
                parsePhrases(sql->by, by_stop, true);
        }

        if (!isKeyword(peek(), "from"))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Expected 'from' in the q-sql template '{}' at position {}", keyword.text, keyword.offset + 1);
        ++pos;

        Stop from_stop;
        from_stop.where = true;
        sql->from = parseExpression(from_stop);
        if (!sql->from)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing the table after 'from' in the q-sql template at position {}", keyword.offset + 1);

        if (isKeyword(peek(), "where"))
        {
            ++pos;
            QColumns constraints;
            parsePhrases(constraints, Stop{}, false);
            for (auto & constraint : constraints)
                sql->where.push_back(constraint.expression);
        }

        if (sql->kind == QSQLTemplate::Kind::Delete && !sql->columns.empty() && !sql->where.empty())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "'delete' takes either the columns or the where phrase, but not both");
        if (sql->kind == QSQLTemplate::Kind::Update && sql->columns.empty())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "'update' requires the columns to update");

        node->sql = std::move(sql);
        finish(*node);
        return node;
    }
};

}

QProgram parseQProgram(std::string_view text, size_t max_depth)
{
    return QParser(text, max_depth).parseProgram();
}

}
