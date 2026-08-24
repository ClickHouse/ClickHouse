#include <gtest/gtest.h>

#include <IO/parseHTTPDate.h>

using namespace DB;

namespace
{

/// 2026-07-28 00:00:00 GMT. Only the obsolete RFC 850 format's two-digit year depends on it.
constexpr time_t reference_time = 1785196800;

std::optional<time_t> parse(std::string_view date)
{
    return tryParseHTTPDate(date, reference_time);
}

}

TEST(ParseHTTPDate, IMFFixdate)
{
    /// The example from RFC 9110, 5.6.7.
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08:49:37 GMT"), 784111777);

    EXPECT_EQ(parse("Thu, 01 Jan 1970 00:00:00 GMT"), 0);
    EXPECT_EQ(parse("Wed, 31 Dec 1969 23:59:59 GMT"), -1);
    EXPECT_EQ(parse("Wed, 21 Oct 2015 07:28:00 GMT"), 1445412480);
    EXPECT_EQ(parse("Tue, 19 Jan 2038 03:14:08 GMT"), 2147483648);

    /// A leap day.
    EXPECT_EQ(parse("Sat, 29 Feb 2020 12:00:00 GMT"), 1582977600);

    /// Every month is recognized.
    EXPECT_EQ(parse("Thu, 01 Jan 2026 00:00:00 GMT"), 1767225600);
    EXPECT_EQ(parse("Sun, 01 Feb 2026 00:00:00 GMT"), 1769904000);
    EXPECT_EQ(parse("Sun, 01 Mar 2026 00:00:00 GMT"), 1772323200);
    EXPECT_EQ(parse("Wed, 01 Apr 2026 00:00:00 GMT"), 1775001600);
    EXPECT_EQ(parse("Fri, 01 May 2026 00:00:00 GMT"), 1777593600);
    EXPECT_EQ(parse("Mon, 01 Jun 2026 00:00:00 GMT"), 1780272000);
    EXPECT_EQ(parse("Wed, 01 Jul 2026 00:00:00 GMT"), 1782864000);
    EXPECT_EQ(parse("Sat, 01 Aug 2026 00:00:00 GMT"), 1785542400);
    EXPECT_EQ(parse("Tue, 01 Sep 2026 00:00:00 GMT"), 1788220800);
    EXPECT_EQ(parse("Thu, 01 Oct 2026 00:00:00 GMT"), 1790812800);
    EXPECT_EQ(parse("Sun, 01 Nov 2026 00:00:00 GMT"), 1793491200);
    EXPECT_EQ(parse("Tue, 01 Dec 2026 00:00:00 GMT"), 1796083200);
}

TEST(ParseHTTPDate, RFC850Date)
{
    /// The example from RFC 9110, 5.6.7, which denotes the same instant as the `IMF-fixdate` example.
    EXPECT_EQ(parse("Sunday, 06-Nov-94 08:49:37 GMT"), 784111777);

    /// Every long day name is recognized; they have different lengths.
    EXPECT_EQ(parse("Monday, 06-Nov-94 08:49:37 GMT"), 784111777);
    EXPECT_EQ(parse("Tuesday, 06-Nov-94 08:49:37 GMT"), 784111777);
    EXPECT_EQ(parse("Wednesday, 06-Nov-94 08:49:37 GMT"), 784111777);
    EXPECT_EQ(parse("Thursday, 06-Nov-94 08:49:37 GMT"), 784111777);
    EXPECT_EQ(parse("Friday, 06-Nov-94 08:49:37 GMT"), 784111777);
    EXPECT_EQ(parse("Saturday, 06-Nov-94 08:49:37 GMT"), 784111777);

    /// The abbreviated day name belongs to the other two forms only.
    EXPECT_EQ(parse("Sun, 06-Nov-94 08:49:37 GMT"), std::nullopt);

    EXPECT_EQ(parse("Sunday, 06-Nov-26 08:49:37 GMT"), 1793954977);
    EXPECT_EQ(parse("Sunday, 06 Nov 94 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sunday, 06-Nov-94 08:49:37 UTC"), std::nullopt);
    EXPECT_EQ(parse("Sunday, 06-Nov-1994 08:49:37 GMT"), std::nullopt);
}

TEST(ParseHTTPDate, RFC850TwoDigitYear)
{
    /// RFC 9110, 5.6.7: a timestamp more than 50 years in the future denotes the most recent past year
    /// with the same last two digits. The reference time is 2026-07-28, so the cutoff is 2076-07-28.
    EXPECT_EQ(parse("Fri, 06-Nov-76 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Monday, 06-Jan-76 08:49:37 GMT"), 3345526177);
    EXPECT_EQ(parse("Saturday, 06-Nov-76 08:49:37 GMT"), 216118177);
    EXPECT_EQ(parse("Sunday, 06-Nov-77 08:49:37 GMT"), 247654177);

    /// A different reference time moves the window.
    EXPECT_EQ(tryParseHTTPDate("Sunday, 06-Nov-94 08:49:37 GMT", 784111777), 784111777);
    EXPECT_EQ(tryParseHTTPDate("Sunday, 06-Nov-69 08:49:37 GMT", 784111777), -4806623);
}

TEST(ParseHTTPDate, RFC850TwoDigitYearCutoff)
{
    /// The cutoff is a whole timestamp, not just a year: with a reference time of 2026-01-01 the last
    /// instant that is not more than 50 years ahead is 2076-01-01 00:00:00, so a day later already
    /// denotes 1976. Comparing years alone would keep both in 2076.
    constexpr time_t start_of_2026 = 1767225600;

    EXPECT_EQ(tryParseHTTPDate("Wednesday, 01-Jan-76 00:00:00 GMT", start_of_2026), 3345062400);
    EXPECT_EQ(tryParseHTTPDate("Friday, 02-Jan-76 00:00:00 GMT", start_of_2026), 189388800);
}

TEST(ParseHTTPDate, RFC850CenturyRollover)
{
    /// Near the end of a century a small two-digit year denotes the next century: seen from
    /// 1999-12-31, 01-Jan-00 is one day ahead and denotes 2000, not 1900.
    constexpr time_t last_day_of_1999 = 946598400;
    EXPECT_EQ(tryParseHTTPDate("Saturday, 01-Jan-00 00:00:00 GMT", last_day_of_1999), 946684800);

    /// And right after the rollover a large two-digit year still denotes the previous century.
    constexpr time_t start_of_2000 = 946684800;
    EXPECT_EQ(tryParseHTTPDate("Friday, 31-Dec-99 00:00:00 GMT", start_of_2000), 946598400);

    /// A timestamp more than 50 years in the past denotes the next year with the same last two
    /// digits, which may be in the next century: seen from 2060, 06 means 2106, not 2006.
    constexpr time_t start_of_2060 = 2840140800;
    EXPECT_EQ(tryParseHTTPDate("Saturday, 06-Nov-06 08:49:37 GMT", start_of_2060), 4318476577);

    /// The year is chosen before the day of month is validated: seen from 1950, 29-Feb-00 denotes
    /// 2000-02-29 even though 1900 had no 29 February.
    constexpr time_t mid_1950 = -616896000;
    EXPECT_EQ(tryParseHTTPDate("Tuesday, 29-Feb-00 00:00:00 GMT", mid_1950), 951782400);
}

TEST(ParseHTTPDate, AsctimeDate)
{
    /// The example from RFC 9110, 5.6.7, which denotes the same instant as the `IMF-fixdate` example.
    EXPECT_EQ(parse("Sun Nov  6 08:49:37 1994"), 784111777);

    /// The day of month may also be zero-padded rather than space-padded.
    EXPECT_EQ(parse("Sun Nov 06 08:49:37 1994"), 784111777);
    EXPECT_EQ(parse("Sun Nov 16 08:49:37 1994"), 784975777);

    /// A space-padded day of month must be a single digit.
    EXPECT_EQ(parse("Sun Nov  16 08:49:37 199"), std::nullopt);
    EXPECT_EQ(parse("Sun Nov   6 08:49:37 199"), std::nullopt);

    /// This form carries no time zone.
    EXPECT_EQ(parse("Sun Nov  6 08:49:37 GMT "), std::nullopt);
    EXPECT_EQ(parse("Sun Nov  6 1994 08:49:37"), std::nullopt);
    EXPECT_EQ(parse("Sunday Nov  6 08:49:37 19"), std::nullopt);
}

TEST(ParseHTTPDate, CaseInsensitive)
{
    /// RFC 9110, 5.6.7 defines `HTTP-date` as case sensitive, but RFC 9111, 4.2 relaxes that for cache
    /// recipients, and `Last-Modified` is read to validate cached schemas and row counts.
    EXPECT_EQ(parse("sun, 06 nov 1994 08:49:37 gmt"), 784111777);
    EXPECT_EQ(parse("SUN, 06 NOV 1994 08:49:37 GMT"), 784111777);
    EXPECT_EQ(parse("sUn, 06 nOv 1994 08:49:37 gMt"), 784111777);

    EXPECT_EQ(parse("sunday, 06-nov-94 08:49:37 gmt"), 784111777);
    EXPECT_EQ(parse("SUNDAY, 06-NOV-94 08:49:37 GMT"), 784111777);

    EXPECT_EQ(parse("sun nov  6 08:49:37 1994"), 784111777);
    EXPECT_EQ(parse("SUN NOV  6 08:49:37 1994"), 784111777);

    /// Case folding is ASCII-only and does not turn an unknown token into a known one.
    EXPECT_EQ(parse("sun, 06 foo 1994 08:49:37 gmt"), std::nullopt);
    EXPECT_EQ(parse("sun, 06 nov 1994 08:49:37 utc"), std::nullopt);
}

TEST(ParseHTTPDate, Invalid)
{
    EXPECT_EQ(parse(""), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08:49:37"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08:49:37 GMT "), std::nullopt);
    EXPECT_EQ(parse(" Sun, 06 Nov 1994 08:49:37 GMT"), std::nullopt);

    /// Only `GMT` is a valid time zone in this format.
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08:49:37 UTC"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08:49:37 EST"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08:49:37 +00"), std::nullopt);

    /// Bad separators.
    EXPECT_EQ(parse("Sun  06 Nov 1994 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06-Nov-1994 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08-49-37 GMT"), std::nullopt);

    /// Bad names.
    EXPECT_EQ(parse("Foo, 06 Nov 1994 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Foo 1994 08:49:37 GMT"), std::nullopt);

    /// Non-digits and out-of-range numbers.
    EXPECT_EQ(parse("Sun, ab Nov 1994 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 19x4 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 +8:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 00 Nov 1994 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 32 Nov 1994 08:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 24:49:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08:60:37 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 06 Nov 1994 08:49:61 GMT"), std::nullopt);
}

TEST(ParseHTTPDate, InvalidCalendarDate)
{
    /// A day of month that does not exist in the given month must be rejected, not folded into the next
    /// month. Otherwise a malformed header would be reported as a real modification time.
    EXPECT_EQ(parse("Mon, 29 Feb 2021 00:00:00 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 31 Apr 2026 00:00:00 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 30 Feb 2026 00:00:00 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 31 Jun 2026 00:00:00 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 31 Sep 2026 00:00:00 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun, 31 Nov 2026 00:00:00 GMT"), std::nullopt);

    /// The century of a two-digit year decides whether 29 February exists.
    EXPECT_EQ(parse("Sunday, 29-Feb-00 00:00:00 GMT"), 951782400);
    EXPECT_EQ(tryParseHTTPDate("Sunday, 29-Feb-00 00:00:00 GMT", -2208988800), std::nullopt);

    /// The obsolete forms are validated too.
    EXPECT_EQ(parse("Monday, 29-Feb-21 00:00:00 GMT"), std::nullopt);
    EXPECT_EQ(parse("Sun Apr 31 00:00:00 2026"), std::nullopt);

    /// The last day of every month is still accepted.
    EXPECT_EQ(parse("Sun, 31 Jan 2026 00:00:00 GMT"), 1769817600);
    EXPECT_EQ(parse("Sat, 28 Feb 2026 00:00:00 GMT"), 1772236800);
    EXPECT_EQ(parse("Sat, 29 Feb 2020 00:00:00 GMT"), 1582934400);
    EXPECT_EQ(parse("Thu, 30 Apr 2026 00:00:00 GMT"), 1777507200);
}

TEST(ParseHTTPDate, LeapSecond)
{
    /// A leap second is representable in all three forms and folds into the next minute.
    EXPECT_EQ(parse("Sun, 31 Dec 2016 23:59:60 GMT"), 1483228800);
    EXPECT_EQ(parse("Saturday, 31-Dec-16 23:59:60 GMT"), 1483228800);
    EXPECT_EQ(parse("Sat Dec 31 23:59:60 2016"), 1483228800);
}
