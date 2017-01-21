// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

#include "time_zone.h"

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using std::chrono::time_point_cast;
using std::chrono::system_clock;
using std::chrono::nanoseconds;
using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::minutes;
using std::chrono::hours;
using testing::HasSubstr;

namespace cctz {

namespace {

// This helper is a macro so that failed expectations show up with the
// correct line numbers.
#define ExpectTime(tp, tz, y, m, d, hh, mm, ss, off, isdst, zone) \
  do {                                                            \
    time_zone::absolute_lookup al = tz.lookup(tp);                \
    EXPECT_EQ(y, al.cs.year());                                   \
    EXPECT_EQ(m, al.cs.month());                                  \
    EXPECT_EQ(d, al.cs.day());                                    \
    EXPECT_EQ(hh, al.cs.hour());                                  \
    EXPECT_EQ(mm, al.cs.minute());                                \
    EXPECT_EQ(ss, al.cs.second());                                \
    EXPECT_EQ(off, al.offset);                                    \
    EXPECT_TRUE(isdst == al.is_dst);                              \
    EXPECT_EQ(zone, al.abbr);                                     \
  } while (0)

const char RFC3339_full[] = "%Y-%m-%dT%H:%M:%E*S%Ez";
const char RFC3339_sec[] =  "%Y-%m-%dT%H:%M:%S%Ez";

const char RFC1123_full[] = "%a, %d %b %Y %H:%M:%S %z";
const char RFC1123_no_wday[] =  "%d %b %Y %H:%M:%S %z";

// A helper that tests the given format specifier by itself, and with leading
// and trailing characters.  For example: TestFormatSpecifier(tp, "%a", "Thu").
template <typename D>
void TestFormatSpecifier(time_point<D> tp, time_zone tz, const std::string& fmt,
                         const std::string& ans) {
  EXPECT_EQ(ans, format(fmt, tp, tz)) << fmt;
  EXPECT_EQ("xxx " + ans, format("xxx " + fmt, tp, tz));
  EXPECT_EQ(ans + " yyy", format(fmt + " yyy", tp, tz));
  EXPECT_EQ("xxx " + ans + " yyy", format("xxx " + fmt + " yyy", tp, tz));
}

}  // namespace

//
// Testing format()
//

TEST(Format, TimePointResolution) {
  const char kFmt[] = "%H:%M:%E*S";
  const time_zone utc = utc_time_zone();
  const time_point<nanoseconds> t0 = system_clock::from_time_t(1420167845) +
                                     milliseconds(123) + microseconds(456) +
                                     nanoseconds(789);
  EXPECT_EQ("03:04:05.123456789",
            format(kFmt, time_point_cast<nanoseconds>(t0), utc));
  EXPECT_EQ("03:04:05.123456",
            format(kFmt, time_point_cast<microseconds>(t0), utc));
  EXPECT_EQ("03:04:05.123",
            format(kFmt, time_point_cast<milliseconds>(t0), utc));
  EXPECT_EQ("03:04:05",
            format(kFmt, time_point_cast<seconds>(t0), utc));
  EXPECT_EQ("03:04:05",
            format(kFmt, time_point_cast<sys_seconds>(t0), utc));
  EXPECT_EQ("03:04:00",
            format(kFmt, time_point_cast<minutes>(t0), utc));
  EXPECT_EQ("03:00:00",
            format(kFmt, time_point_cast<hours>(t0), utc));
}

TEST(Format, TimePointExtendedResolution) {
  const char kFmt[] = "%H:%M:%E*S";
  const time_zone utc = utc_time_zone();
  const time_point<sys_seconds> tp =
      std::chrono::time_point_cast<sys_seconds>(
          std::chrono::system_clock::from_time_t(0)) +
      std::chrono::hours(12) + std::chrono::minutes(34) +
      std::chrono::seconds(56);

  EXPECT_EQ(
      "12:34:56.123456789012345",
      detail::format(kFmt, tp, detail::femtoseconds(123456789012345), utc));
  EXPECT_EQ(
      "12:34:56.012345678901234",
      detail::format(kFmt, tp, detail::femtoseconds(12345678901234), utc));
  EXPECT_EQ(
      "12:34:56.001234567890123",
      detail::format(kFmt, tp, detail::femtoseconds(1234567890123), utc));
  EXPECT_EQ(
      "12:34:56.000123456789012",
      detail::format(kFmt, tp, detail::femtoseconds(123456789012), utc));

  EXPECT_EQ("12:34:56.000000000000123",
            detail::format(kFmt, tp, detail::femtoseconds(123), utc));
  EXPECT_EQ("12:34:56.000000000000012",
            detail::format(kFmt, tp, detail::femtoseconds(12), utc));
  EXPECT_EQ("12:34:56.000000000000001",
            detail::format(kFmt, tp, detail::femtoseconds(1), utc));
}

TEST(Format, Basics) {
  time_zone tz = utc_time_zone();
  time_point<nanoseconds> tp = system_clock::from_time_t(0);

  // Starts with a couple basic edge cases.
  EXPECT_EQ("", format("", tp, tz));
  EXPECT_EQ(" ", format(" ", tp, tz));
  EXPECT_EQ("  ", format("  ", tp, tz));
  EXPECT_EQ("xxx", format("xxx", tp, tz));
  std::string big(128, 'x');
  EXPECT_EQ(big, format(big, tp, tz));
  // Cause the 1024-byte buffer to grow.
  std::string bigger(100000, 'x');
  EXPECT_EQ(bigger, format(bigger, tp, tz));

  tp += hours(13) + minutes(4) + seconds(5);
  tp += milliseconds(6) + microseconds(7) + nanoseconds(8);
  EXPECT_EQ("1970-01-01", format("%Y-%m-%d", tp, tz));
  EXPECT_EQ("13:04:05", format("%H:%M:%S", tp, tz));
  EXPECT_EQ("13:04:05.006", format("%H:%M:%E3S", tp, tz));
  EXPECT_EQ("13:04:05.006007", format("%H:%M:%E6S", tp, tz));
  EXPECT_EQ("13:04:05.006007008", format("%H:%M:%E9S", tp, tz));
}

TEST(Format, PosixConversions) {
  const time_zone tz = utc_time_zone();
  auto tp = system_clock::from_time_t(0);

  TestFormatSpecifier(tp, tz, "%d", "01");
  TestFormatSpecifier(tp, tz, "%e", " 1");  // extension but internal support
  TestFormatSpecifier(tp, tz, "%H", "00");
  TestFormatSpecifier(tp, tz, "%I", "12");
  TestFormatSpecifier(tp, tz, "%j", "001");
  TestFormatSpecifier(tp, tz, "%m", "01");
  TestFormatSpecifier(tp, tz, "%M", "00");
  TestFormatSpecifier(tp, tz, "%S", "00");
  TestFormatSpecifier(tp, tz, "%U", "00");
  TestFormatSpecifier(tp, tz, "%w", "4");  // 4=Thursday
  TestFormatSpecifier(tp, tz, "%W", "00");
  TestFormatSpecifier(tp, tz, "%y", "70");
  TestFormatSpecifier(tp, tz, "%Y", "1970");
  TestFormatSpecifier(tp, tz, "%z", "+0000");
  TestFormatSpecifier(tp, tz, "%Z", "UTC");
  TestFormatSpecifier(tp, tz, "%%", "%");

#if defined(__linux__)
  // SU/C99/TZ extensions
  TestFormatSpecifier(tp, tz, "%C", "19");
  TestFormatSpecifier(tp, tz, "%D", "01/01/70");
  TestFormatSpecifier(tp, tz, "%F", "1970-01-01");
  TestFormatSpecifier(tp, tz, "%g", "70");
  TestFormatSpecifier(tp, tz, "%G", "1970");
  TestFormatSpecifier(tp, tz, "%k", " 0");
  TestFormatSpecifier(tp, tz, "%l", "12");
  TestFormatSpecifier(tp, tz, "%n", "\n");
  TestFormatSpecifier(tp, tz, "%R", "00:00");
  TestFormatSpecifier(tp, tz, "%t", "\t");
  TestFormatSpecifier(tp, tz, "%T", "00:00:00");
  TestFormatSpecifier(tp, tz, "%u", "4");  // 4=Thursday
  TestFormatSpecifier(tp, tz, "%V", "01");
  TestFormatSpecifier(tp, tz, "%s", "0");
#endif
}

TEST(Format, LocaleSpecific) {
  const time_zone tz = utc_time_zone();
  auto tp = system_clock::from_time_t(0);

  TestFormatSpecifier(tp, tz, "%a", "Thu");
  TestFormatSpecifier(tp, tz, "%A", "Thursday");
  TestFormatSpecifier(tp, tz, "%b", "Jan");
  TestFormatSpecifier(tp, tz, "%B", "January");

  // %c should at least produce the numeric year and time-of-day.
  const std::string s = format("%c", tp, utc_time_zone());
  EXPECT_THAT(s, HasSubstr("1970"));
  EXPECT_THAT(s, HasSubstr("00:00:00"));

  TestFormatSpecifier(tp, tz, "%p", "AM");
  TestFormatSpecifier(tp, tz, "%x", "01/01/70");
  TestFormatSpecifier(tp, tz, "%X", "00:00:00");

#if defined(__linux__)
  // SU/C99/TZ extensions
  TestFormatSpecifier(tp, tz, "%h", "Jan");  // Same as %b
  TestFormatSpecifier(tp, tz, "%P", "am");
  TestFormatSpecifier(tp, tz, "%r", "12:00:00 AM");

  // Modified conversion specifiers %E_
  TestFormatSpecifier(tp, tz, "%Ec", "Thu Jan  1 00:00:00 1970");
  TestFormatSpecifier(tp, tz, "%EC", "19");
  TestFormatSpecifier(tp, tz, "%Ex", "01/01/70");
  TestFormatSpecifier(tp, tz, "%EX", "00:00:00");
  TestFormatSpecifier(tp, tz, "%Ey", "70");
  TestFormatSpecifier(tp, tz, "%EY", "1970");

  // Modified conversion specifiers %O_
  TestFormatSpecifier(tp, tz, "%Od", "01");
  TestFormatSpecifier(tp, tz, "%Oe", " 1");
  TestFormatSpecifier(tp, tz, "%OH", "00");
  TestFormatSpecifier(tp, tz, "%OI", "12");
  TestFormatSpecifier(tp, tz, "%Om", "01");
  TestFormatSpecifier(tp, tz, "%OM", "00");
  TestFormatSpecifier(tp, tz, "%OS", "00");
  TestFormatSpecifier(tp, tz, "%Ou", "4");  // 4=Thursday
  TestFormatSpecifier(tp, tz, "%OU", "00");
  TestFormatSpecifier(tp, tz, "%OV", "01");
  TestFormatSpecifier(tp, tz, "%Ow", "4");  // 4=Thursday
  TestFormatSpecifier(tp, tz, "%OW", "00");
  TestFormatSpecifier(tp, tz, "%Oy", "70");
#endif
}

TEST(Format, Escaping) {
  const time_zone tz = utc_time_zone();
  auto tp = system_clock::from_time_t(0);

  TestFormatSpecifier(tp, tz, "%%", "%");
  TestFormatSpecifier(tp, tz, "%%a", "%a");
  TestFormatSpecifier(tp, tz, "%%b", "%b");
  TestFormatSpecifier(tp, tz, "%%Ea", "%Ea");
  TestFormatSpecifier(tp, tz, "%%Es", "%Es");
  TestFormatSpecifier(tp, tz, "%%E3S", "%E3S");
  TestFormatSpecifier(tp, tz, "%%OS", "%OS");
  TestFormatSpecifier(tp, tz, "%%O3S", "%O3S");

  // Multiple levels of escaping.
  TestFormatSpecifier(tp, tz, "%%%Y", "%1970");
  TestFormatSpecifier(tp, tz, "%%%E3S", "%00.000");
  TestFormatSpecifier(tp, tz, "%%%%E3S", "%%E3S");
}

TEST(Format, ExtendedSeconds) {
  const time_zone tz = utc_time_zone();

  // No subseconds.
  time_point<nanoseconds> tp = system_clock::from_time_t(0);
  tp += seconds(5);
  EXPECT_EQ("05", format("%E*S", tp, tz));
  EXPECT_EQ("05", format("%E0S", tp, tz));
  EXPECT_EQ("05.0", format("%E1S", tp, tz));
  EXPECT_EQ("05.00", format("%E2S", tp, tz));
  EXPECT_EQ("05.000", format("%E3S", tp, tz));
  EXPECT_EQ("05.0000", format("%E4S", tp, tz));
  EXPECT_EQ("05.00000", format("%E5S", tp, tz));
  EXPECT_EQ("05.000000", format("%E6S", tp, tz));
  EXPECT_EQ("05.0000000", format("%E7S", tp, tz));
  EXPECT_EQ("05.00000000", format("%E8S", tp, tz));
  EXPECT_EQ("05.000000000", format("%E9S", tp, tz));
  EXPECT_EQ("05.0000000000", format("%E10S", tp, tz));
  EXPECT_EQ("05.00000000000", format("%E11S", tp, tz));
  EXPECT_EQ("05.000000000000", format("%E12S", tp, tz));
  EXPECT_EQ("05.0000000000000", format("%E13S", tp, tz));
  EXPECT_EQ("05.00000000000000", format("%E14S", tp, tz));
  EXPECT_EQ("05.000000000000000", format("%E15S", tp, tz));

  // With subseconds.
  tp += milliseconds(6) + microseconds(7) + nanoseconds(8);
  EXPECT_EQ("05.006007008", format("%E*S", tp, tz));
  EXPECT_EQ("05", format("%E0S", tp, tz));
  EXPECT_EQ("05.0", format("%E1S", tp, tz));
  EXPECT_EQ("05.00", format("%E2S", tp, tz));
  EXPECT_EQ("05.006", format("%E3S", tp, tz));
  EXPECT_EQ("05.0060", format("%E4S", tp, tz));
  EXPECT_EQ("05.00600", format("%E5S", tp, tz));
  EXPECT_EQ("05.006007", format("%E6S", tp, tz));
  EXPECT_EQ("05.0060070", format("%E7S", tp, tz));
  EXPECT_EQ("05.00600700", format("%E8S", tp, tz));
  EXPECT_EQ("05.006007008", format("%E9S", tp, tz));
  EXPECT_EQ("05.0060070080", format("%E10S", tp, tz));
  EXPECT_EQ("05.00600700800", format("%E11S", tp, tz));
  EXPECT_EQ("05.006007008000", format("%E12S", tp, tz));
  EXPECT_EQ("05.0060070080000", format("%E13S", tp, tz));
  EXPECT_EQ("05.00600700800000", format("%E14S", tp, tz));
  EXPECT_EQ("05.006007008000000", format("%E15S", tp, tz));

  // Times before the Unix epoch.
  tp = system_clock::from_time_t(0) + microseconds(-1);
  EXPECT_EQ("1969-12-31 23:59:59.999999",
            format("%Y-%m-%d %H:%M:%E*S", tp, tz));

  // Here is a "%E*S" case we got wrong for a while.  While the first
  // instant below is correctly rendered as "...:07.333304", the second
  // one used to appear as "...:07.33330499999999999".
  tp = system_clock::from_time_t(0) + microseconds(1395024427333304);
  EXPECT_EQ("2014-03-17 02:47:07.333304",
            format("%Y-%m-%d %H:%M:%E*S", tp, tz));
  tp += microseconds(1);
  EXPECT_EQ("2014-03-17 02:47:07.333305",
            format("%Y-%m-%d %H:%M:%E*S", tp, tz));
}

TEST(Format, ExtendedSubeconds) {
  const time_zone tz = utc_time_zone();

  // No subseconds.
  time_point<nanoseconds> tp = system_clock::from_time_t(0);
  tp += seconds(5);
  EXPECT_EQ("0", format("%E*f", tp, tz));
  EXPECT_EQ("", format("%E0f", tp, tz));
  EXPECT_EQ("0", format("%E1f", tp, tz));
  EXPECT_EQ("00", format("%E2f", tp, tz));
  EXPECT_EQ("000", format("%E3f", tp, tz));
  EXPECT_EQ("0000", format("%E4f", tp, tz));
  EXPECT_EQ("00000", format("%E5f", tp, tz));
  EXPECT_EQ("000000", format("%E6f", tp, tz));
  EXPECT_EQ("0000000", format("%E7f", tp, tz));
  EXPECT_EQ("00000000", format("%E8f", tp, tz));
  EXPECT_EQ("000000000", format("%E9f", tp, tz));
  EXPECT_EQ("0000000000", format("%E10f", tp, tz));
  EXPECT_EQ("00000000000", format("%E11f", tp, tz));
  EXPECT_EQ("000000000000", format("%E12f", tp, tz));
  EXPECT_EQ("0000000000000", format("%E13f", tp, tz));
  EXPECT_EQ("00000000000000", format("%E14f", tp, tz));
  EXPECT_EQ("000000000000000", format("%E15f", tp, tz));

  // With subseconds.
  tp += milliseconds(6) + microseconds(7) + nanoseconds(8);
  EXPECT_EQ("006007008", format("%E*f", tp, tz));
  EXPECT_EQ("", format("%E0f", tp, tz));
  EXPECT_EQ("0", format("%E1f", tp, tz));
  EXPECT_EQ("00", format("%E2f", tp, tz));
  EXPECT_EQ("006", format("%E3f", tp, tz));
  EXPECT_EQ("0060", format("%E4f", tp, tz));
  EXPECT_EQ("00600", format("%E5f", tp, tz));
  EXPECT_EQ("006007", format("%E6f", tp, tz));
  EXPECT_EQ("0060070", format("%E7f", tp, tz));
  EXPECT_EQ("00600700", format("%E8f", tp, tz));
  EXPECT_EQ("006007008", format("%E9f", tp, tz));
  EXPECT_EQ("0060070080", format("%E10f", tp, tz));
  EXPECT_EQ("00600700800", format("%E11f", tp, tz));
  EXPECT_EQ("006007008000", format("%E12f", tp, tz));
  EXPECT_EQ("0060070080000", format("%E13f", tp, tz));
  EXPECT_EQ("00600700800000", format("%E14f", tp, tz));
  EXPECT_EQ("006007008000000", format("%E15f", tp, tz));

  // Times before the Unix epoch.
  tp = system_clock::from_time_t(0) + microseconds(-1);
  EXPECT_EQ("1969-12-31 23:59:59.999999",
            format("%Y-%m-%d %H:%M:%S.%E*f", tp, tz));

  // Here is a "%E*S" case we got wrong for a while.  While the first
  // instant below is correctly rendered as "...:07.333304", the second
  // one used to appear as "...:07.33330499999999999".
  tp = system_clock::from_time_t(0) + microseconds(1395024427333304);
  EXPECT_EQ("2014-03-17 02:47:07.333304",
            format("%Y-%m-%d %H:%M:%S.%E*f", tp, tz));
  tp += microseconds(1);
  EXPECT_EQ("2014-03-17 02:47:07.333305",
            format("%Y-%m-%d %H:%M:%S.%E*f", tp, tz));
}

TEST(Format, CompareExtendSecondsVsSubseconds) {
  const time_zone tz = utc_time_zone();

  // This test case illustrates the differences/similarities between:
  //   fmt_A: %E<prec>S
  //   fmt_B: %S.%E<prec>f
  auto fmt_A = [](const std::string& prec) { return "%E" + prec + "S"; };
  auto fmt_B = [](const std::string& prec) { return "%S.%E" + prec + "f"; };

  // No subseconds:
  time_point<nanoseconds> tp = system_clock::from_time_t(0);
  tp += seconds(5);
  // ... %E*S and %S.%E*f are different.
  EXPECT_EQ("05", format(fmt_A("*"), tp, tz));
  EXPECT_EQ("05.0", format(fmt_B("*"), tp, tz));
  // ... %E0S and %S.%E0f are different.
  EXPECT_EQ("05", format(fmt_A("0"), tp, tz));
  EXPECT_EQ("05.", format(fmt_B("0"), tp, tz));
  // ... %E<prec>S and %S.%E<prec>f are the same for prec in [1:15].
  for (int prec = 1; prec <= 15; ++prec) {
    const std::string a = format(fmt_A(std::to_string(prec)), tp, tz);
    const std::string b = format(fmt_B(std::to_string(prec)), tp, tz);
    EXPECT_EQ(a, b) << "prec=" << prec;
  }

  // With subseconds:
  // ... %E*S and %S.%E*f are the same.
  tp += milliseconds(6) + microseconds(7) + nanoseconds(8);
  EXPECT_EQ("05.006007008", format(fmt_A("*"), tp, tz));
  EXPECT_EQ("05.006007008", format(fmt_B("*"), tp, tz));
  // ... %E0S and %S.%E0f are different.
  EXPECT_EQ("05", format(fmt_A("0"), tp, tz));
  EXPECT_EQ("05.", format(fmt_B("0"), tp, tz));
  // ... %E<prec>S and %S.%E<prec>f are the same for prec in [1:15].
  for (int prec = 1; prec <= 15; ++prec) {
    const std::string a = format(fmt_A(std::to_string(prec)), tp, tz);
    const std::string b = format(fmt_B(std::to_string(prec)), tp, tz);
    EXPECT_EQ(a, b) << "prec=" << prec;
  }
}

TEST(Format, ExtendedOffset) {
  auto tp = system_clock::from_time_t(0);

  time_zone tz = utc_time_zone();
  TestFormatSpecifier(tp, tz, "%Ez", "+00:00");

  EXPECT_TRUE(load_time_zone("America/New_York", &tz));
  TestFormatSpecifier(tp, tz, "%Ez", "-05:00");

  EXPECT_TRUE(load_time_zone("America/Los_Angeles", &tz));
  TestFormatSpecifier(tp, tz, "%Ez", "-08:00");

  EXPECT_TRUE(load_time_zone("Australia/Sydney", &tz));
  TestFormatSpecifier(tp, tz, "%Ez", "+10:00");

  EXPECT_TRUE(load_time_zone("Africa/Monrovia", &tz));
  // The true offset is -00:44:30 but %z only gives (truncated) minutes.
  TestFormatSpecifier(tp, tz, "%z", "-0044");
  TestFormatSpecifier(tp, tz, "%Ez", "-00:44");
}

TEST(Format, ExtendedYears) {
  const time_zone utc = utc_time_zone();
  const char e4y_fmt[] = "%E4Y%m%d";  // no separators

  // %E4Y zero-pads the year to produce at least 4 chars, including the sign.
  auto tp = convert(civil_second(-999, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("-9991127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(-99, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("-0991127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(-9, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("-0091127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(-1, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("-0011127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(0, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("00001127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(1, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("00011127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(9, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("00091127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(99, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("00991127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(999, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("09991127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(9999, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("99991127", format(e4y_fmt, tp, utc));

  // When the year is outside [-999:9999], more than 4 chars are produced.
  tp = convert(civil_second(-1000, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("-10001127", format(e4y_fmt, tp, utc));
  tp = convert(civil_second(10000, 11, 27, 0, 0, 0), utc);
  EXPECT_EQ("100001127", format(e4y_fmt, tp, utc));
}

TEST(Format, RFC3339Format) {
  time_zone tz;
  EXPECT_TRUE(load_time_zone("America/Los_Angeles", &tz));

  time_point<nanoseconds> tp =
      convert(civil_second(1977, 6, 28, 9, 8, 7), tz);
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += milliseconds(100);
  EXPECT_EQ("1977-06-28T09:08:07.1-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += milliseconds(20);
  EXPECT_EQ("1977-06-28T09:08:07.12-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += milliseconds(3);
  EXPECT_EQ("1977-06-28T09:08:07.123-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += microseconds(400);
  EXPECT_EQ("1977-06-28T09:08:07.1234-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += microseconds(50);
  EXPECT_EQ("1977-06-28T09:08:07.12345-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += microseconds(6);
  EXPECT_EQ("1977-06-28T09:08:07.123456-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += nanoseconds(700);
  EXPECT_EQ("1977-06-28T09:08:07.1234567-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += nanoseconds(80);
  EXPECT_EQ("1977-06-28T09:08:07.12345678-07:00", format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));

  tp += nanoseconds(9);
  EXPECT_EQ("1977-06-28T09:08:07.123456789-07:00",
            format(RFC3339_full, tp, tz));
  EXPECT_EQ("1977-06-28T09:08:07-07:00", format(RFC3339_sec, tp, tz));
}

TEST(Format, RFC1123Format) {  // locale specific
  time_zone tz;
  EXPECT_TRUE(load_time_zone("America/Los_Angeles", &tz));

  auto tp = convert(civil_second(1977, 6, 28, 9, 8, 7), tz);
  EXPECT_EQ("Tue, 28 Jun 1977 09:08:07 -0700", format(RFC1123_full, tp, tz));
  EXPECT_EQ("28 Jun 1977 09:08:07 -0700", format(RFC1123_no_wday, tp, tz));
}

//
// Testing parse()
//

TEST(Parse, TimePointResolution) {
  const char kFmt[] = "%H:%M:%E*S";
  const time_zone utc = utc_time_zone();

  time_point<nanoseconds> tp_ns;
  EXPECT_TRUE(parse(kFmt, "03:04:05.123456789", utc, &tp_ns));
  EXPECT_EQ("03:04:05.123456789", format(kFmt, tp_ns, utc));
  EXPECT_TRUE(parse(kFmt, "03:04:05.123456", utc, &tp_ns));
  EXPECT_EQ("03:04:05.123456", format(kFmt, tp_ns, utc));

  time_point<microseconds> tp_us;
  EXPECT_TRUE(parse(kFmt, "03:04:05.123456789", utc, &tp_us));
  EXPECT_EQ("03:04:05.123456", format(kFmt, tp_us, utc));
  EXPECT_TRUE(parse(kFmt, "03:04:05.123456", utc, &tp_us));
  EXPECT_EQ("03:04:05.123456", format(kFmt, tp_us, utc));
  EXPECT_TRUE(parse(kFmt, "03:04:05.123", utc, &tp_us));
  EXPECT_EQ("03:04:05.123", format(kFmt, tp_us, utc));

  time_point<milliseconds> tp_ms;
  EXPECT_TRUE(parse(kFmt, "03:04:05.123456", utc, &tp_ms));
  EXPECT_EQ("03:04:05.123", format(kFmt, tp_ms, utc));
  EXPECT_TRUE(parse(kFmt, "03:04:05.123", utc, &tp_ms));
  EXPECT_EQ("03:04:05.123", format(kFmt, tp_ms, utc));
  EXPECT_TRUE(parse(kFmt, "03:04:05", utc, &tp_ms));
  EXPECT_EQ("03:04:05", format(kFmt, tp_ms, utc));

  time_point<seconds> tp_s;
  EXPECT_TRUE(parse(kFmt, "03:04:05.123", utc, &tp_s));
  EXPECT_EQ("03:04:05", format(kFmt, tp_s, utc));
  EXPECT_TRUE(parse(kFmt, "03:04:05", utc, &tp_s));
  EXPECT_EQ("03:04:05", format(kFmt, tp_s, utc));

  time_point<minutes> tp_m;
  EXPECT_TRUE(parse(kFmt, "03:04:05", utc, &tp_m));
  EXPECT_EQ("03:04:00", format(kFmt, tp_m, utc));

  time_point<hours> tp_h;
  EXPECT_TRUE(parse(kFmt, "03:04:05", utc, &tp_h));
  EXPECT_EQ("03:00:00", format(kFmt, tp_h, utc));
}

TEST(Parse, TimePointExtendedResolution) {
  const char kFmt[] = "%H:%M:%E*S";
  const time_zone utc = utc_time_zone();

  time_point<sys_seconds> tp;
  detail::femtoseconds fs;
  EXPECT_TRUE(detail::parse(kFmt, "12:34:56.123456789012345", utc, &tp, &fs));
  EXPECT_EQ("12:34:56.123456789012345", detail::format(kFmt, tp, fs, utc));
  EXPECT_TRUE(detail::parse(kFmt, "12:34:56.012345678901234", utc, &tp, &fs));
  EXPECT_EQ("12:34:56.012345678901234", detail::format(kFmt, tp, fs, utc));
  EXPECT_TRUE(detail::parse(kFmt, "12:34:56.001234567890123", utc, &tp, &fs));
  EXPECT_EQ("12:34:56.001234567890123", detail::format(kFmt, tp, fs, utc));
  EXPECT_TRUE(detail::parse(kFmt, "12:34:56.000000000000123", utc, &tp, &fs));
  EXPECT_EQ("12:34:56.000000000000123", detail::format(kFmt, tp, fs, utc));
  EXPECT_TRUE(detail::parse(kFmt, "12:34:56.000000000000012", utc, &tp, &fs));
  EXPECT_EQ("12:34:56.000000000000012", detail::format(kFmt, tp, fs, utc));
  EXPECT_TRUE(detail::parse(kFmt, "12:34:56.000000000000001", utc, &tp, &fs));
  EXPECT_EQ("12:34:56.000000000000001", detail::format(kFmt, tp, fs, utc));
}

TEST(Parse, Basics) {
  time_zone tz = utc_time_zone();
  time_point<nanoseconds> tp = system_clock::from_time_t(1234567890);

  // Simple edge cases.
  EXPECT_TRUE(parse("", "", tz, &tp));
  EXPECT_EQ(system_clock::from_time_t(0), tp);  // everything defaulted
  EXPECT_TRUE(parse(" ", " ", tz, &tp));
  EXPECT_TRUE(parse("  ", "  ", tz, &tp));
  EXPECT_TRUE(parse("x", "x", tz, &tp));
  EXPECT_TRUE(parse("xxx", "xxx", tz, &tp));

  EXPECT_TRUE(
      parse("%Y-%m-%d %H:%M:%S %z", "2013-06-28 19:08:09 -0800", tz, &tp));
  ExpectTime(tp, tz, 2013, 6, 29, 3, 8, 9, 0, false, "UTC");
}

TEST(Parse, WithTimeZone) {
  time_zone tz;
  EXPECT_TRUE(load_time_zone("America/Los_Angeles", &tz));
  time_point<nanoseconds> tp;

  // We can parse a string without a UTC offset if we supply a timezone.
  EXPECT_TRUE(parse("%Y-%m-%d %H:%M:%S", "2013-06-28 19:08:09", tz, &tp));
  ExpectTime(tp, tz, 2013, 6, 28, 19, 8, 9, -7 * 60 * 60, true, "PDT");

  // But the timezone is ignored when a UTC offset is present.
  EXPECT_TRUE(parse("%Y-%m-%d %H:%M:%S %z", "2013-06-28 19:08:09 +0800",
                    utc_time_zone(), &tp));
  ExpectTime(tp, tz, 2013, 6, 28, 19 - 8 - 7, 8, 9, -7 * 60 * 60, true, "PDT");

  // Check a skipped time (a Spring DST transition).  parse() returns
  // the preferred-offset result, as defined for ConvertDateTime().
  EXPECT_TRUE(parse("%Y-%m-%d %H:%M:%S", "2011-03-13 02:15:00", tz, &tp));
  ExpectTime(tp, tz, 2011, 3, 13, 3, 15, 0, -7 * 60 * 60, true, "PDT");

  // Check a repeated time (a Fall DST transition).  parse() returns
  // the preferred-offset result, as defined for ConvertDateTime().
  EXPECT_TRUE(parse("%Y-%m-%d %H:%M:%S", "2011-11-06 01:15:00", tz, &tp));
  ExpectTime(tp, tz, 2011, 11, 6, 1, 15, 0, -7 * 60 * 60, true, "PDT");
}

TEST(Parse, LeapSecond) {
  time_zone tz;
  EXPECT_TRUE(load_time_zone("America/Los_Angeles", &tz));
  time_point<nanoseconds> tp;

  // ":59" -> ":59"
  EXPECT_TRUE(parse(RFC3339_full, "2013-06-28T07:08:59-08:00", tz, &tp));
  ExpectTime(tp, tz, 2013, 6, 28, 8, 8, 59, -7 * 60 * 60, true, "PDT");

  // ":59.5" -> ":59.5"
  EXPECT_TRUE(parse(RFC3339_full, "2013-06-28T07:08:59.5-08:00", tz, &tp));
  ExpectTime(tp, tz, 2013, 6, 28, 8, 8, 59, -7 * 60 * 60, true, "PDT");

  // ":60" -> ":00"
  EXPECT_TRUE(parse(RFC3339_full, "2013-06-28T07:08:60-08:00", tz, &tp));
  ExpectTime(tp, tz, 2013, 6, 28, 8, 9, 0, -7 * 60 * 60, true, "PDT");

  // ":60.5" -> ":00.0"
  EXPECT_TRUE(parse(RFC3339_full, "2013-06-28T07:08:60.5-08:00", tz, &tp));
  ExpectTime(tp, tz, 2013, 6, 28, 8, 9, 0, -7 * 60 * 60, true, "PDT");

  // ":61" -> error
  EXPECT_FALSE(parse(RFC3339_full, "2013-06-28T07:08:61-08:00", tz, &tp));
}

TEST(Parse, ErrorCases) {
  const time_zone tz = utc_time_zone();
  auto tp = system_clock::from_time_t(0);

  // Illegal trailing data.
  EXPECT_FALSE(parse("%S", "123", tz, &tp));

  // Can't parse an illegal format specifier.
  EXPECT_FALSE(parse("%Q", "x", tz, &tp));

  // Fails because of trailing, unparsed data "blah".
  EXPECT_FALSE(parse("%m-%d", "2-3 blah", tz, &tp));

  // Trailing whitespace is allowed.
  EXPECT_TRUE(parse("%m-%d", "2-3  ", tz, &tp));
  EXPECT_EQ(2, convert(tp, utc_time_zone()).month());
  EXPECT_EQ(3, convert(tp, utc_time_zone()).day());

  // Feb 31 requires normalization.
  EXPECT_FALSE(parse("%m-%d", "2-31", tz, &tp));

  // Check that we cannot have spaces in UTC offsets.
  EXPECT_TRUE(parse("%z", "-0203", tz, &tp));
  EXPECT_FALSE(parse("%z", "- 2 3", tz, &tp));
  EXPECT_TRUE(parse("%Ez", "-02:03", tz, &tp));
  EXPECT_FALSE(parse("%Ez", "- 2: 3", tz, &tp));

  // Check that we reject other malformed UTC offsets.
  EXPECT_FALSE(parse("%Ez", "+-08:00", tz, &tp));
  EXPECT_FALSE(parse("%Ez", "-+08:00", tz, &tp));

  // Check that we do not accept "-0" in fields that allow zero.
  EXPECT_FALSE(parse("%Y", "-0", tz, &tp));
  EXPECT_FALSE(parse("%E4Y", "-0", tz, &tp));
  EXPECT_FALSE(parse("%H", "-0", tz, &tp));
  EXPECT_FALSE(parse("%M", "-0", tz, &tp));
  EXPECT_FALSE(parse("%S", "-0", tz, &tp));
  EXPECT_FALSE(parse("%z", "+-000", tz, &tp));
  EXPECT_FALSE(parse("%Ez", "+-0:00", tz, &tp));
  EXPECT_FALSE(parse("%z", "-00-0", tz, &tp));
  EXPECT_FALSE(parse("%Ez", "-00:-0", tz, &tp));
}

TEST(Parse, PosixConversions) {
  time_zone tz = utc_time_zone();
  auto tp = system_clock::from_time_t(0);
  const auto reset = convert(civil_second(1977, 6, 28, 9, 8, 7), tz);

  tp = reset;
  EXPECT_TRUE(parse("%d", "15", tz, &tp));
  EXPECT_EQ(15, convert(tp, tz).day());

  // %e is an extension, but is supported internally.
  tp = reset;
  EXPECT_TRUE(parse("%e", "15", tz, &tp));
  EXPECT_EQ(15, convert(tp, tz).day());  // Equivalent to %d

  tp = reset;
  EXPECT_TRUE(parse("%H", "17", tz, &tp));
  EXPECT_EQ(17, convert(tp, tz).hour());

  tp = reset;
  EXPECT_TRUE(parse("%I", "5", tz, &tp));
  EXPECT_EQ(5, convert(tp, tz).hour());

  // %j is parsed but ignored.
  EXPECT_TRUE(parse("%j", "32", tz, &tp));

  tp = reset;
  EXPECT_TRUE(parse("%m", "11", tz, &tp));
  EXPECT_EQ(11, convert(tp, tz).month());

  tp = reset;
  EXPECT_TRUE(parse("%M", "33", tz, &tp));
  EXPECT_EQ(33, convert(tp, tz).minute());

  tp = reset;
  EXPECT_TRUE(parse("%S", "55", tz, &tp));
  EXPECT_EQ(55, convert(tp, tz).second());

  // %U is parsed but ignored.
  EXPECT_TRUE(parse("%U", "15", tz, &tp));

  // %w is parsed but ignored.
  EXPECT_TRUE(parse("%w", "2", tz, &tp));

  // %W is parsed but ignored.
  EXPECT_TRUE(parse("%W", "22", tz, &tp));

  tp = reset;
  EXPECT_TRUE(parse("%y", "04", tz, &tp));
  EXPECT_EQ(2004, convert(tp, tz).year());

  tp = reset;
  EXPECT_TRUE(parse("%Y", "2004", tz, &tp));
  EXPECT_EQ(2004, convert(tp, tz).year());

  EXPECT_TRUE(parse("%%", "%", tz, &tp));

#if defined(__linux__)
  // SU/C99/TZ extensions

  tp = reset;
  EXPECT_TRUE(parse("%C", "20", tz, &tp));
  EXPECT_EQ(2000, convert(tp, tz).year());

  tp = reset;
  EXPECT_TRUE(parse("%D", "02/03/04", tz, &tp));
  EXPECT_EQ(2, convert(tp, tz).month());
  EXPECT_EQ(3, convert(tp, tz).day());
  EXPECT_EQ(2004, convert(tp, tz).year());

  EXPECT_TRUE(parse("%n", "\n", tz, &tp));

  tp = reset;
  EXPECT_TRUE(parse("%R", "03:44", tz, &tp));
  EXPECT_EQ(3, convert(tp, tz).hour());
  EXPECT_EQ(44, convert(tp, tz).minute());

  EXPECT_TRUE(parse("%t", "\t\v\f\n\r ", tz, &tp));

  tp = reset;
  EXPECT_TRUE(parse("%T", "03:44:55", tz, &tp));
  EXPECT_EQ(3, convert(tp, tz).hour());
  EXPECT_EQ(44, convert(tp, tz).minute());
  EXPECT_EQ(55, convert(tp, tz).second());

  tp = reset;
  EXPECT_TRUE(parse("%s", "1234567890", tz, &tp));
  EXPECT_EQ(system_clock::from_time_t(1234567890), tp);

  // %s conversion, like %z/%Ez, pays no heed to the optional zone.
  time_zone lax;
  EXPECT_TRUE(load_time_zone("America/Los_Angeles", &lax));
  tp = reset;
  EXPECT_TRUE(parse("%s", "1234567890", lax, &tp));
  EXPECT_EQ(system_clock::from_time_t(1234567890), tp);

  // This is most important when the time has the same YMDhms
  // breakdown in the zone as some other time.  For example, ...
  //  1414917000 in US/Pacific -> Sun Nov 2 01:30:00 2014 (PDT)
  //  1414920600 in US/Pacific -> Sun Nov 2 01:30:00 2014 (PST)
  tp = reset;
  EXPECT_TRUE(parse("%s", "1414917000", lax, &tp));
  EXPECT_EQ(system_clock::from_time_t(1414917000), tp);
  tp = reset;
  EXPECT_TRUE(parse("%s", "1414920600", lax, &tp));
  EXPECT_EQ(system_clock::from_time_t(1414920600), tp);
#endif
}

TEST(Parse, LocaleSpecific) {
  time_zone tz = utc_time_zone();
  auto tp = system_clock::from_time_t(0);
  const auto reset = convert(civil_second(1977, 6, 28, 9, 8, 7), tz);

  // %a is parsed but ignored.
  EXPECT_TRUE(parse("%a", "Mon", tz, &tp));

  // %A is parsed but ignored.
  EXPECT_TRUE(parse("%A", "Monday", tz, &tp));

  tp = reset;
  EXPECT_TRUE(parse("%b", "Feb", tz, &tp));
  EXPECT_EQ(2, convert(tp, tz).month());

  tp = reset;
  EXPECT_TRUE(parse("%B", "February", tz, &tp));
  EXPECT_EQ(2, convert(tp, tz).month());

  // %p is parsed but ignored if it's alone.  But it's used with %I.
  EXPECT_TRUE(parse("%p", "AM", tz, &tp));
  tp = reset;
  EXPECT_TRUE(parse("%I %p", "5 PM", tz, &tp));
  EXPECT_EQ(17, convert(tp, tz).hour());

  tp = reset;
  EXPECT_TRUE(parse("%x", "02/03/04", tz, &tp));
  EXPECT_EQ(2, convert(tp, tz).month());
  EXPECT_EQ(3, convert(tp, tz).day());
  EXPECT_EQ(2004, convert(tp, tz).year());

  tp = reset;
  EXPECT_TRUE(parse("%X", "15:44:55", tz, &tp));
  EXPECT_EQ(15, convert(tp, tz).hour());
  EXPECT_EQ(44, convert(tp, tz).minute());
  EXPECT_EQ(55, convert(tp, tz).second());

#if defined(__linux__)
  // SU/C99/TZ extensions

  tp = reset;
  EXPECT_TRUE(parse("%h", "Feb", tz, &tp));
  EXPECT_EQ(2, convert(tp, tz).month());  // Equivalent to %b

  tp = reset;
  EXPECT_TRUE(parse("%l %p", "5 PM", tz, &tp));
  EXPECT_EQ(17, convert(tp, tz).hour());

  tp = reset;
  EXPECT_TRUE(parse("%r", "03:44:55 PM", tz, &tp));
  EXPECT_EQ(15, convert(tp, tz).hour());
  EXPECT_EQ(44, convert(tp, tz).minute());
  EXPECT_EQ(55, convert(tp, tz).second());

  tp = reset;
  EXPECT_TRUE(parse("%Ec", "Tue Nov 19 05:06:07 2013", tz, &tp));
  EXPECT_EQ(convert(civil_second(2013, 11, 19, 5, 6, 7), tz), tp);

  // Modified conversion specifiers %E_

  tp = reset;
  EXPECT_TRUE(parse("%EC", "20", tz, &tp));
  EXPECT_EQ(2000, convert(tp, tz).year());

  tp = reset;
  EXPECT_TRUE(parse("%Ex", "02/03/04", tz, &tp));
  EXPECT_EQ(2, convert(tp, tz).month());
  EXPECT_EQ(3, convert(tp, tz).day());
  EXPECT_EQ(2004, convert(tp, tz).year());

  tp = reset;
  EXPECT_TRUE(parse("%EX", "15:44:55", tz, &tp));
  EXPECT_EQ(15, convert(tp, tz).hour());
  EXPECT_EQ(44, convert(tp, tz).minute());
  EXPECT_EQ(55, convert(tp, tz).second());

// %Ey, the year offset from %EC, doesn't really make sense alone as there
// is no way to represent it in tm_year (%EC is not simply the century).
// Yet, because we handle each (non-internal) specifier in a separate call
// to strptime(), there is no way to group %EC and %Ey either.  So we just
// skip the %Ey case.
#if 0
  tp = reset;
  EXPECT_TRUE(parse("%Ey", "04", tz, &tp));
  EXPECT_EQ(2004, convert(tp, tz).year());
#endif

  tp = reset;
  EXPECT_TRUE(parse("%EY", "2004", tz, &tp));
  EXPECT_EQ(2004, convert(tp, tz).year());

  // Modified conversion specifiers %O_

  tp = reset;
  EXPECT_TRUE(parse("%Od", "15", tz, &tp));
  EXPECT_EQ(15, convert(tp, tz).day());

  tp = reset;
  EXPECT_TRUE(parse("%Oe", "15", tz, &tp));
  EXPECT_EQ(15, convert(tp, tz).day());  // Equivalent to %d

  tp = reset;
  EXPECT_TRUE(parse("%OH", "17", tz, &tp));
  EXPECT_EQ(17, convert(tp, tz).hour());

  tp = reset;
  EXPECT_TRUE(parse("%OI", "5", tz, &tp));
  EXPECT_EQ(5, convert(tp, tz).hour());

  tp = reset;
  EXPECT_TRUE(parse("%Om", "11", tz, &tp));
  EXPECT_EQ(11, convert(tp, tz).month());

  tp = reset;
  EXPECT_TRUE(parse("%OM", "33", tz, &tp));
  EXPECT_EQ(33, convert(tp, tz).minute());

  tp = reset;
  EXPECT_TRUE(parse("%OS", "55", tz, &tp));
  EXPECT_EQ(55, convert(tp, tz).second());

  // %OU is parsed but ignored.
  EXPECT_TRUE(parse("%OU", "15", tz, &tp));

  // %Ow is parsed but ignored.
  EXPECT_TRUE(parse("%Ow", "2", tz, &tp));

  // %OW is parsed but ignored.
  EXPECT_TRUE(parse("%OW", "22", tz, &tp));

  tp = reset;
  EXPECT_TRUE(parse("%Oy", "04", tz, &tp));
  EXPECT_EQ(2004, convert(tp, tz).year());
#endif
}

TEST(Parse, ExtendedSeconds) {
  const time_zone tz = utc_time_zone();
  const time_point<nanoseconds> unix_epoch = system_clock::from_time_t(0);

  // All %E<prec>S cases are treated the same as %E*S on input.
  auto precisions = {"*", "0", "1",  "2",  "3",  "4",  "5",  "6", "7",
                     "8", "9", "10", "11", "12", "13", "14", "15"};
  for (const std::string& prec : precisions) {
    const std::string fmt = "%E" + prec + "S";
    SCOPED_TRACE(fmt);
    time_point<nanoseconds> tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "5", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05.0", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05.00", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05.6", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5) + milliseconds(600), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05.60", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5) + milliseconds(600), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05.600", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5) + milliseconds(600), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05.67", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5) + milliseconds(670), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05.670", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5) + milliseconds(670), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "05.678", tz, &tp));
    EXPECT_EQ(unix_epoch + seconds(5) + milliseconds(678), tp);
  }

  // Here is a "%E*S" case we got wrong for a while.  The fractional
  // part of the first instant is less than 2^31 and was correctly
  // parsed, while the second (and any subsecond field >=2^31) failed.
  time_point<nanoseconds> tp = unix_epoch;
  EXPECT_TRUE(parse("%E*S", "0.2147483647", tz, &tp));
  EXPECT_EQ(unix_epoch + nanoseconds(214748364), tp);
  tp = unix_epoch;
  EXPECT_TRUE(parse("%E*S", "0.2147483648", tz, &tp));
  EXPECT_EQ(unix_epoch + nanoseconds(214748364), tp);

  // We should also be able to specify long strings of digits far
  // beyond the current resolution and have them convert the same way.
  tp = unix_epoch;
  EXPECT_TRUE(parse(
      "%E*S", "0.214748364801234567890123456789012345678901234567890123456789",
      tz, &tp));
  EXPECT_EQ(unix_epoch + nanoseconds(214748364), tp);
}

TEST(Parse, ExtendedSecondsScan) {
  const time_zone tz = utc_time_zone();
  time_point<nanoseconds> tp;
  for (int ms = 0; ms < 1000; ms += 111) {
    for (int us = 0; us < 1000; us += 27) {
      const int micros = ms * 1000 + us;
      for (int ns = 0; ns < 1000; ns += 9) {
        const auto expected =
            system_clock::from_time_t(0) + nanoseconds(micros * 1000 + ns);
        std::ostringstream oss;
        oss << "0." << std::setfill('0') << std::setw(3);
        oss << ms << std::setw(3) << us << std::setw(3) << ns;
        const std::string input = oss.str();
        EXPECT_TRUE(parse("%E*S", input, tz, &tp));
        EXPECT_EQ(expected, tp) << input;
      }
    }
  }
}

TEST(Parse, ExtendedSubeconds) {
  const time_zone tz = utc_time_zone();
  const time_point<nanoseconds> unix_epoch = system_clock::from_time_t(0);

  // All %E<prec>f cases are treated the same as %E*f on input.
  auto precisions = {"*", "0", "1",  "2",  "3",  "4",  "5",  "6", "7",
                     "8", "9", "10", "11", "12", "13", "14", "15"};
  for (const std::string& prec : precisions) {
    const std::string fmt = "%E" + prec + "f";
    SCOPED_TRACE(fmt);
    time_point<nanoseconds> tp = unix_epoch - seconds(1);
    EXPECT_TRUE(parse(fmt, "", tz, &tp));
    EXPECT_EQ(unix_epoch, tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "6", tz, &tp));
    EXPECT_EQ(unix_epoch + milliseconds(600), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "60", tz, &tp));
    EXPECT_EQ(unix_epoch + milliseconds(600), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "600", tz, &tp));
    EXPECT_EQ(unix_epoch + milliseconds(600), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "67", tz, &tp));
    EXPECT_EQ(unix_epoch + milliseconds(670), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "670", tz, &tp));
    EXPECT_EQ(unix_epoch + milliseconds(670), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "678", tz, &tp));
    EXPECT_EQ(unix_epoch + milliseconds(678), tp);
    tp = unix_epoch;
    EXPECT_TRUE(parse(fmt, "6789", tz, &tp));
    EXPECT_EQ(unix_epoch + milliseconds(678) + microseconds(900), tp);
  }

  // Here is a "%E*f" case we got wrong for a while.  The fractional
  // part of the first instant is less than 2^31 and was correctly
  // parsed, while the second (and any subsecond field >=2^31) failed.
  time_point<nanoseconds> tp = unix_epoch;
  EXPECT_TRUE(parse("%E*f", "2147483647", tz, &tp));
  EXPECT_EQ(unix_epoch + nanoseconds(214748364), tp);
  tp = unix_epoch;
  EXPECT_TRUE(parse("%E*f", "2147483648", tz, &tp));
  EXPECT_EQ(unix_epoch + nanoseconds(214748364), tp);

  // We should also be able to specify long strings of digits far
  // beyond the current resolution and have them convert the same way.
  tp = unix_epoch;
  EXPECT_TRUE(parse(
      "%E*f", "214748364801234567890123456789012345678901234567890123456789",
      tz, &tp));
  EXPECT_EQ(unix_epoch + nanoseconds(214748364), tp);
}

TEST(Parse, ExtendedSubecondsScan) {
  time_point<nanoseconds> tp;
  const time_zone tz = utc_time_zone();
  for (int ms = 0; ms < 1000; ms += 111) {
    for (int us = 0; us < 1000; us += 27) {
      const int micros = ms * 1000 + us;
      for (int ns = 0; ns < 1000; ns += 9) {
        std::ostringstream oss;
        oss << std::setfill('0') << std::setw(3) << ms;
        oss << std::setw(3) << us << std::setw(3) << ns;
        const std::string nanos = oss.str();
        const auto expected =
            system_clock::from_time_t(0) + nanoseconds(micros * 1000 + ns);
        for (int ps = 0; ps < 1000; ps += 250) {
          std::ostringstream oss;
          oss << std::setfill('0') << std::setw(3) << ps;
          const std::string input = nanos + oss.str() + "999";
          EXPECT_TRUE(parse("%E*f", input, tz, &tp));
          EXPECT_EQ(expected + nanoseconds(ps) / 1000, tp) << input;
        }
      }
    }
  }
}

TEST(Parse, ExtendedOffset) {
  const time_zone utc = utc_time_zone();
  time_point<sys_seconds> tp;

  // %z against +-HHMM.
  EXPECT_TRUE(parse("%z", "+0000", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse("%z", "-1234", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 12, 34, 0), utc), tp);
  EXPECT_TRUE(parse("%z", "+1234", utc, &tp));
  EXPECT_EQ(convert(civil_second(1969, 12, 31, 11, 26, 0), utc), tp);
  EXPECT_FALSE(parse("%z", "-123", utc, &tp));

  // %z against +-HH.
  EXPECT_TRUE(parse("%z", "+00", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse("%z", "-12", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 12, 0, 0), utc), tp);
  EXPECT_TRUE(parse("%z", "+12", utc, &tp));
  EXPECT_EQ(convert(civil_second(1969, 12, 31, 12, 0, 0), utc), tp);
  EXPECT_FALSE(parse("%z", "-1", utc, &tp));

  // %Ez against +-HH:MM.
  EXPECT_TRUE(parse("%Ez", "+00:00", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse("%Ez", "-12:34", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 12, 34, 0), utc), tp);
  EXPECT_TRUE(parse("%Ez", "+12:34", utc, &tp));
  EXPECT_EQ(convert(civil_second(1969, 12, 31, 11, 26, 0), utc), tp);
  EXPECT_FALSE(parse("%Ez", "-12:3", utc, &tp));

  // %Ez against +-HHMM.
  EXPECT_TRUE(parse("%Ez", "+0000", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse("%Ez", "-1234", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 12, 34, 0), utc), tp);
  EXPECT_TRUE(parse("%Ez", "+1234", utc, &tp));
  EXPECT_EQ(convert(civil_second(1969, 12, 31, 11, 26, 0), utc), tp);
  EXPECT_FALSE(parse("%Ez", "-123", utc, &tp));

  // %Ez against +-HH.
  EXPECT_TRUE(parse("%Ez", "+00", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse("%Ez", "-12", utc, &tp));
  EXPECT_EQ(convert(civil_second(1970, 1, 1, 12, 0, 0), utc), tp);
  EXPECT_TRUE(parse("%Ez", "+12", utc, &tp));
  EXPECT_EQ(convert(civil_second(1969, 12, 31, 12, 0, 0), utc), tp);
  EXPECT_FALSE(parse("%Ez", "-1", utc, &tp));
}

TEST(Parse, ExtendedYears) {
  const time_zone utc = utc_time_zone();
  const char e4y_fmt[] = "%E4Y%m%d";  // no separators
  time_point<sys_seconds> tp;

  // %E4Y consumes exactly four chars, including any sign.
  EXPECT_TRUE(parse(e4y_fmt, "-9991127", utc, &tp));
  EXPECT_EQ(convert(civil_second(-999, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "-0991127", utc, &tp));
  EXPECT_EQ(convert(civil_second(-99, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "-0091127", utc, &tp));
  EXPECT_EQ(convert(civil_second(-9, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "-0011127", utc, &tp));
  EXPECT_EQ(convert(civil_second(-1, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "00001127", utc, &tp));
  EXPECT_EQ(convert(civil_second(0, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "00011127", utc, &tp));
  EXPECT_EQ(convert(civil_second(1, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "00091127", utc, &tp));
  EXPECT_EQ(convert(civil_second(9, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "00991127", utc, &tp));
  EXPECT_EQ(convert(civil_second(99, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "09991127", utc, &tp));
  EXPECT_EQ(convert(civil_second(999, 11, 27, 0, 0, 0), utc), tp);
  EXPECT_TRUE(parse(e4y_fmt, "99991127", utc, &tp));
  EXPECT_EQ(convert(civil_second(9999, 11, 27, 0, 0, 0), utc), tp);

  // When the year is outside [-999:9999], the parse fails.
  EXPECT_FALSE(parse(e4y_fmt, "-10001127", utc, &tp));
  EXPECT_FALSE(parse(e4y_fmt, "100001127", utc, &tp));
}

TEST(Parse, RFC3339Format) {
  const time_zone tz = utc_time_zone();
  time_point<nanoseconds> tp;
  EXPECT_TRUE(parse(RFC3339_sec, "2014-02-12T20:21:00+00:00", tz, &tp));
  ExpectTime(tp, tz, 2014, 2, 12, 20, 21, 0, 0, false, "UTC");

  // Check that %Ez also accepts "Z" as a synonym for "+00:00".
  time_point<nanoseconds> tp2;
  EXPECT_TRUE(parse(RFC3339_sec, "2014-02-12T20:21:00Z", tz, &tp2));
  EXPECT_EQ(tp, tp2);
}

//
// Roundtrip test for format()/parse().
//

TEST(FormatParse, RoundTrip) {
  time_zone lax;
  EXPECT_TRUE(load_time_zone("America/Los_Angeles", &lax));
  const auto in = convert(civil_second(1977, 6, 28, 9, 8, 7), lax);
  const auto subseconds = nanoseconds(654321);

  // RFC3339, which renders subseconds.
  {
    time_point<nanoseconds> out;
    const std::string s = format(RFC3339_full, in + subseconds, lax);
    EXPECT_TRUE(parse(RFC3339_full, s, lax, &out)) << s;
    EXPECT_EQ(in + subseconds, out);  // RFC3339_full includes %Ez
  }

  // RFC1123, which only does whole seconds.
  {
    time_point<nanoseconds> out;
    const std::string s = format(RFC1123_full, in, lax);
    EXPECT_TRUE(parse(RFC1123_full, s, lax, &out)) << s;
    EXPECT_EQ(in, out);  // RFC1123_full includes %z
  }

  // Even though we don't know what %c will produce, it should roundtrip,
  // but only in the 0-offset timezone.
  {
    time_point<nanoseconds> out;
    time_zone utc = utc_time_zone();
    const std::string s = format("%c", in, utc);
    EXPECT_TRUE(parse("%c", s, utc, &out)) << s;
    EXPECT_EQ(in, out);
  }
}

TEST(FormatParse, RoundTripDistantFuture) {
  const time_zone utc = utc_time_zone();
  const time_point<sys_seconds> in = time_point<sys_seconds>::max();
  const std::string s = format(RFC3339_full, in, utc);
  time_point<sys_seconds> out;
  EXPECT_TRUE(parse(RFC3339_full, s, utc, &out)) << s;
  EXPECT_EQ(in, out);
}

TEST(FormatParse, RoundTripDistantPast) {
  const time_zone utc = utc_time_zone();
  const time_point<sys_seconds> in = time_point<sys_seconds>::min();
  const std::string s = format(RFC3339_full, in, utc);
  time_point<sys_seconds> out;
  EXPECT_TRUE(parse(RFC3339_full, s, utc, &out)) << s;
  EXPECT_EQ(in, out);
}

}  // namespace cctz
