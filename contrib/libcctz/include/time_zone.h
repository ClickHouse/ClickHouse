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

// A library for translating between absolute times (represented by
// std::chrono::time_points of the std::chrono::system_clock) and civil
// times (represented by cctz::civil_second) using the rules defined by
// a time zone (cctz::time_zone).

#ifndef CCTZ_TIME_ZONE_H_
#define CCTZ_TIME_ZONE_H_

#include <chrono>
#include <cstdint>
#include <string>

#include "civil_time.h"

namespace cctz {

// Convenience aliases. Not intended as public API points.
template <typename D>
using time_point = std::chrono::time_point<std::chrono::system_clock, D>;
using sys_seconds = std::chrono::duration<std::int_fast64_t>;

// cctz::time_zone is an opaque, small, value-type class representing a
// geo-political region within which particular rules are used for mapping
// between absolute and civil times. Time zones are named using the TZ
// identifiers from the IANA Time Zone Database, such as "America/Los_Angeles"
// or "Australia/Sydney". Time zones are created from factory functions such
// as load_time_zone(). Note: strings like "PST" and "EDT" are not valid TZ
// identifiers.
//
// Example:
//   cctz::time_zone utc = cctz::utc_time_zone();
//   cctz::time_zone loc = cctz::local_time_zone();
//   cctz::time_zone lax;
//   if (!cctz::load_time_zone("America/Los_Angeles", &lax)) { ... }
//
// See also:
// - http://www.iana.org/time-zones
// - http://en.wikipedia.org/wiki/Zoneinfo
class time_zone {
 public:
  time_zone() = default;  // Equivalent to UTC
  time_zone(const time_zone&) = default;
  time_zone& operator=(const time_zone&) = default;

  std::string name() const;

  // An absolute_lookup represents the civil time (cctz::civil_second) within
  // this time_zone at the given absolute time (time_point). There are
  // additionally a few other fields that may be useful when working with
  // older APIs, such as std::tm.
  //
  // Example:
  //   const cctz::time_zone tz = ...
  //   const auto tp = std::chrono::system_clock::now();
  //   const cctz::time_zone::absolute_lookup al = tz.lookup(tp);
  struct absolute_lookup {
    civil_second cs;
    // Note: The following fields exist for backward compatibility with older
    // APIs. Accessing these fields directly is a sign of imprudent logic in
    // the calling code. Modern time-related code should only access this data
    // indirectly by way of cctz::format().
    int offset;        // civil seconds east of UTC
    bool is_dst;       // is offset non-standard?
    std::string abbr;  // time-zone abbreviation (e.g., "PST")
  };
  absolute_lookup lookup(const time_point<sys_seconds>& tp) const;
  template <typename D>
  absolute_lookup lookup(const time_point<D>& tp) const {
    return lookup(std::chrono::time_point_cast<sys_seconds>(tp));
  }

  // A civil_lookup represents the absolute time(s) (time_point) that
  // correspond to the given civil time (cctz::civil_second) within this
  // time_zone. Usually the given civil time represents a unique instant in
  // time, in which case the conversion is unambiguous and correct. However,
  // within this time zone, the given civil time may be skipped (e.g., during
  // a positive UTC offset shift), or repeated (e.g., during a negative UTC
  // offset shift). To account for these possibilities, civil_lookup is richer
  // than just a single output time_point.
  //
  // In all cases the civil_lookup::kind enum will indicate the nature of the
  // given civil-time argument, and the pre, trans, and post, members will
  // give the absolute time answers using the pre-transition offset, the
  // transition point itself, and the post-transition offset, respectively
  // (these are all equal if kind == UNIQUE).
  //
  // Example:
  //   cctz::time_zone lax;
  //   if (!cctz::load_time_zone("America/Los_Angeles", &lax)) { ... }
  //
  //   // A unique civil time.
  //   auto jan01 = lax.lookup(cctz::civil_second(2011, 1, 1, 0, 0, 0));
  //   // jan01.kind == cctz::time_zone::civil_lookup::UNIQUE
  //   // jan01.pre    is 2011/01/01 00:00:00 -0800
  //   // jan01.trans  is 2011/01/01 00:00:00 -0800
  //   // jan01.post   is 2011/01/01 00:00:00 -0800
  //
  //   // A Spring DST transition, when there is a gap in civil time.
  //   auto mar13 = lax.lookup(cctz::civil_second(2011, 3, 13, 2, 15, 0));
  //   // mar13.kind == cctz::time_zone::civil_lookup::SKIPPED
  //   // mar13.pre   is 2011/03/13 03:15:00 -0700
  //   // mar13.trans is 2011/03/13 03:00:00 -0700
  //   // mar13.post  is 2011/03/13 01:15:00 -0800
  //
  //   // A Fall DST transition, when civil times are repeated.
  //   auto nov06 = lax.lookup(cctz::civil_second(2011, 11, 6, 1, 15, 0));
  //   // nov06.kind == cctz::time_zone::civil_lookup::REPEATED
  //   // nov06.pre   is 2011/11/06 01:15:00 -0700
  //   // nov06.trans is 2011/11/06 01:00:00 -0800
  //   // nov06.post  is 2011/11/06 01:15:00 -0800
  struct civil_lookup {
    enum civil_kind {
      UNIQUE,    // the civil time was singular (pre == trans == post)
      SKIPPED,   // the civil time did not exist
      REPEATED,  // the civil time was ambiguous
    } kind;
    time_point<sys_seconds> pre;    // Uses the pre-transition offset
    time_point<sys_seconds> trans;  // Instant of civil-offset change
    time_point<sys_seconds> post;   // Uses the post-transition offset
  };
  civil_lookup lookup(const civil_second& cs) const;

  class Impl;

 private:
  explicit time_zone(const Impl* impl) : impl_(impl) {}
  const Impl* impl_ = nullptr;
};

// Relational operators.
bool operator==(time_zone lhs, time_zone rhs);
inline bool operator!=(time_zone lhs, time_zone rhs) { return !(lhs == rhs); }

// Loads the named time zone. May perform I/O on the initial load.
// If the name is invalid, or some other kind of error occurs, returns
// false and "*tz" is set to the UTC time zone.
bool load_time_zone(const std::string& name, time_zone* tz);

// Returns a time_zone representing UTC. Cannot fail.
time_zone utc_time_zone();

// Returns a time zone representing the local time zone. Falls back to UTC.
time_zone local_time_zone();

// Returns the civil time (cctz::civil_second) within the given time zone at
// the given absolute time (time_point). Since the additional fields provided
// by the time_zone::absolute_lookup struct should rarely be needed in modern
// code, this convert() function is simpler and should be preferred.
template <typename D>
inline civil_second convert(const time_point<D>& tp, const time_zone& tz) {
  return tz.lookup(tp).cs;
}

// Returns the absolute time (time_point) that corresponds to the given civil
// time within the given time zone. If the civil time is not unique (i.e., if
// it was either repeated or non-existent), then the returned time_point is
// the best estimate that preserves relative order. That is, this function
// guarantees that if cs1 < cs2, then convert(cs1, tz) <= convert(cs2, tz).
inline time_point<sys_seconds> convert(const civil_second& cs,
                                       const time_zone& tz) {
  const time_zone::civil_lookup cl = tz.lookup(cs);
  if (cl.kind == time_zone::civil_lookup::SKIPPED) return cl.trans;
  return cl.pre;
}

namespace detail {
template <typename D>
inline std::pair<time_point<sys_seconds>, D>
split_seconds(const time_point<D>& tp) {
  auto sec = std::chrono::time_point_cast<sys_seconds>(tp);
  auto sub = tp - sec;
  if (sub.count() < 0) {
    sec -= sys_seconds(1);
    sub += sys_seconds(1);
  }
  return {sec, std::chrono::duration_cast<D>(sub)};
}
inline std::pair<time_point<sys_seconds>, sys_seconds>
split_seconds(const time_point<sys_seconds>& tp) {
  return {tp, sys_seconds(0)};
}
using femtoseconds = std::chrono::duration<std::int_fast64_t, std::femto>;
std::string format(const std::string&, const time_point<sys_seconds>&,
                   const femtoseconds&, const time_zone&);
bool parse(const std::string&, const std::string&, const time_zone&,
           time_point<sys_seconds>*, femtoseconds*);
}  // namespace detail

// Formats the given time_point in the given cctz::time_zone according to
// the provided format string. Uses strftime()-like formatting options,
// with the following extensions:
//
//   - %Ez  - RFC3339-compatible numeric time zone (+hh:mm or -hh:mm)
//   - %E#S - Seconds with # digits of fractional precision
//   - %E*S - Seconds with full fractional precision (a literal '*')
//   - %E#f - Fractional seconds with # digits of precision
//   - %E*f - Fractional seconds with full precision (a literal '*')
//   - %E4Y - Four-character years (-999 ... -001, 0000, 0001 ... 9999)
//
// Note that %E0S behaves like %S, and %E0f produces no characters.  In
// contrast %E*f always produces at least one digit, which may be '0'.
//
// Note that %Y produces as many characters as it takes to fully render the
// year. A year outside of [-999:9999] when formatted with %E4Y will produce
// more than four characters, just like %Y.
//
// Tip: Format strings should include the UTC offset (e.g., %z or %Ez) so that
// the resultng string uniquely identifies an absolute time.
//
// Example:
//   cctz::time_zone lax;
//   if (!cctz::load_time_zone("America/Los_Angeles", &lax)) { ... }
//   auto tp = cctz::convert(cctz::civil_second(2013, 1, 2, 3, 4, 5), lax);
//   std::string f = cctz::format("%H:%M:%S", tp, lax);  // "03:04:05"
//   f = cctz::format("%H:%M:%E3S", tp, lax);            // "03:04:05.000"
template <typename D>
inline std::string format(const std::string& fmt, const time_point<D>& tp,
                          const time_zone& tz) {
  const auto p = detail::split_seconds(tp);
  const auto n = std::chrono::duration_cast<detail::femtoseconds>(p.second);
  return detail::format(fmt, p.first, n, tz);
}

// Parses an input string according to the provided format string and
// returns the corresponding time_point. Uses strftime()-like formatting
// options, with the same extensions as cctz::format(), but with the
// exceptions that %E#S is interpreted as %E*S, and %E#f as %E*f.
//
// %Y consumes as many numeric characters as it can, so the matching data
// should always be terminated with a non-numeric. %E4Y always consumes
// exactly four characters, including any sign.
//
// Unspecified fields are taken from the default date and time of ...
//
//   "1970-01-01 00:00:00.0 +0000"
//
// For example, parsing a string of "15:45" (%H:%M) will return a time_point
// that represents "1970-01-01 15:45:00.0 +0000".
//
// Note that parse() returns time instants, so it makes most sense to parse
// fully-specified date/time strings that include a UTC offset (%z or %Ez).
//
// Note also that parse() only heeds the fields year, month, day, hour,
// minute, (fractional) second, and UTC offset. Other fields, like weekday (%a
// or %A), while parsed for syntactic validity, are ignored in the conversion.
//
// Date and time fields that are out-of-range will be treated as errors rather
// than normalizing them like cctz::civil_second() would do. For example, it
// is an error to parse the date "Oct 32, 2013" because 32 is out of range.
//
// A second of ":60" is normalized to ":00" of the following minute with
// fractional seconds discarded. The following table shows how the given
// seconds and subseconds will be parsed:
//
//   "59.x" -> 59.x  // exact
//   "60.x" -> 00.0  // normalized
//   "00.x" -> 00.x  // exact
//
// Errors are indicated by returning false.
//
// Example:
//   const cctz::time_zone tz = ...
//   std::chrono::system_clock::time_point tp;
//   if (cctz::parse("%Y-%m-%d", "2015-10-09", tz, &tp)) {
//     ...
//   }
template <typename D>
inline bool parse(const std::string& fmt, const std::string& input,
                  const time_zone& tz, time_point<D>* tpp) {
  time_point<sys_seconds> sec;
  detail::femtoseconds fs;
  const bool b = detail::parse(fmt, input, tz, &sec, &fs);
  if (b) {
    // TODO: Return false if unrepresentable as a time_point<D>.
    *tpp = std::chrono::time_point_cast<D>(sec);
    *tpp += std::chrono::duration_cast<D>(fs);
  }
  return b;
}

}  // namespace cctz

#endif  // CCTZ_TIME_ZONE_H_
