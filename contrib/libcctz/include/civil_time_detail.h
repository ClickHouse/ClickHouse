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

#include <cstdint>
#include <iomanip>
#include <limits>
#include <ostream>
#include <sstream>
#include <type_traits>

// Disable constexpr support unless we are using clang in C++14 mode.
#if __clang__ && __cpp_constexpr >= 201304
#define CONSTEXPR_D constexpr  // data
#define CONSTEXPR_F constexpr  // function
#define CONSTEXPR_M constexpr  // member
#define CONSTEXPR_T constexpr  // template
#else
#define CONSTEXPR_D const
#define CONSTEXPR_F inline
#define CONSTEXPR_M
#define CONSTEXPR_T
#endif

namespace cctz {

// Support years that at least span the range of 64-bit time_t values.
using year_t = std::int_fast64_t;

// Type alias that indicates an argument is not normalized (e.g., the
// constructor parameters and operands/results of addition/subtraction).
using diff_t = std::int_fast64_t;

namespace detail {

// Type aliases that indicate normalized argument values.
using month_t = std::int_fast8_t;   // [1:12]
using day_t = std::int_fast8_t;     // [1:31]
using hour_t = std::int_fast8_t;    // [0:23]
using minute_t = std::int_fast8_t;  // [0:59]
using second_t = std::int_fast8_t;  // [0:59]

// Normalized civil-time fields: Y-M-D HH:MM:SS.
struct fields {
  CONSTEXPR_M fields(year_t year, month_t month, day_t day,
                     hour_t hour, minute_t minute, second_t second)
      : y(year), m(month), d(day), hh(hour), mm(minute), ss(second) {}
  std::int_least64_t y;
  std::int_least8_t m;
  std::int_least8_t d;
  std::int_least8_t hh;
  std::int_least8_t mm;
  std::int_least8_t ss;
};

struct second_tag {};
struct minute_tag : second_tag {};
struct hour_tag : minute_tag {};
struct day_tag : hour_tag {};
struct month_tag : day_tag {};
struct year_tag : month_tag {};

////////////////////////////////////////////////////////////////////////

// Field normalization (without avoidable overflow).

namespace impl {

CONSTEXPR_F bool is_leap_year(year_t y) noexcept {
  return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
}
CONSTEXPR_F int year_index(year_t y, month_t m) noexcept {
  return (((y + (m > 2)) % 400) + 400) % 400;
}
CONSTEXPR_F int days_per_century(year_t y, month_t m) noexcept {
  const int yi = year_index(y, m);
  return 36524 + (yi == 0 || yi > 300);
}
CONSTEXPR_F int days_per_4years(year_t y, month_t m) noexcept {
  const int yi = year_index(y, m);
  return 1460 + (yi == 0 || yi > 300 || (yi - 1) % 100 < 96);
}
CONSTEXPR_F int days_per_year(year_t y, month_t m) noexcept {
  return is_leap_year(y + (m > 2)) ? 366 : 365;
}
CONSTEXPR_F int days_per_month(year_t y, month_t m) noexcept {
  CONSTEXPR_D signed char k_days_per_month[12] = {
      31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31  // non leap year
  };
  return k_days_per_month[m - 1] + (m == 2 && is_leap_year(y));
}

CONSTEXPR_F fields n_day(year_t y, month_t m, diff_t d, diff_t cd,
                         hour_t hh, minute_t mm, second_t ss) noexcept {
  y += (cd / 146097) * 400;
  cd %= 146097;
  if (cd < 0) {
    y -= 400;
    cd += 146097;
  }
  y += (d / 146097) * 400;
  d = d % 146097 + cd;
  if (d <= 0) {
    y -= 400;
    d += 146097;
  } else if (d > 146097) {
    y += 400;
    d -= 146097;
  }
  if (d > 365) {
    for (int n = days_per_century(y, m); d > n; n = days_per_century(y, m)) {
      d -= n;
      y += 100;
    }
    for (int n = days_per_4years(y, m); d > n; n = days_per_4years(y, m)) {
      d -= n;
      y += 4;
    }
    for (int n = days_per_year(y, m); d > n; n = days_per_year(y, m)) {
      d -= n;
      ++y;
    }
  }
  if (d > 28) {
    for (int n = days_per_month(y, m); d > n; n = days_per_month(y, m)) {
      d -= n;
      if (++m > 12) {
        ++y;
        m = 1;
      }
    }
  }
  return fields(y, m, d, hh, mm, ss);
}
CONSTEXPR_F fields n_mon(year_t y, diff_t m, diff_t d, diff_t cd,
                         hour_t hh, minute_t mm, second_t ss) noexcept {
  if (m != 12) {
    y += m / 12;
    m %= 12;
    if (m <= 0) {
      y -= 1;
      m += 12;
    }
  }
  return n_day(y, m, d, cd, hh, mm, ss);
}
CONSTEXPR_F fields n_hour(year_t y, diff_t m, diff_t d, diff_t cd,
                          diff_t hh, minute_t mm, second_t ss) noexcept {
  cd += hh / 24;
  hh %= 24;
  if (hh < 0) {
    cd -= 1;
    hh += 24;
  }
  return n_mon(y, m, d, cd, hh, mm, ss);
}
CONSTEXPR_F fields n_min(year_t y, diff_t m, diff_t d, diff_t hh, diff_t ch,
                         diff_t mm, second_t ss) noexcept {
  ch += mm / 60;
  mm %= 60;
  if (mm < 0) {
    ch -= 1;
    mm += 60;
  }
  return n_hour(y, m, d, hh / 24 + ch / 24, hh % 24 + ch % 24, mm, ss);
}
CONSTEXPR_F fields n_sec(year_t y, diff_t m, diff_t d, diff_t hh, diff_t mm,
                         diff_t ss) noexcept {
  diff_t cm = ss / 60;
  ss %= 60;
  if (ss < 0) {
    cm -= 1;
    ss += 60;
  }
  return n_min(y, m, d, hh, mm / 60 + cm / 60, mm % 60 + cm % 60, ss);
}

}  // namespace impl

////////////////////////////////////////////////////////////////////////

// Increments the indicated (normalized) field by "n".
CONSTEXPR_F fields step(second_tag, fields f, diff_t n) noexcept {
  return impl::n_sec(f.y, f.m, f.d, f.hh, f.mm + n / 60, f.ss + n % 60);
}
CONSTEXPR_F fields step(minute_tag, fields f, diff_t n) noexcept {
  return impl::n_min(f.y, f.m, f.d, f.hh + n / 60, 0, f.mm + n % 60, f.ss);
}
CONSTEXPR_F fields step(hour_tag, fields f, diff_t n) noexcept {
  return impl::n_hour(f.y, f.m, f.d + n / 24, 0, f.hh + n % 24, f.mm, f.ss);
}
CONSTEXPR_F fields step(day_tag, fields f, diff_t n) noexcept {
  return impl::n_day(f.y, f.m, f.d, n, f.hh, f.mm, f.ss);
}
CONSTEXPR_F fields step(month_tag, fields f, diff_t n) noexcept {
  return impl::n_mon(f.y + n / 12, f.m + n % 12, f.d, 0, f.hh, f.mm, f.ss);
}
CONSTEXPR_F fields step(year_tag, fields f, diff_t n) noexcept {
  return fields(f.y + n, f.m, f.d, f.hh, f.mm, f.ss);
}

////////////////////////////////////////////////////////////////////////

namespace impl {

// Returns (v * f + a) but avoiding intermediate overflow when possible.
CONSTEXPR_F diff_t scale_add(diff_t v, diff_t f, diff_t a) noexcept {
  return (v < 0) ? ((v + 1) * f + a) - f : ((v - 1) * f + a) + f;
}

// Map a (normalized) Y/M/D to the number of days before/after 1970-01-01.
// Probably overflows for years outside [-292277022656:292277026595].
CONSTEXPR_F diff_t ymd_ord(year_t y, month_t m, day_t d) noexcept {
  const diff_t eyear = (m <= 2) ? y - 1 : y;
  const diff_t era = (eyear >= 0 ? eyear : eyear - 399) / 400;
  const diff_t yoe = eyear - era * 400;
  const diff_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const diff_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

// Returns the difference in days between two normalized Y-M-D tuples.
// ymd_ord() will encounter integer overflow given extreme year values,
// yet the difference between two such extreme values may actually be
// small, so we take a little care to avoid overflow when possible by
// exploiting the 146097-day cycle.
CONSTEXPR_F diff_t day_difference(year_t y1, month_t m1, day_t d1,
                                  year_t y2, month_t m2, day_t d2) noexcept {
  const diff_t a_c4_off = y1 % 400;
  const diff_t b_c4_off = y2 % 400;
  diff_t c4_diff = (y1 - a_c4_off) - (y2 - b_c4_off);
  diff_t delta = ymd_ord(a_c4_off, m1, d1) - ymd_ord(b_c4_off, m2, d2);
  if (c4_diff > 0 && delta < 0) {
    delta += 2 * 146097;
    c4_diff -= 2 * 400;
  } else if (c4_diff < 0 && delta > 0) {
    delta -= 2 * 146097;
    c4_diff += 2 * 400;
  }
  return (c4_diff / 400 * 146097) + delta;
}

}  // namespace impl

// Returns the difference between fields structs using the indicated unit.
CONSTEXPR_F diff_t difference(year_tag, fields f1, fields f2) noexcept {
  return f1.y - f2.y;
}
CONSTEXPR_F diff_t difference(month_tag, fields f1, fields f2) noexcept {
  return impl::scale_add(difference(year_tag{}, f1, f2), 12, (f1.m - f2.m));
}
CONSTEXPR_F diff_t difference(day_tag, fields f1, fields f2) noexcept {
  return impl::day_difference(f1.y, f1.m, f1.d, f2.y, f2.m, f2.d);
}
CONSTEXPR_F diff_t difference(hour_tag, fields f1, fields f2) noexcept {
  return impl::scale_add(difference(day_tag{}, f1, f2), 24, (f1.hh - f2.hh));
}
CONSTEXPR_F diff_t difference(minute_tag, fields f1, fields f2) noexcept {
  return impl::scale_add(difference(hour_tag{}, f1, f2), 60, (f1.mm - f2.mm));
}
CONSTEXPR_F diff_t difference(second_tag, fields f1, fields f2) noexcept {
  return impl::scale_add(difference(minute_tag{}, f1, f2), 60, f1.ss - f2.ss);
}

////////////////////////////////////////////////////////////////////////

// Aligns the (normalized) fields struct to the indicated field.
CONSTEXPR_F fields align(second_tag, fields f) noexcept {
  return f;
}
CONSTEXPR_F fields align(minute_tag, fields f) noexcept {
  return fields{f.y, f.m, f.d, f.hh, f.mm, 0};
}
CONSTEXPR_F fields align(hour_tag, fields f) noexcept {
  return fields{f.y, f.m, f.d, f.hh, 0, 0};
}
CONSTEXPR_F fields align(day_tag, fields f) noexcept {
  return fields{f.y, f.m, f.d, 0, 0, 0};
}
CONSTEXPR_F fields align(month_tag, fields f) noexcept {
  return fields{f.y, f.m, 1, 0, 0, 0};
}
CONSTEXPR_F fields align(year_tag, fields f) noexcept {
  return fields{f.y, 1, 1, 0, 0, 0};
}

////////////////////////////////////////////////////////////////////////

template <typename T>
class civil_time {
 public:
  explicit CONSTEXPR_M civil_time(year_t y, diff_t m = 1, diff_t d = 1,
                                  diff_t hh = 0, diff_t mm = 0,
                                  diff_t ss = 0) noexcept
      : civil_time(impl::n_sec(y, m, d, hh, mm, ss)) {}

  CONSTEXPR_M civil_time() noexcept : f_{1970, 1, 1, 0, 0, 0} {}
  civil_time(const civil_time&) = default;
  civil_time& operator=(const civil_time&) = default;

  // Conversion between civil times of different alignment. Conversion to
  // a more precise alignment is allowed implicitly (e.g., day -> hour),
  // but conversion where information is discarded must be explicit
  // (e.g., second -> minute).
  template <typename U, typename S>
  using preserves_data =
      typename std::enable_if<std::is_base_of<U, S>::value>::type;
  template <typename U>
  CONSTEXPR_M civil_time(const civil_time<U>& ct,
                         preserves_data<T, U>* = nullptr) noexcept
      : civil_time(ct.f_) {}
  template <typename U>
  explicit CONSTEXPR_M civil_time(const civil_time<U>& ct,
                                  preserves_data<U, T>* = nullptr) noexcept
      : civil_time(ct.f_) {}

  // Factories for the maximum/minimum representable civil_time.
  static civil_time max() {
    const auto max_year = std::numeric_limits<std::int_least64_t>::max();
    return civil_time(max_year, 12, 31, 23, 59, 59);
  }
  static civil_time min() {
    const auto min_year = std::numeric_limits<std::int_least64_t>::min();
    return civil_time(min_year, 1, 1, 0, 0, 0);
  }

  // Field accessors.  Note: All but year() return an int.
  CONSTEXPR_M year_t year() const noexcept { return f_.y; }
  CONSTEXPR_M int month() const noexcept { return f_.m; }
  CONSTEXPR_M int day() const noexcept { return f_.d; }
  CONSTEXPR_M int hour() const noexcept { return f_.hh; }
  CONSTEXPR_M int minute() const noexcept { return f_.mm; }
  CONSTEXPR_M int second() const noexcept { return f_.ss; }

  // Assigning arithmetic.
  CONSTEXPR_M civil_time& operator+=(diff_t n) noexcept {
    f_ = step(T{}, f_, n);
    return *this;
  }
  CONSTEXPR_M civil_time& operator-=(diff_t n) noexcept {
    if (n != std::numeric_limits<diff_t>::min()) {
      f_ = step(T{}, f_, -n);
    } else {
      f_ = step(T{}, step(T{}, f_, -(n + 1)), 1);
    }
    return *this;
  }
  CONSTEXPR_M civil_time& operator++() noexcept {
    return *this += 1;
  }
  CONSTEXPR_M civil_time operator++(int) noexcept {
    const civil_time a = *this;
    ++*this;
    return a;
  }
  CONSTEXPR_M civil_time& operator--() noexcept {
    return *this -= 1;
  }
  CONSTEXPR_M civil_time operator--(int) noexcept {
    const civil_time a = *this;
    --*this;
    return a;
  }

  // Binary arithmetic operators.
  inline friend CONSTEXPR_M civil_time operator+(civil_time a,
                                                 diff_t n) noexcept {
    return a += n;
  }
  inline friend CONSTEXPR_M civil_time operator+(diff_t n,
                                                 civil_time a) noexcept {
    return a += n;
  }
  inline friend CONSTEXPR_M civil_time operator-(civil_time a,
                                                 diff_t n) noexcept {
    return a -= n;
  }
  inline friend CONSTEXPR_M diff_t operator-(const civil_time& lhs,
                                             const civil_time& rhs) noexcept {
    return difference(T{}, lhs.f_, rhs.f_);
  }

 private:
  // All instantiations of this template are allowed to call the following
  // private constructor and access the private fields member.
  template <typename U>
  friend class civil_time;

  // The designated constructor that all others eventually call.
  explicit CONSTEXPR_M civil_time(fields f) noexcept : f_(align(T{}, f)) {}

  fields f_;
};

// Disallows difference between differently aligned types.
// auto n = civil_day(...) - civil_hour(...);  // would be confusing.
template <typename Tag1, typename Tag2>
CONSTEXPR_F diff_t operator-(civil_time<Tag1>, civil_time<Tag2>) = delete;

using civil_year = civil_time<year_tag>;
using civil_month = civil_time<month_tag>;
using civil_day = civil_time<day_tag>;
using civil_hour = civil_time<hour_tag>;
using civil_minute = civil_time<minute_tag>;
using civil_second = civil_time<second_tag>;

////////////////////////////////////////////////////////////////////////

// Relational operators that work with differently aligned objects.
// Always compares all six fields.
template <typename T1, typename T2>
CONSTEXPR_T bool operator<(const civil_time<T1>& lhs,
                           const civil_time<T2>& rhs) noexcept {
  return (lhs.year() < rhs.year() ||
          (lhs.year() == rhs.year() &&
           (lhs.month() < rhs.month() ||
            (lhs.month() == rhs.month() &&
             (lhs.day() < rhs.day() ||
              (lhs.day() == rhs.day() &&
               (lhs.hour() < rhs.hour() ||
                (lhs.hour() == rhs.hour() &&
                 (lhs.minute() < rhs.minute() ||
                  (lhs.minute() == rhs.minute() &&
                   (lhs.second() < rhs.second())))))))))));
}
template <typename T1, typename T2>
CONSTEXPR_T bool operator<=(const civil_time<T1>& lhs,
                            const civil_time<T2>& rhs) noexcept {
  return !(rhs < lhs);
}
template <typename T1, typename T2>
CONSTEXPR_T bool operator>=(const civil_time<T1>& lhs,
                            const civil_time<T2>& rhs) noexcept {
  return !(lhs < rhs);
}
template <typename T1, typename T2>
CONSTEXPR_T bool operator>(const civil_time<T1>& lhs,
                           const civil_time<T2>& rhs) noexcept {
  return rhs < lhs;
}
template <typename T1, typename T2>
CONSTEXPR_T bool operator==(const civil_time<T1>& lhs,
                            const civil_time<T2>& rhs) noexcept {
  return lhs.year() == rhs.year() && lhs.month() == rhs.month() &&
         lhs.day() == rhs.day() && lhs.hour() == rhs.hour() &&
         lhs.minute() == rhs.minute() && lhs.second() == rhs.second();
}
template <typename T1, typename T2>
CONSTEXPR_T bool operator!=(const civil_time<T1>& lhs,
                            const civil_time<T2>& rhs) noexcept {
  return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////

// Output stream operators output a format matching YYYY-MM-DDThh:mm:ss,
// while omitting fields inferior to the type's alignment. For example,
// civil_day is formatted only as YYYY-MM-DD.
inline std::ostream& operator<<(std::ostream& os, const civil_year& y) {
  std::stringstream ss;
  ss << y.year();  // No padding.
  return os << ss.str();
}
inline std::ostream& operator<<(std::ostream& os, const civil_month& m) {
  std::stringstream ss;
  ss << civil_year(m) << '-';
  ss << std::setfill('0') << std::setw(2) << m.month();
  return os << ss.str();
}
inline std::ostream& operator<<(std::ostream& os, const civil_day& d) {
  std::stringstream ss;
  ss << civil_month(d) << '-';
  ss << std::setfill('0') << std::setw(2) << d.day();
  return os << ss.str();
}
inline std::ostream& operator<<(std::ostream& os, const civil_hour& h) {
  std::stringstream ss;
  ss << civil_day(h) << 'T';
  ss << std::setfill('0') << std::setw(2) << h.hour();
  return os << ss.str();
}
inline std::ostream& operator<<(std::ostream& os, const civil_minute& m) {
  std::stringstream ss;
  ss << civil_hour(m) << ':';
  ss << std::setfill('0') << std::setw(2) << m.minute();
  return os << ss.str();
}
inline std::ostream& operator<<(std::ostream& os, const civil_second& s) {
  std::stringstream ss;
  ss << civil_minute(s) << ':';
  ss << std::setfill('0') << std::setw(2) << s.second();
  return os << ss.str();
}

////////////////////////////////////////////////////////////////////////

enum class weekday {
  monday,
  tuesday,
  wednesday,
  thursday,
  friday,
  saturday,
  sunday,
};

inline std::ostream& operator<<(std::ostream& os, weekday wd) {
  switch (wd) {
    case weekday::monday:
      return os << "Monday";
    case weekday::tuesday:
      return os << "Tuesday";
    case weekday::wednesday:
      return os << "Wednesday";
    case weekday::thursday:
      return os << "Thursday";
    case weekday::friday:
      return os << "Friday";
    case weekday::saturday:
      return os << "Saturday";
    case weekday::sunday:
      return os << "Sunday";
  }
}

CONSTEXPR_F weekday get_weekday(const civil_day& cd) noexcept {
  CONSTEXPR_D weekday k_weekday_by_thu_off[] = {
      weekday::thursday,  weekday::friday,  weekday::saturday,
      weekday::sunday,    weekday::monday,  weekday::tuesday,
      weekday::wednesday,
  };
  return k_weekday_by_thu_off[((cd - civil_day()) % 7 + 7) % 7];
}

////////////////////////////////////////////////////////////////////////

CONSTEXPR_F civil_day next_weekday(civil_day cd, weekday wd) noexcept {
  do { cd += 1; } while (get_weekday(cd) != wd);
  return cd;
}

CONSTEXPR_F civil_day prev_weekday(civil_day cd, weekday wd) noexcept {
  do { cd -= 1; } while (get_weekday(cd) != wd);
  return cd;
}

CONSTEXPR_F int get_yearday(const civil_day& cd) noexcept {
  return cd - civil_day(civil_year(cd)) + 1;
}

}  // namespace detail
}  // namespace cctz

#undef CONSTEXPR_T
#undef CONSTEXPR_M
#undef CONSTEXPR_F
#undef CONSTEXPR_D
