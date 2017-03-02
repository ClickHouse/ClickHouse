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

#include "time_zone_libc.h"

#include <chrono>
#include <cstdint>
#include <ctime>

// Define OFFSET(tm) and ABBR(tm) for your platform to return the UTC
// offset and zone abbreviation after a call to localtime_r().
#if defined(linux)
# if defined(__USE_BSD) || defined(__USE_MISC)
#  define OFFSET(tm) ((tm).tm_gmtoff)
#  define ABBR(tm)   ((tm).tm_zone)
# else
#  define OFFSET(tm) ((tm).__tm_gmtoff)
#  define ABBR(tm)   ((tm).__tm_zone)
# endif
#elif defined(__APPLE__)
# define OFFSET(tm) ((tm).tm_gmtoff)
# define ABBR(tm)   ((tm).tm_zone)
#elif defined(__sun)
# define OFFSET(tm) ((tm).tm_isdst > 0 ? altzone : timezone)
# define ABBR(tm)   (tzname[(tm).tm_isdst > 0])
#elif defined(_WIN32) || defined(_WIN64)
static long get_timezone() {
  long seconds;
  _get_timezone(&seconds);
  return seconds;
}
static std::string get_tzname(int index) {
  char time_zone_name[32] = {0};
  std::size_t size_in_bytes = sizeof time_zone_name;
  _get_tzname(&size_in_bytes, time_zone_name, size_in_bytes, index);
  return time_zone_name;
}
# define OFFSET(tm) (get_timezone() + ((tm).tm_isdst > 0 ? 60 * 60 : 0))
# define ABBR(tm)   (get_tzname((tm).tm_isdst > 0))
#else
# define OFFSET(tm) (timezone + ((tm).tm_isdst > 0 ? 60 * 60 : 0))
# define ABBR(tm)   (tzname[(tm).tm_isdst > 0])
#endif

namespace cctz {

TimeZoneLibC::TimeZoneLibC(const std::string& name) {
  local_ = (name == "localtime");
  if (!local_) {
    // TODO: Support "UTC-05:00", for example.
    offset_ = 0;
    abbr_ = "UTC";
  }
}

time_zone::absolute_lookup TimeZoneLibC::BreakTime(
    const time_point<sys_seconds>& tp) const {
  time_zone::absolute_lookup al;
  std::time_t t = ToUnixSeconds(tp);
  std::tm tm;
  if (local_) {
#if defined(_WIN32) || defined(_WIN64)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    al.offset = OFFSET(tm);
    al.abbr = ABBR(tm);
  } else {
#if defined(_WIN32) || defined(_WIN64)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    al.offset = offset_;
    al.abbr = abbr_;
  }
  // TODO: Eliminate redundant normalization.
  al.cs = civil_second(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                       tm.tm_hour, tm.tm_min, tm.tm_sec);
  al.is_dst = tm.tm_isdst > 0;
  return al;
}

time_zone::civil_lookup TimeZoneLibC::MakeTime(const civil_second& cs) const {
  time_zone::civil_lookup cl;
  std::time_t t;
  if (local_) {
    // Does not handle SKIPPED/AMBIGUOUS or huge years.
    std::tm tm;
    tm.tm_year = static_cast<int>(cs.year() - 1900);
    tm.tm_mon = cs.month() - 1;
    tm.tm_mday = cs.day();
    tm.tm_hour = cs.hour();
    tm.tm_min = cs.minute();
    tm.tm_sec = cs.second();
    tm.tm_isdst = -1;
    t = std::mktime(&tm);
  } else {
    t = cs - civil_second();
  }
  cl.kind = time_zone::civil_lookup::UNIQUE;
  cl.pre = cl.trans = cl.post = FromUnixSeconds(t);
  return cl;
}

}  // namespace cctz
