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

#ifndef CCTZ_TIME_ZONE_IF_H_
#define CCTZ_TIME_ZONE_IF_H_

#include <cstdint>
#include <memory>
#include <string>

#include "civil_time.h"
#include "time_zone.h"

namespace cctz {

// A simple interface used to hide time-zone complexities from time_zone::Impl.
// Subclasses implement the functions for civil-time conversions in the zone.
class TimeZoneIf {
 public:
  // A factory function for TimeZoneIf implementations.
  static std::unique_ptr<TimeZoneIf> Load(const std::string& name);

  virtual ~TimeZoneIf() {}

  virtual time_zone::absolute_lookup BreakTime(
      const time_point<sys_seconds>& tp) const = 0;
  virtual time_zone::civil_lookup MakeTime(
      const civil_second& cs) const = 0;

 protected:
  TimeZoneIf() {}
};

// Converts tp to a count of seconds since the Unix epoch.
inline std::int_fast64_t ToUnixSeconds(const time_point<sys_seconds>& tp) {
  return (tp - std::chrono::time_point_cast<sys_seconds>(
                   std::chrono::system_clock::from_time_t(0)))
      .count();
}

// Converts a count of seconds since the Unix epoch to a
// time_point<sys_seconds>.
inline time_point<sys_seconds> FromUnixSeconds(std::int_fast64_t t) {
  return std::chrono::time_point_cast<sys_seconds>(
             std::chrono::system_clock::from_time_t(0)) +
         sys_seconds(t);
}

}  // namespace cctz

#endif  // CCTZ_TIME_ZONE_IF_H_
