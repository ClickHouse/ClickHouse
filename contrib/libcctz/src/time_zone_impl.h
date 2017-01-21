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

#ifndef CCTZ_TIME_ZONE_IMPL_H_
#define CCTZ_TIME_ZONE_IMPL_H_

#include <memory>
#include <string>

#include "time_zone.h"
#include "time_zone_info.h"

namespace cctz {

// time_zone::Impl is the internal object referenced by a cctz::time_zone.
class time_zone::Impl {
 public:
  // The UTC time zone. Also used for other time zones that fail to load.
  static time_zone UTC();

  // Load a named time zone. Returns false if the name is invalid, or if
  // some other kind of error occurs. Note that loading "UTC" never fails.
  static bool LoadTimeZone(const std::string& name, time_zone* tz);

  // Dereferences the time_zone to obtain its Impl.
  static const time_zone::Impl& get(const time_zone& tz);

  // The primary key is the time-zone ID (e.g., "America/New_York").
  const std::string& name() const { return name_; }

  // Breaks a time_point down to civil-time components in this time zone.
  time_zone::absolute_lookup BreakTime(
      const time_point<sys_seconds>& tp) const {
    return zone_->BreakTime(tp);
  }

  // Converts the civil-time components in this time zone into a time_point.
  // That is, the opposite of BreakTime(). The requested civil time may be
  // ambiguous or illegal due to a change of UTC offset.
  time_zone::civil_lookup MakeTime(const civil_second& cs) const {
    return zone_->MakeTime(cs);
  }

 private:
  explicit Impl(const std::string& name);
  static const Impl* UTCImpl();

  const std::string name_;
  std::unique_ptr<TimeZoneIf> zone_;
};

}  // namespace cctz

#endif  // CCTZ_TIME_ZONE_IMPL_H_
