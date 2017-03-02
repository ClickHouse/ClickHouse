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

#include "time_zone_if.h"
#include "time_zone_info.h"
#include "time_zone_libc.h"

namespace cctz {

std::unique_ptr<TimeZoneIf> TimeZoneIf::Load(const std::string& name) {
  // Support "libc:localtime" and "libc:*" to access the legacy
  // localtime and UTC support respectively from the C library.
  if (name.compare(0, 5, "libc:") == 0) {
    return std::unique_ptr<TimeZoneIf>(new TimeZoneLibC(name.substr(5)));
  }

  // Otherwise use the "zoneinfo" implementation by default.
  std::unique_ptr<TimeZoneInfo> tz(new TimeZoneInfo);
  if (!tz->Load(name)) tz.reset();
  return std::unique_ptr<TimeZoneIf>(tz.release());
}

}  // namespace cctz
