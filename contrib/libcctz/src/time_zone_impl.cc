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

#include "time_zone_impl.h"

#include <mutex>
#include <unordered_map>

namespace cctz {

namespace {

// time_zone::Impls are linked into a map to support fast lookup by name.
using TimeZoneImplByName =
    std::unordered_map<std::string, const time_zone::Impl*>;
TimeZoneImplByName* time_zone_map = nullptr;

// Mutual exclusion for time_zone_map.
std::mutex time_zone_mutex;

}  // namespace

time_zone time_zone::Impl::UTC() {
  return time_zone(UTCImpl());
}

bool time_zone::Impl::LoadTimeZone(const std::string& name, time_zone* tz) {
  // First check for UTC.
  if (name.compare("UTC") == 0) {
    *tz = time_zone(UTCImpl());
    return true;
  }

  // Then check, under a shared lock, whether the time zone has already
  // been loaded. This is the common path. TODO: Move to shared_mutex.
  {
    std::lock_guard<std::mutex> lock(time_zone_mutex);
    if (time_zone_map != nullptr) {
      TimeZoneImplByName::const_iterator itr = time_zone_map->find(name);
      if (itr != time_zone_map->end()) {
        *tz = time_zone(itr->second);
        return itr->second != UTCImpl();
      }
    }
  }

  // Now check again, under an exclusive lock.
  std::lock_guard<std::mutex> lock(time_zone_mutex);
  if (time_zone_map == nullptr) time_zone_map = new TimeZoneImplByName;
  const Impl*& impl = (*time_zone_map)[name];
  bool fallback_utc = false;
  if (impl == nullptr) {
    // The first thread in loads the new time zone.
    Impl* new_impl = new Impl(name);
    new_impl->zone_ = TimeZoneIf::Load(new_impl->name_);
    if (new_impl->zone_ == nullptr) {
      delete new_impl;  // free the nascent Impl
      impl = UTCImpl();  // and fallback to UTC
      fallback_utc = true;
    } else {
      impl = new_impl;  // install new time zone
    }
  }
  *tz = time_zone(impl);
  return !fallback_utc;
}

const time_zone::Impl& time_zone::Impl::get(const time_zone& tz) {
  if (tz.impl_ == nullptr) {
    // Dereferencing an implicit-UTC time_zone is expected to be
    // rare, so we don't mind paying a small synchronization cost.
    return *UTCImpl();
  }
  return *tz.impl_;
}

time_zone::Impl::Impl(const std::string& name) : name_(name) {}

const time_zone::Impl* time_zone::Impl::UTCImpl() {
  static Impl* utc_impl = [] {
    Impl* impl = new Impl("UTC");
    impl->zone_ = TimeZoneIf::Load(impl->name_);  // never fails
    return impl;
  }();
  return utc_impl;
}

}  // namespace cctz
