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

// A command-line tool for exercising the CCTZ library.

#include <getopt.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "civil_time.h"
#include "time_zone.h"

// Pulls in the aliases from cctz for brevity.
template <typename D>
using time_point = cctz::time_point<D>;
using sys_seconds = cctz::sys_seconds;

// parse() specifiers for command-line time arguments.
const char* const kFormats[] = {
  "%Y   %m   %d   %H   %M   %E*S",
  "%Y - %m - %d T %H : %M : %E*S",
  "%Y - %m - %d %H : %M : %E*S",
  "%Y - %m - %d T %H : %M",
  "%Y - %m - %d %H : %M",
  "%Y - %m - %d",
  "%a %b %d %H : %M : %E*S %Z %Y",
  "%a %e %b %Y %H : %M : %E*S",
  "%a %b %e %Y %H : %M : %E*S",
  "%e %b %Y %H : %M : %E*S",
  "%b %e %Y %H : %M : %E*S",
  "%a %e %b %Y %H : %M",
  "%a %b %e %Y %H : %M",
  "%e %b %Y %H : %M",
  "%b %e %Y %H : %M",
  "%a %e %b %Y",
  "%a %b %e %Y",
  "%e %b %Y",
  "%b %e %Y",
  nullptr
};

bool ParseTimeSpec(const std::string& args, cctz::time_zone zone,
                   time_point<sys_seconds>* when) {
  for (const char* const* fmt = kFormats; *fmt != NULL; ++fmt) {
    const std::string format = std::string(*fmt) + " %Ez";
    time_point<sys_seconds> tp;
    if (cctz::parse(format, args, zone, &tp)) {
      *when = tp;
      return true;
    }
  }
  return false;
}

bool ParseBreakdownSpec(const std::string& args, cctz::civil_second* when) {
  const cctz::time_zone utc = cctz::utc_time_zone();
  for (const char* const* fmt = kFormats; *fmt != NULL; ++fmt) {
    time_point<sys_seconds> tp;
    if (cctz::parse(*fmt, args, utc, &tp)) {
      *when = cctz::convert(tp, utc);
      return true;
    }
  }
  return false;
}

// The FormatTime() specifier for output.
const char* const kFormat = "%Y-%m-%d %H:%M:%S %Ez (%Z)";

const char* WeekDayName(cctz::weekday wd) {
  switch (wd) {
    case cctz::weekday::monday: return "Mon";
    case cctz::weekday::tuesday: return "Tue";
    case cctz::weekday::wednesday: return "Wed";
    case cctz::weekday::thursday: return "Thu";
    case cctz::weekday::friday: return "Fri";
    case cctz::weekday::saturday: return "Sat";
    case cctz::weekday::sunday: return "Sun";
  }
  return "XXX";
}

std::string FormatTimeInZone(time_point<sys_seconds> when,
                             cctz::time_zone zone) {
  std::ostringstream oss;
  oss << std::setw(33) << std::left << cctz::format(kFormat, when, zone);
  cctz::time_zone::absolute_lookup al = zone.lookup(when);
  cctz::civil_day cd(al.cs);
  oss << " [wd=" << WeekDayName(cctz::get_weekday(cd))
      << " yd=" << std::setw(3) << std::setfill('0')
      << std::right << cctz::get_yearday(cd)
      << " dst=" << (al.is_dst ? 'T' : 'F')
      << " off=" << std::showpos << al.offset << std::noshowpos << "]";
  return oss.str();
}

void InstantInfo(const std::string& label, time_point<sys_seconds> when,
                 cctz::time_zone zone) {
  const cctz::time_zone utc = cctz::utc_time_zone();  // might == zone
  const std::string time_label = "time_t";
  const std::string utc_label = "UTC";
  const std::string zone_label = "in-tz";  // perhaps zone.name()?
  std::size_t width =
      2 + std::max(std::max(time_label.size(), utc_label.size()),
                   zone_label.size());
  std::cout << label << " {\n";
  std::cout << std::setw(width) << std::right << time_label << ": ";
  std::cout << std::setw(10) << format("%s", when, utc);
  std::cout << "\n";
  std::cout << std::setw(width) << std::right << utc_label << ": ";
  std::cout << FormatTimeInZone(when, utc) << "\n";
  std::cout << std::setw(width) << std::right << zone_label << ": ";
  std::cout << FormatTimeInZone(when, zone) << "\n";
  std::cout << "}\n";
}

// Report everything we know about a cctz::civil_second (YMDHMS).
int BreakdownInfo(const cctz::civil_second& cs, cctz::time_zone zone) {
  std::cout << "tz: " << zone.name() << "\n";
  cctz::time_zone::civil_lookup cl = zone.lookup(cs);
  switch (cl.kind) {
    case cctz::time_zone::civil_lookup::UNIQUE: {
      std::cout << "kind: UNIQUE\n";
      InstantInfo("when", cl.pre, zone);
      break;
    }
    case cctz::time_zone::civil_lookup::SKIPPED: {
      std::cout << "kind: SKIPPED\n";
      InstantInfo("post", cl.post, zone);  // might == trans-1
      InstantInfo("trans-1", cl.trans - std::chrono::seconds(1), zone);
      InstantInfo("trans", cl.trans, zone);
      InstantInfo("pre", cl.pre, zone);  // might == trans
      break;
    }
    case cctz::time_zone::civil_lookup::REPEATED: {
      std::cout << "kind: REPEATED\n";
      InstantInfo("pre", cl.pre, zone);  // might == trans-1
      InstantInfo("trans-1", cl.trans - std::chrono::seconds(1), zone);
      InstantInfo("trans", cl.trans, zone);
      InstantInfo("post", cl.post, zone);  // might == trans
      break;
    }
  }
  return 0;
}

// Report everything we know about a time_point<sys_seconds>.
int TimeInfo(time_point<sys_seconds> when, cctz::time_zone zone) {
  std::cout << "tz: " << zone.name() << "\n";
  std::cout << "kind: UNIQUE\n";
  InstantInfo("when", when, zone);
  return 0;
}

const char* Basename(const char* p) {
  if (const char* b = strrchr(p, '/')) return ++b;
  return p;
}

// std::regex doesn't work before gcc 4.9.
bool LooksLikeNegOffset(const char* s) {
  if (s[0] == '-' && std::isdigit(s[1]) && std::isdigit(s[2])) {
    int i = (s[3] == ':') ? 4 : 3;
    if (std::isdigit(s[i]) && std::isdigit(s[i + 1])) {
      return s[i + 2] == '\0';
    }
  }
  return false;
}

int main(int argc, char** argv) {
  const std::string prog = argv[0] ? Basename(argv[0]) : "time_tool";

  // Escape arguments that look like negative offsets so that they
  // don't look like flags.
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--") == 0) break;
    if (LooksLikeNegOffset(argv[i])) {
      char* buf = new char[strlen(argv[i] + 2)];
      buf[0] = ' ';  // will later be ignorned
      strcpy(buf + 1, argv[i]);
      argv[i] = buf;
    }
  }

  // Determine the time zone.
  cctz::time_zone zone = cctz::local_time_zone();
  for (;;) {
    static option opts[] = {
        {"tz", required_argument, nullptr, 'z'},
        {nullptr, 0, nullptr, 0},
    };
    int c = getopt_long(argc, argv, "z:", opts, nullptr);
    if (c == -1) break;
    switch (c) {
      case 'z':
        if (!cctz::load_time_zone(optarg, &zone)) {
          std::cerr << optarg << ": Unrecognized time zone\n";
          return 1;
        }
        break;
      default:
        std::cerr << "Usage: " << prog << " [--tz=<zone>] [<time-spec>]\n";
        return 1;
    }
  }

  // Determine the time point.
  time_point<sys_seconds> tp = std::chrono::time_point_cast<sys_seconds>(
      std::chrono::system_clock::now());
  std::string args;
  for (int i = optind; i < argc; ++i) {
    if (i != optind) args += " ";
    args += argv[i];
  }
  std::replace(args.begin(), args.end(), ',', ' ');
  std::replace(args.begin(), args.end(), '/', '-');
  bool have_time = ParseTimeSpec(args, zone, &tp);
  if (!have_time && !args.empty()) {
    std::string spec = args.substr((args[0] == '@') ? 1 : 0);
    if ((spec.size() > 0 && std::isdigit(spec[0])) ||
        (spec.size() > 1 && spec[0] == '-' && std::isdigit(spec[1]))) {
      std::size_t end;
      const time_t t = std::stoll(spec, &end);
      if (end == spec.size()) {
        tp = std::chrono::time_point_cast<cctz::sys_seconds>(
                 std::chrono::system_clock::from_time_t(0)) +
             sys_seconds(t);
        have_time = true;
      }
    }
  }
  cctz::civil_second when = cctz::convert(tp, zone);
  bool have_break_down = !have_time && ParseBreakdownSpec(args, &when);

  // Show results.
  if (have_break_down) return BreakdownInfo(when, zone);
  if (have_time || args.empty()) return TimeInfo(tp, zone);

  std::cerr << args << ": Malformed time spec\n";
  return 1;
}
