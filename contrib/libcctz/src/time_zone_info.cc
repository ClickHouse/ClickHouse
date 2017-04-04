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

// This file implements the TimeZoneIf interface using the "zoneinfo"
// data provided by the IANA Time Zone Database (i.e., the only real game
// in town).
//
// TimeZoneInfo represents the history of UTC-offset changes within a time
// zone. Most changes are due to daylight-saving rules, but occasionally
// shifts are made to the time-zone's base offset. The database only attempts
// to be definitive for times since 1970, so be wary of local-time conversions
// before that. Also, rule and zone-boundary changes are made at the whim
// of governments, so the conversion of future times needs to be taken with
// a grain of salt.
//
// For more information see tzfile(5), http://www.iana.org/time-zones, or
// http://en.wikipedia.org/wiki/Zoneinfo.
//
// Note that we assume the proleptic Gregorian calendar and 60-second
// minutes throughout.

#include "time_zone_info.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>

#include "time_zone_posix.h"

namespace cctz {

namespace {

// Convert errnum to a message, using buf[buflen] if necessary.
// buf must be non-null, and buflen non-zero.
char* errmsg(int errnum, char* buf, std::size_t buflen) {
#if defined(_MSC_VER)
  strerror_s(buf, buflen, errnum);
  return buf;
#elif defined(__APPLE__)
  strerror_r(errnum, buf, buflen);
  return buf;
#elif (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && !_GNU_SOURCE
  strerror_r(errnum, buf, buflen);
  return buf;
#else
  return strerror_r(errnum, buf, buflen);
#endif
}

// Wrap the tzfile.h isleap() macro with an inline function, which will
// then have normal argument-passing semantics (i.e., single evaluation).
inline bool IsLeap(cctz::year_t year) { return isleap(year); }

// The day offsets of the beginning of each (1-based) month in non-leap
// and leap years respectively. That is, sigma[1:n]:kDaysPerMonth[][i].
// For example, in a leap year there are 335 days before December.
const std::int_least16_t kMonthOffsets[2][1 + MONSPERYEAR + 1] = {
  {-1, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365},
  {-1, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366},
};

// 400-year chunks always have 146097 days (20871 weeks).
const std::int_least64_t kSecPer400Years = 146097LL * SECSPERDAY;

// The number of seconds in non-leap and leap years respectively.
const std::int_least32_t kSecPerYear[2] = {
  DAYSPERNYEAR * SECSPERDAY,
  DAYSPERLYEAR * SECSPERDAY,
};

// Like kSecPerYear[] but scaled down by a factor of SECSPERDAY.
const std::int_least32_t kDaysPerYear[2] = {DAYSPERNYEAR, DAYSPERLYEAR};

// January 1st at 00:00:00 in the epoch year.
const civil_second unix_epoch(EPOCH_YEAR, 1, 1, 0, 0, 0);

// Single-byte, unsigned numeric values are encoded directly.
inline std::uint_fast8_t Decode8(const char* cp) {
  return static_cast<std::uint_fast8_t>(*cp) & 0xff;
}

// Multi-byte, numeric values are encoded using a MSB first,
// twos-complement representation. These helpers decode, from
// the given address, 4-byte and 8-byte values respectively.
std::int_fast32_t Decode32(const char* cp) {
  std::uint_fast32_t v = 0;
  for (int i = 0; i != (32 / 8); ++i) v = (v << 8) | Decode8(cp++);
  if (v <= INT32_MAX) return static_cast<std::int_fast32_t>(v);
  return static_cast<std::int_fast32_t>(v - INT32_MAX - 1) + INT32_MIN;
}

std::int_fast64_t Decode64(const char* cp) {
  std::uint_fast64_t v = 0;
  for (int i = 0; i != (64 / 8); ++i) v = (v << 8) | Decode8(cp++);
  if (v <= INT64_MAX) return static_cast<std::int_fast64_t>(v);
  return static_cast<std::int_fast64_t>(v - INT64_MAX - 1) + INT64_MIN;
}

// Generate a year-relative offset for a PosixTransition.
std::int_fast64_t TransOffset(bool leap_year, int jan1_weekday,
                              const PosixTransition& pt) {
  std::int_fast64_t days = 0;
  switch (pt.date.fmt) {
    case PosixTransition::J: {
      days = pt.date.j.day;
      if (!leap_year || days < kMonthOffsets[1][TM_MARCH + 1]) days -= 1;
      break;
    }
    case PosixTransition::N: {
      days = pt.date.n.day;
      break;
    }
    case PosixTransition::M: {
      const bool last_week = (pt.date.m.week == 5);
      days = kMonthOffsets[leap_year][pt.date.m.month + last_week];
      const int weekday = (jan1_weekday + days) % DAYSPERWEEK;
      if (last_week) {
        days -=
            (weekday + DAYSPERWEEK - 1 - pt.date.m.weekday) % DAYSPERWEEK + 1;
      } else {
        days += (pt.date.m.weekday + DAYSPERWEEK - weekday) % DAYSPERWEEK;
        days += (pt.date.m.week - 1) * DAYSPERWEEK;
      }
      break;
    }
  }
  return (days * SECSPERDAY) + pt.time.offset;
}

inline time_zone::civil_lookup MakeUnique(std::int_fast64_t unix_time) {
  time_zone::civil_lookup cl;
  cl.pre = cl.trans = cl.post = FromUnixSeconds(unix_time);
  cl.kind = time_zone::civil_lookup::UNIQUE;
  return cl;
}

inline time_zone::civil_lookup MakeSkipped(const Transition& tr,
                                           const DateTime& dt) {
  time_zone::civil_lookup cl;
  cl.pre = FromUnixSeconds(tr.unix_time - 1 + (dt - tr.prev_date_time));
  cl.trans = FromUnixSeconds(tr.unix_time);
  cl.post = FromUnixSeconds(tr.unix_time - (tr.date_time - dt));
  cl.kind = time_zone::civil_lookup::SKIPPED;
  return cl;
}

inline time_zone::civil_lookup MakeRepeated(const Transition& tr,
                                            const DateTime& dt) {
  time_zone::civil_lookup cl;
  cl.pre = FromUnixSeconds(tr.unix_time - 1 - (tr.prev_date_time - dt));
  cl.trans = FromUnixSeconds(tr.unix_time);
  cl.post = FromUnixSeconds(tr.unix_time + (dt - tr.date_time));
  cl.kind = time_zone::civil_lookup::REPEATED;
  return cl;
}

civil_second YearShift(const civil_second& cs, cctz::year_t year_shift) {
  // TODO: How do we do this while avoiding any normalization tests?
  return civil_second(cs.year() + year_shift, cs.month(), cs.day(),
                      cs.hour(), cs.minute(), cs.second());
}

}  // namespace

// Assign from a civil_second, created using a TimeZoneInfo timestamp.
void DateTime::Assign(const civil_second& cs) {
  offset = cs - unix_epoch;
}

// What (no leap-seconds) UTC+seconds zoneinfo would look like.
bool TimeZoneInfo::ResetToBuiltinUTC(std::int_fast32_t seconds) {
  transition_types_.resize(1);
  TransitionType& tt(transition_types_.back());
  tt.utc_offset = seconds;
  tt.is_dst = false;
  tt.abbr_index = 0;

  transitions_.clear();
  transitions_.reserve(2);
  for (const std::int_fast64_t unix_time : {-(1LL << 59), 2147483647LL}) {
    Transition& tr(*transitions_.emplace(transitions_.end()));
    tr.unix_time = unix_time;
    tr.type_index = 0;
    tr.date_time.Assign(LocalTime(tr.unix_time, tt).cs);
    tr.prev_date_time = tr.date_time;
    tr.prev_date_time.offset -= 1;
  }

  default_transition_type_ = 0;
  abbreviations_ = "UTC";  // TODO: Handle non-zero offset.
  abbreviations_.append(1, '\0');  // add NUL
  future_spec_.clear();  // never needed for a fixed-offset zone
  extended_ = false;

  transitions_.shrink_to_fit();
  return true;
}

// Builds the in-memory header using the raw bytes from the file.
void TimeZoneInfo::Header::Build(const tzhead& tzh) {
  timecnt = Decode32(tzh.tzh_timecnt);
  typecnt = Decode32(tzh.tzh_typecnt);
  charcnt = Decode32(tzh.tzh_charcnt);
  leapcnt = Decode32(tzh.tzh_leapcnt);
  ttisstdcnt = Decode32(tzh.tzh_ttisstdcnt);
  ttisgmtcnt = Decode32(tzh.tzh_ttisgmtcnt);
}

// How many bytes of data are associated with this header. The result
// depends upon whether this is a section with 4-byte or 8-byte times.
std::size_t TimeZoneInfo::Header::DataLength(std::size_t time_len) const {
  std::size_t len = 0;
  len += (time_len + 1) * timecnt;  // unix_time + type_index
  len += (4 + 1 + 1) * typecnt;     // utc_offset + is_dst + abbr_index
  len += 1 * charcnt;               // abbreviations
  len += (time_len + 4) * leapcnt;  // leap-time + TAI-UTC
  len += 1 * ttisstdcnt;            // UTC/local indicators
  len += 1 * ttisgmtcnt;            // standard/wall indicators
  return len;
}

// Check that the TransitionType has the expected offset/is_dst/abbreviation.
void TimeZoneInfo::CheckTransition(const std::string& name,
                                   const TransitionType& tt,
                                   std::int_fast32_t offset, bool is_dst,
                                   const std::string& abbr) const {
  if (tt.utc_offset != offset || tt.is_dst != is_dst ||
      &abbreviations_[tt.abbr_index] != abbr) {
    std::clog << name << ": Transition"
              << " offset=" << tt.utc_offset << "/"
              << (tt.is_dst ? "DST" : "STD")
              << "/abbr=" << &abbreviations_[tt.abbr_index]
              << " does not match POSIX spec '" << future_spec_ << "'\n";
  }
}

// zic(8) can generate no-op transitions when a zone changes rules at an
// instant when there is actually no discontinuity.  So we check whether
// two transitions have equivalent types (same offset/is_dst/abbr).
bool TimeZoneInfo::EquivTransitions(std::uint_fast8_t tt1_index,
                                    std::uint_fast8_t tt2_index) const {
  if (tt1_index == tt2_index) return true;
  const TransitionType& tt1(transition_types_[tt1_index]);
  const TransitionType& tt2(transition_types_[tt2_index]);
  if (tt1.is_dst != tt2.is_dst) return false;
  if (tt1.utc_offset != tt2.utc_offset) return false;
  if (tt1.abbr_index != tt2.abbr_index) return false;
  return true;
}

// Use the POSIX-TZ-environment-variable-style string to handle times
// in years after the last transition stored in the zoneinfo data.
void TimeZoneInfo::ExtendTransitions(const std::string& name,
                                     const Header& hdr) {
  extended_ = false;
  bool extending = !future_spec_.empty();

  PosixTimeZone posix;
  if (extending && !ParsePosixSpec(future_spec_, &posix)) {
    std::clog << name << ": Failed to parse '" << future_spec_ << "'\n";
    extending = false;
  }

  if (extending && posix.dst_abbr.empty()) {  // std only
    // The future specification should match the last/default transition,
    // and that means that handling the future will fall out naturally.
    std::uint_fast8_t index = default_transition_type_;
    if (hdr.timecnt != 0) index = transitions_[hdr.timecnt - 1].type_index;
    const TransitionType& tt(transition_types_[index]);
    CheckTransition(name, tt, posix.std_offset, false, posix.std_abbr);
    extending = false;
  }

  if (extending && hdr.timecnt < 2) {
    std::clog << name << ": Too few transitions for POSIX spec\n";
    extending = false;
  }

  if (!extending) {
    // Ensure that there is always a transition in the second half of the
    // time line (the BIG_BANG transition is in the first half) so that the
    // signed difference between a DateTime and the DateTime of its previous
    // transition is always representable, without overflow.
    const Transition& last(transitions_.back());
    if (last.unix_time < 0) {
      const std::uint_fast8_t type_index = last.type_index;
      Transition& tr(*transitions_.emplace(transitions_.end()));
      tr.unix_time = 2147483647;  // 2038-01-19T03:14:07+00:00
      tr.type_index = type_index;
    }
    return;  // last transition wins
  }

  // Extend the transitions for an additional 400 years using the
  // future specification. Years beyond those can be handled by
  // mapping back to a cycle-equivalent year within that range.
  // zic(8) should probably do this so that we don't have to.
  transitions_.resize(hdr.timecnt + 400 * 2);
  extended_ = true;

  // The future specification should match the last two transitions,
  // and those transitions should have different is_dst flags but be
  // in the same year.
  // TODO: Investigate the actual guarantees made by zic.
  const Transition& tr0(transitions_[hdr.timecnt - 1]);
  const Transition& tr1(transitions_[hdr.timecnt - 2]);
  const TransitionType& tt0(transition_types_[tr0.type_index]);
  const TransitionType& tt1(transition_types_[tr1.type_index]);
  const TransitionType& spring(tt0.is_dst ? tt0 : tt1);
  const TransitionType& autumn(tt0.is_dst ? tt1 : tt0);
  CheckTransition(name, spring, posix.dst_offset, true, posix.dst_abbr);
  CheckTransition(name, autumn, posix.std_offset, false, posix.std_abbr);
  last_year_ = LocalTime(tr0.unix_time, tt0).cs.year();
  if (LocalTime(tr1.unix_time, tt1).cs.year() != last_year_) {
    std::clog << name << ": Final transitions not in same year\n";
  }

  // Add the transitions to tr1 and back to tr0 for each extra year.
  const PosixTransition& pt1(tt0.is_dst ? posix.dst_end : posix.dst_start);
  const PosixTransition& pt0(tt0.is_dst ? posix.dst_start : posix.dst_end);
  Transition* tr = &transitions_[hdr.timecnt];  // next trans to fill
  const civil_day jan1(last_year_, 1, 1);
  std::int_fast64_t jan1_time = civil_second(jan1) - unix_epoch;
  int jan1_weekday = (static_cast<int>(get_weekday(jan1)) + 1) % DAYSPERWEEK;
  bool leap_year = IsLeap(last_year_);
  for (const cctz::year_t limit = last_year_ + 400; last_year_ < limit;) {
    last_year_ += 1;  // an additional year of generated transitions
    jan1_time += kSecPerYear[leap_year];
    jan1_weekday = (jan1_weekday + kDaysPerYear[leap_year]) % DAYSPERWEEK;
    leap_year = !leap_year && IsLeap(last_year_);
    tr->unix_time =
        jan1_time + TransOffset(leap_year, jan1_weekday, pt1) - tt0.utc_offset;
    tr++->type_index = tr1.type_index;
    tr->unix_time =
        jan1_time + TransOffset(leap_year, jan1_weekday, pt0) - tt1.utc_offset;
    tr++->type_index = tr0.type_index;
  }
}

bool TimeZoneInfo::Load(const std::string& name, FILE* fp) {
  // Read and validate the header.
  tzhead tzh;
  if (fread(&tzh, 1, sizeof tzh, fp) != sizeof tzh)
    return false;
  if (strncmp(tzh.tzh_magic, TZ_MAGIC, sizeof(tzh.tzh_magic)) != 0)
    return false;
  Header hdr;
  hdr.Build(tzh);
  std::size_t time_len = 4;
  if (tzh.tzh_version[0] != '\0') {
    // Skip the 4-byte data.
    if (fseek(fp, static_cast<long>(hdr.DataLength(time_len)), SEEK_CUR) != 0)
      return false;
    // Read and validate the header for the 8-byte data.
    if (fread(&tzh, 1, sizeof tzh, fp) != sizeof tzh)
      return false;
    if (strncmp(tzh.tzh_magic, TZ_MAGIC, sizeof(tzh.tzh_magic)) != 0)
      return false;
    if (tzh.tzh_version[0] == '\0')
      return false;
    hdr.Build(tzh);
    time_len = 8;
  }
  if (hdr.timecnt < 0 || hdr.typecnt <= 0)
    return false;
  if (hdr.leapcnt != 0) {
    // This code assumes 60-second minutes so we do not want
    // the leap-second encoded zoneinfo. We could reverse the
    // compensation, but it's never in a Google zoneinfo anyway,
    // so currently we simply reject such data.
    return false;
  }
  if (hdr.ttisstdcnt != 0 && hdr.ttisstdcnt != hdr.typecnt)
    return false;
  if (hdr.ttisgmtcnt != 0 && hdr.ttisgmtcnt != hdr.typecnt)
    return false;

  // Read the data into a local buffer.
  std::size_t len = hdr.DataLength(time_len);
  std::vector<char> tbuf(len);
  if (fread(tbuf.data(), 1, len, fp) != len)
    return false;
  const char* bp = tbuf.data();

  // Decode and validate the transitions.
  transitions_.reserve(hdr.timecnt + 2);  // We might add a couple.
  transitions_.resize(hdr.timecnt);
  for (std::int_fast32_t i = 0; i != hdr.timecnt; ++i) {
    transitions_[i].unix_time = (time_len == 4) ? Decode32(bp) : Decode64(bp);
    bp += time_len;
    if (i != 0) {
      // Check that the transitions are ordered by time (as zic guarantees).
      if (!Transition::ByUnixTime()(transitions_[i - 1], transitions_[i]))
        return false;  // out of order
    }
  }
  bool seen_type_0 = false;
  for (std::int_fast32_t i = 0; i != hdr.timecnt; ++i) {
    transitions_[i].type_index = Decode8(bp++);
    if (transitions_[i].type_index >= hdr.typecnt)
      return false;
    if (transitions_[i].type_index == 0)
      seen_type_0 = true;
  }

  // Decode and validate the transition types.
  transition_types_.resize(hdr.typecnt);
  for (std::int_fast32_t i = 0; i != hdr.typecnt; ++i) {
    transition_types_[i].utc_offset = Decode32(bp);
    if (transition_types_[i].utc_offset >= SECSPERDAY ||
        transition_types_[i].utc_offset <= -SECSPERDAY)
      return false;
    bp += 4;
    transition_types_[i].is_dst = (Decode8(bp++) != 0);
    transition_types_[i].abbr_index = Decode8(bp++);
    if (transition_types_[i].abbr_index >= hdr.charcnt)
      return false;
  }

  // Determine the before-first-transition type.
  default_transition_type_ = 0;
  if (seen_type_0 && hdr.timecnt != 0) {
    std::int_fast8_t index = 0;
    if (transition_types_[0].is_dst) {
      index = transitions_[0].type_index;
      while (index != 0 && transition_types_[index].is_dst)
        --index;
    }
    while (index != hdr.typecnt && transition_types_[index].is_dst)
      ++index;
    if (index != hdr.typecnt)
      default_transition_type_ = index;
  }

  // Copy all the abbreviations.
  abbreviations_.assign(bp, hdr.charcnt);
  bp += hdr.charcnt;

  // Skip the unused portions. We've already dispensed with leap-second
  // encoded zoneinfo. The ttisstd/ttisgmt indicators only apply when
  // interpreting a POSIX spec that does not include start/end rules, and
  // that isn't the case here (see "zic -p").
  bp += (8 + 4) * hdr.leapcnt;  // leap-time + TAI-UTC
  bp += 1 * hdr.ttisstdcnt;     // UTC/local indicators
  bp += 1 * hdr.ttisgmtcnt;     // standard/wall indicators

  future_spec_.clear();
  if (tzh.tzh_version[0] != '\0') {
    // Snarf up the NL-enclosed future POSIX spec. Note
    // that version '3' files utilize an extended format.
    if (fgetc(fp) != '\n')
      return false;
    for (int c = fgetc(fp); c != '\n'; c = fgetc(fp)) {
      if (c == EOF)
        return false;
      future_spec_.push_back(static_cast<char>(c));
    }
  }

  // We don't check for EOF so that we're forwards compatible.

  // Trim redundant transitions. zic may have added these to work around
  // differences between the glibc and reference implementations (see
  // zic.c:dontmerge) and the Qt library (see zic.c:WORK_AROUND_QTBUG_53071).
  // For us, they just get in the way when we do future_spec_ extension.
  while (hdr.timecnt > 1) {
    if (!EquivTransitions(transitions_[hdr.timecnt - 1].type_index,
                          transitions_[hdr.timecnt - 2].type_index)) {
      break;
    }
    hdr.timecnt -= 1;
  }
  transitions_.resize(hdr.timecnt);

  // Ensure that there is always a transition in the first half of the
  // time line (the second half is handled in ExtendTransitions()) so
  // that the signed difference between a DateTime and the DateTime of
  // its previous transition is always representable, without overflow.
  // A contemporary zic will usually have already done this for us.
  if (transitions_.empty() || transitions_.front().unix_time >= 0) {
    Transition& tr(*transitions_.emplace(transitions_.begin()));
    tr.unix_time = -(1LL << 59);  // see tz/zic.c "BIG_BANG"
    tr.type_index = default_transition_type_;
    hdr.timecnt += 1;
  }

  // Extend the transitions using the future specification.
  ExtendTransitions(name, hdr);

  // Compute the local civil time for each transition and the preceeding
  // second. These will be used for reverse conversions in MakeTime().
  const TransitionType* ttp = &transition_types_[default_transition_type_];
  for (std::size_t i = 0; i != transitions_.size(); ++i) {
    Transition& tr(transitions_[i]);
    tr.prev_date_time.Assign(LocalTime(tr.unix_time, *ttp).cs);
    tr.prev_date_time.offset -= 1;
    ttp = &transition_types_[tr.type_index];
    tr.date_time.Assign(LocalTime(tr.unix_time, *ttp).cs);
    if (i != 0) {
      // Check that the transitions are ordered by date/time. Essentially
      // this means that an offset change cannot cross another such change.
      // No one does this in practice, and we depend on it in MakeTime().
      if (!Transition::ByDateTime()(transitions_[i - 1], tr))
        return false;  // out of order
    }
  }

  // We remember the transitions found during the last BreakTime() and
  // MakeTime() calls. If the next request is for the same transition we
  // will avoid re-searching.
  local_time_hint_ = 0;
  time_local_hint_ = 0;

  transitions_.shrink_to_fit();
  return true;
}

bool TimeZoneInfo::Load(const std::string& name) {
  // We can ensure that the loading of UTC or any other fixed-offset
  // zone never fails because the simple, fixed-offset state can be
  // internally generated. Note that this depends on our choice to not
  // accept leap-second encoded ("right") zoneinfo.
  if (name == "UTC") return ResetToBuiltinUTC(0);

  // Map time-zone name to its machine-specific path.
  std::string path;
  if (name == "localtime") {
#if defined(_MSC_VER)
    char* localtime = nullptr;
    _dupenv_s(&localtime, nullptr, "LOCALTIME");
    path = localtime ? localtime : "/etc/localtime";
    free(localtime);
#else
    const char* localtime = std::getenv("LOCALTIME");
    path = localtime ? localtime : "/etc/localtime";
#endif
  } else if (!name.empty() && name[0] == '/') {
    path = name;
  } else {
#if defined(_MSC_VER)
    char* tzdir = nullptr;
    _dupenv_s(&tzdir, nullptr, "TZDIR");
    path = tzdir ? tzdir : "/usr/share/zoneinfo";
    free(tzdir);
#else
    const char* tzdir = std::getenv("TZDIR");
    path = tzdir ? tzdir : "/usr/share/zoneinfo";
#endif
    path += '/';
    path += name;
  }

  // Load the time-zone data.
  bool loaded = false;
#if defined(_MSC_VER)
  FILE* fp;
  if (fopen_s(&fp, path.c_str(), "rb") != 0) fp = nullptr;
#else
  FILE* fp = fopen(path.c_str(), "rb");
#endif
  if (fp != nullptr) {
    loaded = Load(name, fp);
    fclose(fp);
  } else {
    char ebuf[64];
    std::clog << path << ": " << errmsg(errno, ebuf, sizeof ebuf) << "\n";
  }
  return loaded;
}

// BreakTime() translation for a particular transition type.
time_zone::absolute_lookup TimeZoneInfo::LocalTime(
    std::int_fast64_t unix_time, const TransitionType& tt) const {
  time_zone::absolute_lookup al;

  // A civil time in "+offset" looks like (time+offset) in UTC.
  // Note: We perform two additions in the civil_second domain to
  // sidestep the chance of overflow in (unix_time + tt.utc_offset).
  al.cs = unix_epoch + unix_time;
  al.cs += tt.utc_offset;

  // Handle offset, is_dst, and abbreviation.
  al.offset = tt.utc_offset;
  al.is_dst = tt.is_dst;
  al.abbr = &abbreviations_[tt.abbr_index];

  return al;
}

// MakeTime() translation with a conversion-preserving offset.
time_zone::civil_lookup TimeZoneInfo::TimeLocal(
    const civil_second& cs, std::int_fast64_t offset) const {
  time_zone::civil_lookup cl = MakeTime(cs);
  cl.pre += sys_seconds(offset);
  cl.trans += sys_seconds(offset);
  cl.post += sys_seconds(offset);
  return cl;
}

time_zone::absolute_lookup TimeZoneInfo::BreakTime(
    const time_point<sys_seconds>& tp) const {
  std::int_fast64_t unix_time = ToUnixSeconds(tp);
  const std::size_t timecnt = transitions_.size();
  if (timecnt == 0 || unix_time < transitions_[0].unix_time) {
    const std::uint_fast8_t type_index = default_transition_type_;
    return LocalTime(unix_time, transition_types_[type_index]);
  }
  if (unix_time >= transitions_[timecnt - 1].unix_time) {
    // After the last transition. If we extended the transitions using
    // future_spec_, shift back to a supported year using the 400-year
    // cycle of calendaric equivalence and then compensate accordingly.
    if (extended_) {
      const std::int_fast64_t diff =
          unix_time - transitions_[timecnt - 1].unix_time;
      const cctz::year_t shift = diff / kSecPer400Years + 1;
      const auto d = sys_seconds(shift * kSecPer400Years);
      time_zone::absolute_lookup al = BreakTime(tp - d);
      al.cs = YearShift(al.cs, shift * 400);
      return al;
    }
    const std::uint_fast8_t type_index = transitions_[timecnt - 1].type_index;
    return LocalTime(unix_time, transition_types_[type_index]);
  }

  const std::size_t hint = local_time_hint_.load(std::memory_order_relaxed);
  if (0 < hint && hint < timecnt) {
    if (unix_time < transitions_[hint].unix_time) {
      if (!(unix_time < transitions_[hint - 1].unix_time)) {
        const std::uint_fast8_t type_index = transitions_[hint - 1].type_index;
        return LocalTime(unix_time, transition_types_[type_index]);
      }
    }
  }

  const Transition target = {unix_time, 0, {0}, {0}};
  const Transition* begin = &transitions_[0];
  const Transition* tr = std::upper_bound(begin, begin + timecnt, target,
                                          Transition::ByUnixTime());
  local_time_hint_.store(tr - begin, std::memory_order_relaxed);
  const std::uint_fast8_t type_index = (--tr)->type_index;
  return LocalTime(unix_time, transition_types_[type_index]);
}

time_zone::civil_lookup TimeZoneInfo::MakeTime(const civil_second& cs) const {
  Transition target;
  DateTime& dt(target.date_time);
  dt.Assign(cs);

  const std::size_t timecnt = transitions_.size();
  if (timecnt == 0) {
    // Use the default offset.
    const std::int_fast32_t default_offset =
        transition_types_[default_transition_type_].utc_offset;
    return MakeUnique((dt - DateTime{0}) - default_offset);
  }

  // Find the first transition after our target date/time.
  const Transition* tr = nullptr;
  const Transition* begin = &transitions_[0];
  const Transition* end = begin + timecnt;
  if (dt < begin->date_time) {
    tr = begin;
  } else if (!(dt < transitions_[timecnt - 1].date_time)) {
    tr = end;
  } else {
    const std::size_t hint = time_local_hint_.load(std::memory_order_relaxed);
    if (0 < hint && hint < timecnt) {
      if (dt < transitions_[hint].date_time) {
        if (!(dt < transitions_[hint - 1].date_time)) {
          tr = begin + hint;
        }
      }
    }
    if (tr == nullptr) {
      tr = std::upper_bound(begin, end, target, Transition::ByDateTime());
      time_local_hint_.store(tr - begin, std::memory_order_relaxed);
    }
  }

  if (tr == begin) {
    if (!(tr->prev_date_time < dt)) {
      // Before first transition, so use the default offset.
      const std::int_fast32_t default_offset =
          transition_types_[default_transition_type_].utc_offset;
      return MakeUnique((dt - DateTime{0}) - default_offset);
    }
    // tr->prev_date_time < dt < tr->date_time
    return MakeSkipped(*tr, dt);
  }

  if (tr == end) {
    if ((--tr)->prev_date_time < dt) {
      // After the last transition. If we extended the transitions using
      // future_spec_, shift back to a supported year using the 400-year
      // cycle of calendaric equivalence and then compensate accordingly.
      if (extended_ && cs.year() > last_year_) {
        const cctz::year_t shift = (cs.year() - last_year_) / 400 + 1;
        return TimeLocal(YearShift(cs, shift * -400), shift * kSecPer400Years);
      }
      return MakeUnique(tr->unix_time + (dt - tr->date_time));
    }
    // tr->date_time <= dt <= tr->prev_date_time
    return MakeRepeated(*tr, dt);
  }

  if (tr->prev_date_time < dt) {
    // tr->prev_date_time < dt < tr->date_time
    return MakeSkipped(*tr, dt);
  }

  if (!((--tr)->prev_date_time < dt)) {
    // tr->date_time <= dt <= tr->prev_date_time
    return MakeRepeated(*tr, dt);
  }

  // In between transitions.
  return MakeUnique(tr->unix_time + (dt - tr->date_time));
}

}  // namespace cctz
