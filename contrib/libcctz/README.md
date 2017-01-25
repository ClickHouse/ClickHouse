This is not an official Google product.

# Overview

CCTZ contains two libraries that cooperate with `<chrono>` to give C++
programmers all the necessary tools for computing with dates, times, and time
zones in a simple and correct manner. The libraries in CCTZ are:

* **The Civil-Time Library** &mdash; This is a header-only library that supports
  computing with human-scale time, such as dates (which are represented by the
  `cctz::civil_day` class). This library is declared in [`include/civil_time.h`]
(https://github.com/google/cctz/blob/master/include/civil_time.h).
* **The Time-Zone Library** &mdash; This library uses the IANA time zone
  database that is installed on the system to convert between *absolute time*
  and *civil time*. This library is declared in [`include/time_zone.h`](https://github.com/google/cctz/blob/master/include/time_zone.h).

These libraries are currently known to work on **Linux** and **Mac OS X**. We
are actively interested in help getting them working on Windows. Please contact
us if you're interested in contributing.

# Getting Started

CCTZ is best built and tested using the [Bazel](http://bazel.io) build system
and the [Google Test](https://github.com/google/googletest) framework. (There
is also a simple [`Makefile`](https://github.com/google/cctz/blob/master/Makefile)
that should work if you're unable to use Bazel.)

1.  Download/install Bazel http://bazel.io/docs/install.html
2.  Get the cctz source: `git clone https://github.com/google/cctz.git` then `cd
    cctz`
3.  Build cctz and run the tests: `bazel test :all`

Note: When using CCTZ in your own project, you might find it easiest to compile
the sources using your existing build system.

Next Steps:

1.  See the documentation for the libraries in CCTZ:
  * Civil Time: [`include/civil_time.h`](https://github.com/google/cctz/blob/master/include/civil_time.h)
  * Time Zone: [`include/time_zone.h`](https://github.com/google/cctz/blob/master/include/time_zone.h)
2.  Look at the examples in https://github.com/google/cctz/tree/master/examples
3.  Join our mailing list to ask questions and keep informed of changes: 
  * https://groups.google.com/forum/#!forum/cctz

# Fundamental Concepts

*[The concepts presented here describe general truths about the problem domain
and are library and language agnostic. An understanding of these concepts helps
the programmer correctly reason about even the most-complicated time-programming
challenges and produce the simplest possible solutions.]*

There are two main ways to think about time in a computer program: as *absolute
time*, and as *civil time*. Both have their uses and it is important to
understand when each is appropriate. Absolute and civil times may be converted
back and forth using a *time zone* &mdash; this is the only way to correctly
convert between them. Let us now look more deeply at the three main concepts of
time programming: Absolute Time, Civil Time, and Time Zone.

*Absolute time* uniquely and universally represents a specific instant in time.
It has no notion of calendars, or dates, or times of day. Instead, it is a
measure of the passage of real time, typically as a simple count of ticks since
some epoch. Absolute times are independent of all time zones and do not suffer
from human-imposed complexities such as daylight-saving time (DST). Many C++
types exist to represent absolute times, classically `time_t` and more recently
`std::chrono::time_point`.

*Civil time* is the legally recognized representation of time for ordinary
affairs (cf. http://www.merriam-webster.com/dictionary/civil). It is a
human-scale representation of time that consists of the six fields &mdash;
year, month, day, hour, minute, and second (sometimes shortened to "YMDHMS")
&mdash; and it follows the rules of the Proleptic Gregorian Calendar, with
24-hour days divided into 60-minute hours and 60-second minutes. Like absolute
times, civil times are also independent of all time zones and their related
complexities (e.g., DST). While `std::tm` contains the six civil-time fields
(YMDHMS), plus a few more, it does not have behavior to enforce the rules of
civil time.

*Time zones* are geo-political regions within which human-defined rules are
shared to convert between the absolute-time and civil-time domains. A time
zone's rules include things like the region's offset from the UTC time standard,
daylight-saving adjustments, and short abbreviation strings. Time zones often
have a history of disparate rules that apply only for certain periods, because
the rules may change at the whim of a region's local government. For this
reason, time-zone rules are usually compiled into data snapshots that are used
at runtime to perform conversions between absolute and civil times. There is
currently no C++ standard library supporting arbitrary time zones.

In order for programmers to reason about and program applications that correctly
deal with these concepts, they must have a library that correctly implements the
above concepts. CCTZ adds to the existing C++11 `<chrono>` library to fully
implement the above concepts.

* Absolute time &mdash; This is implemented by the existing C++11 
  [`<chrono>`](http://en.cppreference.com/w/cpp/chrono)
  library without modification. For example, an absolute point in time is
  represented by a `std::chrono::time_point`.
* Civil time &mdash; This is implemented by the [`include/civil_time.h`](https://github.com/google/cctz/blob/master/include/civil_time.h) library
  that is provided as part of CCTZ. For example, a "date" is represented by a
  `cctz::civil_day`.
* Time zone &mdash; This is implemented by the [`include/time_zone.h`](https://github.com/google/cctz/blob/master/include/time_zone.h) library
  that is provided as part of CCTZ. For example, a time zone is represented by
  an instance of the class `cctz::time_zone`.

# Examples

## Hello February 2016

This "hello world" example uses a for-loop to iterate the days from the first of
February until the month of March. Each day is streamed to output, and if the
day happens to be the 29th, we also output the day of the week.

```
#include <iostream>
#include "civil_time.h"

int main() {
  for (cctz::civil_day d(2016, 2, 1); d < cctz::civil_month(2016, 3); ++d) {
    std::cout << "Hello " << d;
    if (d.day() == 29) {
      std::cout << " <- leap day is a " << cctz::get_weekday(d);
    }
    std::cout << "\n";
  }
}
```

The output of the above program is

```
Hello 2016-02-01
Hello 2016-02-02
Hello 2016-02-03
[...]
Hello 2016-02-27
Hello 2016-02-28
Hello 2016-02-29 <- leap day is a Monday
```

## One giant leap

This example shows how to use all three libraries (`<chrono>`, civil time, and
time zone) together. In this example, we know that viewers in New York watched
Neil Armstrong first walk on the moon on July 20, 1969 at 10:56 PM. But we'd
like to see what time it was for our friend watching in Sydney Australia.

```
#include <iostream>
#include "civil_time.h"
#include "time_zone.h"

int main() {
  cctz::time_zone nyc;
  cctz::load_time_zone("America/New_York", &nyc);

  // Converts the input civil time in NYC to an absolute time.
  const auto moon_walk =
    cctz::convert(cctz::civil_second(1969, 7, 20, 22, 56, 0), nyc);

  std::cout << "Moon walk in NYC: "
            << cctz::format("%Y-%m-%d %H:%M:%S %Ez\n", moon_walk, nyc);

  cctz::time_zone syd;
  if (!cctz::load_time_zone("Australia/Sydney", &syd)) return -1;
  std::cout << "Moon walk in SYD: "
            << cctz::format("%Y-%m-%d %H:%M:%S %Ez\n", moon_walk, syd);
}
```

The output of the above program is

```
Moon walk in NYC: 1969-07-20 22:56:00 -04:00
Moon walk in SYD: 1969-07-21 12:56:00 +10:00
```

This example shows that the absolute time (the `std::chrono::time_point`) of the
first walk on the moon is the same no matter the time zone of the viewer (the
same time point is used in both calls to `format()`). The only difference is the
time zone in which the `moon_walk` time point is rendered. And in this case we
can see that our friend in Sydney was probably eating lunch while watching that
historic event.

# References

* CCTZ [FAQ](https://github.com/google/cctz/wiki/FAQ)
* See also the [Time Programming Fundamentals](https://youtu.be/2rnIHsqABfM)
  talk from CppCon 2015 ([slides available here](http://goo.gl/ofof4N)). This
  talk mostly describes the older CCTZ v1 API, but the *concepts* are the same.
* ISO C++ proposal to standardize the Civil-Time Library:
  https://github.com/devjgm/papers/blob/master/d0215r1.md
* ISO C++ proposal to standardize the Time-Zone Library:
  https://github.com/devjgm/papers/blob/master/d0216r1.md
