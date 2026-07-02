#pragma once
#include <type_traits>
#include <IO/ReadHelpers.h>
#include <Core/Defines.h>
#include <base/shift10.h>
#include <Common/StringUtils.h>

/** Methods for reading floating point numbers from text with decimal representation.
  * There are "precise", "fast" and "simple" implementations.
  *
  * Neither of methods support hexadecimal numbers (0xABC), binary exponent (1p100), leading plus sign.
  *
  * Precise method always returns a number that is the closest machine representable number to the input.
  *
  * Fast method is faster (up to 3 times) and usually return the same value,
  *  but in rare cases result may differ by lest significant bit (for Float32)
  *  and by up to two least significant bits (for Float64) from precise method.
  * Also fast method may parse some garbage as some other unspecified garbage.
  *
  * Simple method is little faster for cases of parsing short (few digit) integers, but less precise and slower in other cases.
  * It's not recommended to use simple method and it is left only for reference.
  *
  * For performance test, look at 'read_float_perf' test.
  *
  * The implementations live in readFloatText.cpp so that the heavy <fast_float/fast_float.h>
  * dependency and the parsing bodies do not get recompiled into every translation unit that
  * only needs to call these functions.
  *
  * For precision test.
  * Parse all existing Float32 numbers:

CREATE TABLE test.floats ENGINE = Log AS SELECT reinterpretAsFloat32(reinterpretAsString(toUInt32(number))) AS x FROM numbers(0x100000000);

WITH
    toFloat32(toString(x)) AS y,
    reinterpretAsUInt32(reinterpretAsString(x)) AS bin_x,
    reinterpretAsUInt32(reinterpretAsString(y)) AS bin_y,
    abs(bin_x - bin_y) AS diff
SELECT
    diff,
    count()
FROM test.floats
WHERE NOT isNaN(x)
GROUP BY diff
ORDER BY diff ASC
LIMIT 100

  * Here are the results:
  *
    Precise:
    ┌─diff─┬────count()─┐
    │    0 │ 4278190082 │
    └──────┴────────────┘
    (100% roundtrip property)

    Fast:
    ┌─diff─┬────count()─┐
    │    0 │ 3685260580 │
    │    1 │  592929502 │
    └──────┴────────────┘
    (The difference is 1 in least significant bit in 13.8% of numbers.)

    Simple:
    ┌─diff─┬────count()─┐
    │    0 │ 2169879994 │
    │    1 │ 1807178292 │
    │    2 │  269505944 │
    │    3 │   28826966 │
    │    4 │    2566488 │
    │    5 │     212878 │
    │    6 │      18276 │
    │    7 │       1214 │
    │    8 │         30 │
    └──────┴────────────┘

  * Parse random Float64 numbers:

WITH
    rand64() AS bin_x,
    reinterpretAsFloat64(reinterpretAsString(bin_x)) AS x,
    toFloat64(toString(x)) AS y,
    reinterpretAsUInt64(reinterpretAsString(y)) AS bin_y,
    abs(bin_x - bin_y) AS diff
SELECT
    diff,
    count()
FROM numbers(100000000)
WHERE NOT isNaN(x)
GROUP BY diff
ORDER BY diff ASC
LIMIT 100

  */


namespace DB
{

/// Returns true, iff parsed.
bool parseInfinity(ReadBuffer & buf);
bool parseNaN(ReadBuffer & buf);

void assertInfinity(ReadBuffer & buf);
void assertNaN(ReadBuffer & buf);


/// Parsing methods. Definitions and the explicit instantiations (for BFloat16, Float32, Float64)
/// are in readFloatText.cpp.

template <typename T> void readFloatTextPrecise(T & x, ReadBuffer & in);
template <typename T> bool tryReadFloatTextPrecise(T & x, ReadBuffer & in);

template <typename T> void readFloatTextFast(T & x, ReadBuffer & in);
template <typename T> bool tryReadFloatTextFast(T & x, ReadBuffer & in);

template <typename T> void readFloatTextSimple(T & x, ReadBuffer & in);
template <typename T> bool tryReadFloatTextSimple(T & x, ReadBuffer & in);

/// Implementation that is selected as default.
template <typename T> void readFloatText(T & x, ReadBuffer & in);
template <typename T> bool tryReadFloatText(T & x, ReadBuffer & in);

/// Don't read exponent part of the number.
template <typename T> bool tryReadFloatTextNoExponent(T & x, ReadBuffer & in);

/// With a @has_fractional flag. Used for input_format_try_infer_integers.
template <typename T> bool tryReadFloatTextExt(T & x, ReadBuffer & in, bool & has_fractional);
template <typename T> bool tryReadFloatTextExtNoExponent(T & x, ReadBuffer & in, bool & has_fractional);

}
