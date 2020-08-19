#include <IO/readFloatText.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

/** Must successfully parse inf, INF and Infinity.
  * All other variants in different cases are also parsed for simplicity.
  */
bool parseInfinity(ReadBuffer & buf)
{
    if (!checkStringCaseInsensitive("inf", buf))
        return false;

    /// Just inf.
    if (buf.eof() || !isWordCharASCII(*buf.position()))
        return true;

    /// If word characters after inf, it should be infinity.
    return checkStringCaseInsensitive("inity", buf);
}


/** Must successfully parse nan, NAN and NaN.
  * All other variants in different cases are also parsed for simplicity.
  */
bool parseNaN(ReadBuffer & buf)
{
    return checkStringCaseInsensitive("nan", buf);
}


void assertInfinity(ReadBuffer & buf)
{
    if (!parseInfinity(buf))
        throw Exception("Cannot parse infinity.", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
}

void assertNaN(ReadBuffer & buf)
{
    if (!parseNaN(buf))
        throw Exception("Cannot parse NaN.", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
}


template void readFloatTextPrecise<Float32>(Float32 &, ReadBuffer &);
template void readFloatTextPrecise<Float64>(Float64 &, ReadBuffer &);
template bool tryReadFloatTextPrecise<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatTextPrecise<Float64>(Float64 &, ReadBuffer &);

template void readFloatTextFast<Float32>(Float32 &, ReadBuffer &);
template void readFloatTextFast<Float64>(Float64 &, ReadBuffer &);
template bool tryReadFloatTextFast<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatTextFast<Float64>(Float64 &, ReadBuffer &);

template void readFloatTextSimple<Float32>(Float32 &, ReadBuffer &);
template void readFloatTextSimple<Float64>(Float64 &, ReadBuffer &);
template bool tryReadFloatTextSimple<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatTextSimple<Float64>(Float64 &, ReadBuffer &);

template void readFloatText<Float32>(Float32 &, ReadBuffer &);
template void readFloatText<Float64>(Float64 &, ReadBuffer &);
template bool tryReadFloatText<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatText<Float64>(Float64 &, ReadBuffer &);

}
