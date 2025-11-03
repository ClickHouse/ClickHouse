#include <Functions/FunctionsConversion.h>

namespace DB
{

REGISTER_FUNCTION(Conversion)
{
    /// toUInt8 documentation
    FunctionDocumentation::Description toUInt8_description = R"(
Converts an input value to a value of type [`UInt8`](../data-types/int-uint.md).
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt8('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
For example: `SELECT toUInt8(256) == 0;`.
:::

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

See also:
- [`toUInt8OrZero`](#toUInt8OrZero).
- [`toUInt8OrNull`](#toUInt8OrNull).
- [`toUInt8OrDefault`](#toUInt8OrDefault).
    )";
    FunctionDocumentation::Syntax toUInt8_syntax = "toUInt8(expr)";
    FunctionDocumentation::Arguments toUInt8_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toUInt8_returned_value = {"Returns an 8-bit unsigned integer value.", {"UInt8"}};
    FunctionDocumentation::Examples toUInt8_examples = {
    {
        "Usage example",
        R"(
SELECT
    toUInt8(8),
    toUInt8(8.8),
    toUInt8('8')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt8(8):   8
toUInt8(8.8): 8
toUInt8('8'): 8
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toUInt8_introduced_in = {1, 1};
    FunctionDocumentation::Category toUInt8_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toUInt8_documentation = {toUInt8_description, toUInt8_syntax, toUInt8_arguments, toUInt8_returned_value, toUInt8_examples, toUInt8_introduced_in, toUInt8_category};

    /// toUInt16 documentation
    FunctionDocumentation::Description toUInt16_description = R"(
Converts an input value to a value of type [`UInt16`](../data-types/int-uint.md).
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt16('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt16`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
For example: `SELECT toUInt16(65536) == 0;`.
:::

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

See also:
- [`toUInt16OrZero`](#toUInt16OrZero).
- [`toUInt16OrNull`](#toUInt16OrNull).
- [`toUInt16OrDefault`](#toUInt16OrDefault).
    )";
    FunctionDocumentation::Syntax toUInt16_syntax = "toUInt16(expr)";
    FunctionDocumentation::Arguments toUInt16_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toUInt16_returned_value = {"Returns a 16-bit unsigned integer value.", {"UInt16"}};
    FunctionDocumentation::Examples toUInt16_examples = {
    {
        "Usage example",
        R"(
SELECT
    toUInt16(16),
    toUInt16(16.16),
    toUInt16('16')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt16(16):    16
toUInt16(16.16): 16
toUInt16('16'):  16
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toUInt16_introduced_in = {1, 1};
    FunctionDocumentation::Category toUInt16_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toUInt16_documentation = {toUInt16_description, toUInt16_syntax, toUInt16_arguments, toUInt16_returned_value, toUInt16_examples, toUInt16_introduced_in, toUInt16_category};

    factory.registerFunction<detail::FunctionToUInt8>(toUInt8_documentation);
    factory.registerFunction<detail::FunctionToUInt16>(toUInt16_documentation);

    /// toUInt32 documentation
    FunctionDocumentation::Description toUInt32_description = R"(
Converts an input value to a value of type [`UInt32`](../data-types/int-uint.md).
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt32('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt32`](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
For example: `SELECT toUInt32(4294967296) == 0;`
:::

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

See also:
- [`toUInt32OrZero`](#toUInt32OrZero).
- [`toUInt32OrNull`](#toUInt32OrNull).
- [`toUInt32OrDefault`](#toUInt32OrDefault).
    )";
    FunctionDocumentation::Syntax toUInt32_syntax = "toUInt32(expr)";
    FunctionDocumentation::Arguments toUInt32_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toUInt32_returned_value = {"Returns a 32-bit unsigned integer value.", {"UInt32"}};
    FunctionDocumentation::Examples toUInt32_examples = {
    {
        "Usage example",
        R"(
SELECT
    toUInt32(32),
    toUInt32(32.32),
    toUInt32('32')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt32(32):    32
toUInt32(32.32): 32
toUInt32('32'):  32
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toUInt32_introduced_in = {1, 1};
    FunctionDocumentation::Category toUInt32_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toUInt32_documentation = {toUInt32_description, toUInt32_syntax, toUInt32_arguments, toUInt32_returned_value, toUInt32_examples, toUInt32_introduced_in, toUInt32_category};

    factory.registerFunction<detail::FunctionToUInt32>(toUInt32_documentation);

    /// toUInt64 documentation
    FunctionDocumentation::Description toUInt64_description = R"(
Converts an input value to a value of type [`UInt64`](../data-types/int-uint.md).
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported types:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt64('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt64`](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
For example: `SELECT toUInt64(18446744073709551616) == 0;`
:::

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

See also:
- [`toUInt64OrZero`](#toUInt64OrZero).
- [`toUInt64OrNull`](#toUInt64OrNull).
- [`toUInt64OrDefault`](#toUInt64OrDefault).
    )";
    FunctionDocumentation::Syntax toUInt64_syntax = "toUInt64(expr)";
    FunctionDocumentation::Arguments toUInt64_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toUInt64_returned_value = {"Returns a 64-bit unsigned integer value.", {"UInt64"}};
    FunctionDocumentation::Examples toUInt64_examples = {
    {
        "Usage example",
        R"(
SELECT
    toUInt64(64),
    toUInt64(64.64),
    toUInt64('64')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt64(64):    64
toUInt64(64.64): 64
toUInt64('64'):  64
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toUInt64_introduced_in = {1, 1};
    FunctionDocumentation::Category toUInt64_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toUInt64_documentation = {toUInt64_description, toUInt64_syntax, toUInt64_arguments, toUInt64_returned_value, toUInt64_examples, toUInt64_introduced_in, toUInt64_category};

    factory.registerFunction<detail::FunctionToUInt64>(toUInt64_documentation);

    /// toUInt128 documentation
    FunctionDocumentation::Description toUInt128_description = R"(
Converts an input value to a value of type [`UInt128`](/sql-reference/functions/type-conversion-functions#touint128).
Throws an exception in case of an error.
The function uses rounding towards zero, meaning it truncates fractional digits of numbers.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt128('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of UInt128, the result over or under flows.
This is not considered an error.
:::

See also:
- [`toUInt128OrZero`](#toUInt128OrZero).
- [`toUInt128OrNull`](#toUInt128OrNull).
- [`toUInt128OrDefault`](#toUInt128OrDefault).
    )";
    FunctionDocumentation::Syntax toUInt128_syntax = "toUInt128(expr)";
    FunctionDocumentation::Arguments toUInt128_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toUInt128_returned_value = {"Returns a 128-bit unsigned integer value.", {"UInt128"}};
    FunctionDocumentation::Examples toUInt128_examples = {
    {
        "Usage example",
        R"(
SELECT
    toUInt128(128),
    toUInt128(128.8),
    toUInt128('128')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt128(128):   128
toUInt128(128.8): 128
toUInt128('128'): 128
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toUInt128_introduced_in = {1, 1};
    FunctionDocumentation::Category toUInt128_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toUInt128_documentation = {toUInt128_description, toUInt128_syntax, toUInt128_arguments, toUInt128_returned_value, toUInt128_examples, toUInt128_introduced_in, toUInt128_category};

    factory.registerFunction<detail::FunctionToUInt128>(toUInt128_documentation);
    FunctionDocumentation::Description toUInt256_description = R"(
Converts an input value to a value of type UInt256.
Throws an exception in case of an error.
The function uses rounding towards zero, meaning it truncates fractional digits of numbers.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt256('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of UInt256, the result over or under flows.
This is not considered an error.
:::

See also:
- [`toUInt256OrZero`](#toUInt256OrZero).
- [`toUInt256OrNull`](#toUInt256OrNull).
- [`toUInt256OrDefault`](#toUInt256OrDefault).
    )";
    FunctionDocumentation::Syntax toUInt256_syntax = "toUInt256(expr)";
    FunctionDocumentation::Arguments toUInt256_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toUInt256_returned_value = {"Returns a 256-bit unsigned integer value.", {"UInt256"}};
    FunctionDocumentation::Examples toUInt256_examples = {
    {
        "Usage example",
        R"(
SELECT
    toUInt256(256),
    toUInt256(256.256),
    toUInt256('256')
FORMAT Vertical
         )",
         R"(
Row 1:
──────
toUInt256(256):     256
toUInt256(256.256): 256
toUInt256('256'):   256
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toUInt256_introduced_in = {1, 1};
    FunctionDocumentation::Category toUInt256_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toUInt256_documentation = {toUInt256_description, toUInt256_syntax, toUInt256_arguments, toUInt256_returned_value, toUInt256_examples, toUInt256_introduced_in, toUInt256_category};

    factory.registerFunction<detail::FunctionToUInt256>(toUInt256_documentation);

    /// toInt8 documentation
    FunctionDocumentation::Description toInt8_description = R"(
Converts an input value to a value of type [`Int8`](../data-types/int-uint.md).
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt8('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
For example: `SELECT toInt8(128) == -128;`.
:::

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

See also:
- [`toInt8OrZero`](#toInt8OrZero).
- [`toInt8OrNull`](#toInt8OrNull).
- [`toInt8OrDefault`](#toInt8OrDefault).
    )";
    FunctionDocumentation::Syntax toInt8_syntax = "toInt8(expr)";
    FunctionDocumentation::Arguments toInt8_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toInt8_returned_value = {"Returns an 8-bit integer value.", {"Int8"}};
    FunctionDocumentation::Examples toInt8_examples = {
    {
        "Usage example",
        R"(
SELECT
    toInt8(-8),
    toInt8(-8.8),
    toInt8('-8')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt8(-8):   -8
toInt8(-8.8): -8
toInt8('-8'): -8
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toInt8_introduced_in = {1, 1};
    FunctionDocumentation::Category toInt8_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toInt8_documentation = {toInt8_description, toInt8_syntax, toInt8_arguments, toInt8_returned_value, toInt8_examples, toInt8_introduced_in, toInt8_category};

    factory.registerFunction<detail::FunctionToInt8>(toInt8_documentation);

    /// toInt16 documentation
    FunctionDocumentation::Description toInt16_description = R"(
Converts an input value to a value of type [`Int16`](../data-types/int-uint.md).
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt16('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
For example: `SELECT toInt16(32768) == -32768;`.
:::

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

See also:
- [`toInt16OrZero`](#toInt16OrZero).
- [`toInt16OrNull`](#toInt16OrNull).
- [`toInt16OrDefault`](#toInt16OrDefault).
    )";
    FunctionDocumentation::Syntax toInt16_syntax = "toInt16(expr)";
    FunctionDocumentation::Arguments toInt16_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toInt16_returned_value = {"Returns a 16-bit integer value.", {"Int16"}};
    FunctionDocumentation::Examples toInt16_examples = {
    {
        "Usage example",
        R"(
SELECT
    toInt16(-16),
    toInt16(-16.16),
    toInt16('-16')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt16(-16):    -16
toInt16(-16.16): -16
toInt16('-16'):  -16
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toInt16_introduced_in = {1, 1};
    FunctionDocumentation::Category toInt16_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toInt16_documentation = {toInt16_description, toInt16_syntax, toInt16_arguments, toInt16_returned_value, toInt16_examples, toInt16_introduced_in, toInt16_category};

    factory.registerFunction<detail::FunctionToInt16>(toInt16_documentation);

    /// toInt32 documentation
    FunctionDocumentation::Description toInt32_description = R"(
Converts an input value to a value of type [`Int32`](../data-types/int-uint.md).
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt32('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int32](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
For example: `SELECT toInt32(2147483648) == -2147483648;`
:::

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

See also:
- [`toInt32OrZero`](#toInt32OrZero).
- [`toInt32OrNull`](#toInt32OrNull).
- [`toInt32OrDefault`](#toInt32OrDefault).
    )";
    FunctionDocumentation::Syntax toInt32_syntax = "toInt32(expr)";
    FunctionDocumentation::Arguments toInt32_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toInt32_returned_value = {"Returns a 32-bit integer value.", {"Int32"}};
    FunctionDocumentation::Examples toInt32_examples = {
    {
        "Usage example",
        R"(
SELECT
    toInt32(-32),
    toInt32(-32.32),
    toInt32('-32')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt32(-32):    -32
toInt32(-32.32): -32
toInt32('-32'):  -32
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toInt32_introduced_in = {1, 1};
    FunctionDocumentation::Category toInt32_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toInt32_documentation = {toInt32_description, toInt32_syntax, toInt32_arguments, toInt32_returned_value, toInt32_examples, toInt32_introduced_in, toInt32_category};

    factory.registerFunction<detail::FunctionToInt32>(toInt32_documentation);

    /// toInt64 documentation
    FunctionDocumentation::Description toInt64_description = R"(
Converts an input value to a value of type [`Int64`](../data-types/int-uint.md).
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt64('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int64](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
For example: `SELECT toInt64(9223372036854775808) == -9223372036854775808;`
:::

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

See also:
- [`toInt64OrZero`](#toInt64OrZero).
- [`toInt64OrNull`](#toInt64OrNull).
- [`toInt64OrDefault`](#toInt64OrDefault).
    )";
    FunctionDocumentation::Syntax toInt64_syntax = "toInt64(expr)";
    FunctionDocumentation::Arguments toInt64_arguments = {
        {"expr", "Expression returning a number or a string representation of a number. Supported: values or string representations of type (U)Int*, values of type Float*. Unsupported: string representations of Float* values including NaN and Inf, string representations of binary and hexadecimal values.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toInt64_returned_value = {"Returns a 64-bit integer value.", {"Int64"}};
    FunctionDocumentation::Examples toInt64_examples = {
    {
        "Usage example",
        R"(
SELECT
    toInt64(-64),
    toInt64(-64.64),
    toInt64('-64')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt64(-64):    -64
toInt64(-64.64): -64
toInt64('-64'):  -64
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toInt64_introduced_in = {1, 1};
    FunctionDocumentation::Category toInt64_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toInt64_documentation = {toInt64_description, toInt64_syntax, toInt64_arguments, toInt64_returned_value, toInt64_examples, toInt64_introduced_in, toInt64_category};

    factory.registerFunction<detail::FunctionToInt64>(toInt64_documentation);

    /// toInt128 documentation
    FunctionDocumentation::Description toInt128_description = R"(
Converts an input value to a value of type [Int128](/sql-reference/data-types/int-uint).
Throws an exception in case of an error.
The function uses rounding towards zero, meaning it truncates fractional digits of numbers.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt128('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of Int128, the result over or under flows.
This is not considered an error.
:::

See also:
- [`toInt128OrZero`](#toInt128OrZero).
- [`toInt128OrNull`](#toInt128OrNull).
- [`toInt128OrDefault`](#toInt128OrDefault).
    )";
    FunctionDocumentation::Syntax toInt128_syntax = "toInt128(expr)";
    FunctionDocumentation::Arguments toInt128_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toInt128_returned_value = {"Returns a 128-bit integer value.", {"Int128"}};
    FunctionDocumentation::Examples toInt128_examples = {
    {
        "Usage example",
        R"(
SELECT
    toInt128(-128),
    toInt128(-128.8),
    toInt128('-128')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt128(-128):   -128
toInt128(-128.8): -128
toInt128('-128'): -128
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toInt128_introduced_in = {1, 1};
    FunctionDocumentation::Category toInt128_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toInt128_documentation = {toInt128_description, toInt128_syntax, toInt128_arguments, toInt128_returned_value, toInt128_examples, toInt128_introduced_in, toInt128_category};

    factory.registerFunction<detail::FunctionToInt128>(toInt128_documentation);

    /// toInt256 documentation
    FunctionDocumentation::Description toInt256_description = R"(
Converts an input value to a value of type [Int256](/sql-reference/data-types/int-uint).
Throws an exception in case of an error.
The function uses rounding towards zero, meaning it truncates fractional digits of numbers.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values of type Float*.

Unsupported arguments:
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt256('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of Int256, the result over or under flows.
This is not considered an error.
:::

See also:
- [`toInt256OrZero`](#toInt256OrZero).
- [`toInt256OrNull`](#toInt256OrNull).
- [`toInt256OrDefault`](#toInt256OrDefault).
    )";
    FunctionDocumentation::Syntax toInt256_syntax = "toInt256(expr)";
    FunctionDocumentation::Arguments toInt256_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toInt256_returned_value = {"Returns a 256-bit integer value.", {"Int256"}};
    FunctionDocumentation::Examples toInt256_examples = {
    {
        "Usage example",
        R"(
SELECT
    toInt256(-256),
    toInt256(-256.256),
    toInt256('-256')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt256(-256):     -256
toInt256(-256.256): -256
toInt256('-256'):   -256
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toInt256_introduced_in = {1, 1};
    FunctionDocumentation::Category toInt256_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toInt256_documentation = {toInt256_description, toInt256_syntax, toInt256_arguments, toInt256_returned_value, toInt256_examples, toInt256_introduced_in, toInt256_category};

    factory.registerFunction<detail::FunctionToInt256>(toInt256_documentation);

    /// toBFloat16 documentation
    FunctionDocumentation::Description toBFloat16_description = R"(
Converts an input value to a value of type BFloat16.
Throws an exception in case of an error.

See also:
- [`toBFloat16OrZero`](#toBFloat16OrZero).
- [`toBFloat16OrNull`](#toBFloat16OrNull).
    )";
    FunctionDocumentation::Syntax toBFloat16_syntax = "toBFloat16(expr)";
    FunctionDocumentation::Arguments toBFloat16_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toBFloat16_returned_value = {"Returns a 16-bit brain-float value.", {"BFloat16"}};
    FunctionDocumentation::Examples toBFloat16_examples = {
    {
        "Usage example",
        R"(
SELECT
toBFloat16(toFloat32(42.7)),
toBFloat16(toFloat32('42.7')),
toBFloat16('42.7')
FORMAT Vertical;
        )",
        R"(
toBFloat16(toFloat32(42.7)): 42.5
toBFloat16(t⋯32('42.7')):    42.5
toBFloat16('42.7'):          42.5
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toBFloat16_introduced_in = {1, 1};
    FunctionDocumentation::Category toBFloat16_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toBFloat16_documentation = {toBFloat16_description, toBFloat16_syntax, toBFloat16_arguments, toBFloat16_returned_value, toBFloat16_examples, toBFloat16_introduced_in, toBFloat16_category};

    factory.registerFunction<detail::FunctionToBFloat16>(toBFloat16_documentation);

    /// toFloat32 documentation
    FunctionDocumentation::Description toFloat32_description = R"(
Converts an input value to a value of type [Float32](/sql-reference/data-types/float).
Throws an exception in case of an error.

Supported arguments:
- Values of type (U)Int*.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float*, including `NaN` and `Inf`.
- String representations of Float*, including `NaN` and `Inf` (case-insensitive).

Unsupported arguments:
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat32('0xc0fe');`.

See also:
- [`toFloat32OrZero`](#toFloat32OrZero).
- [`toFloat32OrNull`](#toFloat32OrNull).
- [`toFloat32OrDefault`](#toFloat32OrDefault).
    )";
    FunctionDocumentation::Syntax toFloat32_syntax = "toFloat32(expr)";
    FunctionDocumentation::Arguments toFloat32_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toFloat32_returned_value = {"Returns a 32-bit floating point value.", {"Float32"}};
    FunctionDocumentation::Examples toFloat32_examples = {
    {
        "Usage example",
        R"(
SELECT
    toFloat32(42.7),
    toFloat32('42.7'),
    toFloat32('NaN')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toFloat32(42.7):   42.7
toFloat32('42.7'): 42.7
toFloat32('NaN'):  nan
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toFloat32_introduced_in = {1, 1};
    FunctionDocumentation::Category toFloat32_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toFloat32_documentation = {toFloat32_description, toFloat32_syntax, toFloat32_arguments, toFloat32_returned_value, toFloat32_examples, toFloat32_introduced_in, toFloat32_category};

    factory.registerFunction<detail::FunctionToFloat32>(toFloat32_documentation);

    /// toFloat64 documentation
    FunctionDocumentation::Description toFloat64_description = R"(
Converts an input value to a value of type [`Float64`](../data-types/float.md).
Throws an exception in case of an error.

Supported arguments:
- Values of type (U)Int*.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float*, including `NaN` and `Inf`.
- String representations of type Float*, including `NaN` and `Inf` (case-insensitive).

Unsupported arguments:
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat64('0xc0fe');`.

See also:
- [`toFloat64OrZero`](#toFloat64OrZero).
- [`toFloat64OrNull`](#toFloat64OrNull).
- [`toFloat64OrDefault`](#toFloat64OrDefault).
    )";
    FunctionDocumentation::Syntax toFloat64_syntax = "toFloat64(expr)";
    FunctionDocumentation::Arguments toFloat64_arguments = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}}
    };
    FunctionDocumentation::ReturnedValue toFloat64_returned_value = {"Returns a 64-bit floating point value.", {"Float64"}};
    FunctionDocumentation::Examples toFloat64_examples = {
    {
        "Usage example",
        R"(
SELECT
    toFloat64(42.7),
    toFloat64('42.7'),
    toFloat64('NaN')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toFloat64(42.7):   42.7
toFloat64('42.7'): 42.7
toFloat64('NaN'):  nan
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toFloat64_introduced_in = {1, 1};
    FunctionDocumentation::Category toFloat64_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toFloat64_documentation = {toFloat64_description, toFloat64_syntax, toFloat64_arguments, toFloat64_returned_value, toFloat64_examples, toFloat64_introduced_in, toFloat64_category};

    factory.registerFunction<detail::FunctionToFloat64>(toFloat64_documentation);

    /// toDecimal64 documentation
    FunctionDocumentation::Description description_toDecimal64 = R"(
Converts an input value to a value of type [`Decimal(18, S)`](../data-types/decimal.md) with scale of `S`.
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments:
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal64('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal64`:`(-1*10^(18 - S), 1*10^(18 - S))`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an exception.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal64(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal64('1.15', 2) = 1.15`
:::
    )";
    FunctionDocumentation::Syntax syntax_toDecimal64 = "toDecimal64(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal64 = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 18, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal64 = {"Returns a decimal value.", {"Decimal(18, S)"}};
    FunctionDocumentation::Examples examples_toDecimal64 = {
    {
        "Usage example",
        R"(
SELECT
    toDecimal64(2, 1) AS a, toTypeName(a) AS type_a,
    toDecimal64(4.2, 2) AS b, toTypeName(b) AS type_b,
    toDecimal64('4.2', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical
        )",
        R"(
Row 1:
──────
a:      2.0
type_a: Decimal(18, 1)
b:      4.20
type_b: Decimal(18, 2)
c:      4.200
type_c: Decimal(18, 3)
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal64 = {18, 12};
    FunctionDocumentation::Category category_toDecimal64 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal64 = {description_toDecimal64, syntax_toDecimal64, arguments_toDecimal64, returned_value_toDecimal64, examples_toDecimal64, introduced_in_toDecimal64, category_toDecimal64};

    /// toDecimal32 documentation
    FunctionDocumentation::Description description_toDecimal32 = R"(
Converts an input value to a value of type [`Decimal(9, S)`](../data-types/decimal.md) with scale of `S`. Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments:
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal32('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal32`:`(-1*10^(9 - S), 1*10^(9 - S))`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an exception.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal32(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal32('1.15', 2) = 1.15`
:::
    )";
    FunctionDocumentation::Syntax syntax_toDecimal32 = "toDecimal32(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal32 = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 9, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal32 = {"Returns a value of type `Decimal(9, S)`", {"Decimal32(S)"}};
    FunctionDocumentation::Examples examples_toDecimal32 = {
    {
        "Usage example",
        R"(
SELECT
    toDecimal32(2, 1) AS a, toTypeName(a) AS type_a,
    toDecimal32(4.2, 2) AS b, toTypeName(b) AS type_b,
    toDecimal32('4.2', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical
        )",
        R"(
Row 1:
──────
a:      2
type_a: Decimal(9, 1)
b:      4.2
type_b: Decimal(9, 2)
c:      4.2
type_c: Decimal(9, 3)
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 12};
    FunctionDocumentation::Category category_toDecimal32 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal32 = {description_toDecimal32, syntax_toDecimal32, arguments_toDecimal32, returned_value_toDecimal32, examples_toDecimal32, introduced_in, category_toDecimal32};

    /// toDecimal128 documentation
    FunctionDocumentation::Description description_toDecimal128 = R"(
Converts an input value to a value of type [`Decimal(38, S)`](../data-types/decimal.md) with scale of `S`.
Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments:
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal128('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal128`:`(-1*10^(38 - S), 1*10^(38 - S))`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an exception.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal128(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal128('1.15', 2) = 1.15`
:::
    )";
    FunctionDocumentation::Syntax syntax_toDecimal128 = "toDecimal128(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal128 = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 38, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal128 = {"Returns a value of type `Decimal(38, S)`", {"Decimal128(S)"}};
    FunctionDocumentation::Examples examples_toDecimal128 = {
    {
        "Usage example",
        R"(
SELECT
    toDecimal128(99, 1) AS a, toTypeName(a) AS type_a,
    toDecimal128(99.67, 2) AS b, toTypeName(b) AS type_b,
    toDecimal128('99.67', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical
        )",
        R"(
Row 1:
──────
a:      99
type_a: Decimal(38, 1)
b:      99.67
type_b: Decimal(38, 2)
c:      99.67
type_c: Decimal(38, 3)
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal128 = {18, 12};
    FunctionDocumentation::Category category_toDecimal128 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal128 = {description_toDecimal128, syntax_toDecimal128, arguments_toDecimal128, returned_value_toDecimal128, examples_toDecimal128, introduced_in_toDecimal128, category_toDecimal128};

    /// toDecimal256 documentation
    FunctionDocumentation::Description description_toDecimal256 = R"(
Converts an input value to a value of type [`Decimal(76, S)`](../data-types/decimal.md) with scale of `S`. Throws an exception in case of an error.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments:
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal256('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal256`:`(-1*10^(76 - S), 1*10^(76 - S))`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an exception.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal256(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal256('1.15', 2) = 1.15`
:::
    )";
    FunctionDocumentation::Syntax syntax_toDecimal256 = "toDecimal256(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal256 = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 76, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal256 = {"Returns a value of type `Decimal(76, S)`.", {"Decimal256(S)"}};
    FunctionDocumentation::Examples examples_toDecimal256 = {
    {
        "Usage example",
        R"(
SELECT
    toDecimal256(99, 1) AS a, toTypeName(a) AS type_a,
    toDecimal256(99.67, 2) AS b, toTypeName(b) AS type_b,
    toDecimal256('99.67', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical
        )",
        R"(
Row 1:
──────
a:      99
type_a: Decimal(76, 1)
b:      99.67
type_b: Decimal(76, 2)
c:      99.67
type_c: Decimal(76, 3)
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal256 = {20, 8};
    FunctionDocumentation::Category category_toDecimal256 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal256 = {description_toDecimal256, syntax_toDecimal256, arguments_toDecimal256, returned_value_toDecimal256, examples_toDecimal256, introduced_in_toDecimal256, category_toDecimal256};

    factory.registerFunction<detail::FunctionToDecimal32>(documentation_toDecimal32);
    factory.registerFunction<detail::FunctionToDecimal64>(documentation_toDecimal64);
    factory.registerFunction<detail::FunctionToDecimal128>(documentation_toDecimal128);
    factory.registerFunction<detail::FunctionToDecimal256>(documentation_toDecimal256);

    /// toDate documentation
    FunctionDocumentation::Description description_toDate = R"(
Converts an input value to type [`Date`](/sql-reference/data-types/date).
Supports conversion from String, FixedString, DateTime, or numeric types.
    )";
    FunctionDocumentation::Syntax syntax_toDate = "toDate(x)";
    FunctionDocumentation::Arguments arguments_toDate = {
        {"x", "Input value to convert.", {"String", "FixedString", "DateTime", "(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDate = {"Returns the converted input value.", {"Date"}};
    FunctionDocumentation::Examples examples_toDate = {
    {
        "String to Date conversion",
        R"(
SELECT toDate('2025-04-15')
        )",
        R"(
2025-04-15
        )"
    },
    {
        "DateTime to Date conversion",
        R"(
SELECT toDate(toDateTime('2025-04-15 10:30:00'))
        )",
        R"(
2025-04-15
        )"
    },
    {
        "Integer to Date conversion",
        R"(
SELECT toDate(20297)
        )",
        R"(
2025-07-28
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDate = {1, 1};
    FunctionDocumentation::Category category_toDate = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDate = {description_toDate, syntax_toDate, arguments_toDate, returned_value_toDate, examples_toDate, introduced_in_toDate, category_toDate};

    factory.registerFunction<detail::FunctionToDate>(documentation_toDate);

    /// MySQL compatibility alias. Cannot be registered as alias,
    /// because we don't want it to be normalized to toDate in queries,
    /// otherwise CREATE DICTIONARY query breaks.
    factory.registerFunction("DATE", &detail::FunctionToDate::create, {}, FunctionFactory::Case::Insensitive);

    /// toDate32 documentation
    FunctionDocumentation::Description description_toDate32 = R"(
Converts the argument to the [Date32](../data-types/date32.md) data type.
If the value is outside the range, `toDate32` returns the border values supported by [Date32](../data-types/date32.md).
If the argument is of type [`Date`](../data-types/date.md), it's bounds are taken into account.
    )";
    FunctionDocumentation::Syntax syntax_toDate32 = "toDate32(expr)";
    FunctionDocumentation::Arguments arguments_toDate32 = {
        {"expr", "The value to convert.", {"String", "UInt32", "Date"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDate32 = {"Returns a calendar date.", {"Date32"}};
    FunctionDocumentation::Examples examples_toDate32 = {
    {
        "Within range",
        R"(
SELECT toDate32('2025-01-01') AS value, toTypeName(value)
FORMAT Vertical
        )",
        R"(
Row 1:
──────
value:           2025-01-01
toTypeName(value): Date32
        )"
    },
    {
        "Outside range",
        R"(
SELECT toDate32('1899-01-01') AS value, toTypeName(value)
FORMAT Vertical
        )",
        R"(
Row 1:
──────
value:           1900-01-01
toTypeName(value): Date32
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDate32 = {21, 9};
    FunctionDocumentation::Category category_toDate32 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDate32 = {description_toDate32, syntax_toDate32, arguments_toDate32, returned_value_toDate32, examples_toDate32, introduced_in_toDate32, category_toDate32};

    factory.registerFunction<detail::FunctionToDate32>(documentation_toDate32);

    /// toTime documentation
    FunctionDocumentation::Description description_toTime = R"(
Converts an input value to type [Time](/sql-reference/data-types/time).
Supports conversion from String, FixedString, DateTime, or numeric types representing seconds since midnight.
    )";
    FunctionDocumentation::Syntax syntax_toTime = "toTime(x)";
    FunctionDocumentation::Arguments arguments_toTime = {
        {"x", "Input value to convert.", {"String", "FixedString", "DateTime", "(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toTime = {"Returns the converted value.", {"Time"}};
    FunctionDocumentation::Examples examples_toTime = {
    {
        "String to Time conversion",
        R"(
SELECT toTime('14:30:25')
        )",
        R"(
14:30:25
        )"
    },
    {
        "DateTime to Time conversion",
        R"(
SELECT toTime(toDateTime('2025-04-15 14:30:25'))
        )",
        R"(
14:30:25
        )"
    },
    {
        "Integer to Time conversion",
        R"(
SELECT toTime(52225)
        )",
        R"(
14:30:25
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toTime = {1, 1};
    FunctionDocumentation::Category category_toTime = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toTime = {description_toTime, syntax_toTime, arguments_toTime, returned_value_toTime, examples_toTime, introduced_in_toTime, category_toTime};

    factory.registerFunction<detail::FunctionToTime>(documentation_toTime);

    FunctionDocumentation::Description description_toTime64 = R"(
Converts an input value to type [Time64](/sql-reference/data-types/time64).
Supports conversion from String, FixedString, DateTime64, or numeric types representing microseconds since midnight.
Provides microsecond precision for time values.
    )";
    FunctionDocumentation::Syntax syntax_toTime64 = "toTime64(x)";
    FunctionDocumentation::Arguments arguments_toTime64 = {
        {"x", "Input value to convert.", {"String", "FixedString", "DateTime64", "(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toTime64 = {"Returns the converted input value with microsecond precision.", {"Time64(6)"}};
    FunctionDocumentation::Examples examples_toTime64 = {
    {
        "String to Time64 conversion",
        R"(
SELECT toTime64('14:30:25.123456')
        )",
        R"(
14:30:25.123456
        )"
    },
    {
        "DateTime64 to Time64 conversion",
        R"(
SELECT toTime64(toDateTime64('2025-04-15 14:30:25.123456', 6))
        )",
        R"(
14:30:25.123456
        )"
    },
    {
        "Integer to Time64 conversion",
        R"(
SELECT toTime64(52225123456)
        )",
        R"(
14:30:25.123456
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toTime64 = {25, 6};
    FunctionDocumentation::Category category_toTime64 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toTime64 = {description_toTime64, syntax_toTime64, arguments_toTime64, returned_value_toTime64, examples_toTime64, introduced_in_toTime64, category_toTime64};

    factory.registerFunction<detail::FunctionToTime64>(documentation_toTime64);

    /// toDateTime documentation
    FunctionDocumentation::Description description_toDateTime = R"(
Converts an input value to type [DateTime](../data-types/datetime.md).

:::note
If `expr` is a number, it is interpreted as the number of seconds since the beginning of the Unix Epoch (as Unix timestamp).
If `expr` is a [String](../data-types/string.md), it may be interpreted as a Unix timestamp or as a string representation of date / date with time.
Thus, parsing of short numbers' string representations (up to 4 digits) is explicitly disabled due to ambiguity, e.g. a string `'1999'` may be both a year (an incomplete string representation of Date / DateTime) or a unix timestamp. Longer numeric strings are allowed.
:::
    )";
    FunctionDocumentation::Syntax syntax_toDateTime = "toDateTime(expr[, time_zone])";
    FunctionDocumentation::Arguments arguments_toDateTime = {
        {"expr", "The value.", {"String", "Int", "Date", "DateTime"}},
        {"time_zone", "Time zone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateTime = {"Returns a date time.", {"DateTime"}};
    FunctionDocumentation::Examples examples_toDateTime = {
    {
        "Usage example",
        R"(
SELECT toDateTime('2025-01-01 00:00:00'), toDateTime(1735689600, 'UTC')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toDateTime('2025-01-01 00:00:00'): 2025-01-01 00:00:00
toDateTime(1735689600, 'UTC'):     2025-01-01 00:00:00
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateTime = {1, 1};
    FunctionDocumentation::Category category_toDateTime = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateTime = {description_toDateTime, syntax_toDateTime, arguments_toDateTime, returned_value_toDateTime, examples_toDateTime, introduced_in_toDateTime, category_toDateTime};

    factory.registerFunction<detail::FunctionToDateTime>(documentation_toDateTime);

    /// toDateTime32 documentation
    FunctionDocumentation::Description description_toDateTime32 = R"(
Converts an input value to type `DateTime`.
Supports conversion from `String`, `FixedString`, `Date`, `Date32`, `DateTime`, or numeric types (`(U)Int*`, `Float*`, `Decimal`).
DateTime32 provides extended range compared to `DateTime`, supporting dates from `1900-01-01` to `2299-12-31`.
    )";
    FunctionDocumentation::Syntax syntax_toDateTime32 = "toDateTime32(x[, timezone])";
    FunctionDocumentation::Arguments arguments_toDateTime32 = {
        {"x", "Input value to convert.", {"String", "FixedString", "UInt*", "Float*", "Date", "DateTime", "DateTime64"}},
        {"timezone", "Optional. Timezone for the returned `DateTime` value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateTime32 = {"Returns the converted input value.", {"DateTime"}};
    FunctionDocumentation::Examples examples_toDateTime32 = {
    {
        "The value is within the range",
        R"(
SELECT toDateTime64('2025-01-01 00:00:00.000', 3) AS value, toTypeName(value);
        )",
        R"(
┌───────────────────value─┬─toTypeName(toDateTime64('20255-01-01 00:00:00.000', 3))─┐
│ 2025-01-01 00:00:00.000 │ DateTime64(3)                                          │
└─────────────────────────┴────────────────────────────────────────────────────────┘
        )"
    },
    {
        "As a decimal with precision",
        R"(
SELECT toDateTime64(1735689600.000, 3) AS value, toTypeName(value);
-- without the decimal point the value is still treated as Unix Timestamp in seconds
SELECT toDateTime64(1546300800000, 3) AS value, toTypeName(value);
        )",
        R"(
┌───────────────────value─┬─toTypeName(toDateTime64(1735689600.000, 3))─┐
│ 2025-01-01 00:00:00.000 │ DateTime64(3)                            │
└─────────────────────────┴──────────────────────────────────────────┘
┌───────────────────value─┬─toTypeName(toDateTime64(1546300800000, 3))─┐
│ 2282-12-31 00:00:00.000 │ DateTime64(3)                              │
└─────────────────────────┴────────────────────────────────────────────┘
        )"
    },
    {
        "With a timezone",
        R"(
SELECT toDateTime64('2025-01-01 00:00:00', 3, 'Asia/Istanbul') AS value, toTypeName(value);
        )",
        R"(
┌───────────────────value─┬─toTypeName(toDateTime64('2025-01-01 00:00:00', 3, 'Asia/Istanbul'))─┐
│ 2025-01-01 00:00:00.000 │ DateTime64(3, 'Asia/Istanbul')                                      │
└─────────────────────────┴─────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateTime32 = {20, 9};
    FunctionDocumentation::Category category_toDateTime32 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateTime32 = {description_toDateTime32, syntax_toDateTime32, arguments_toDateTime32, returned_value_toDateTime32, examples_toDateTime32, introduced_in_toDateTime32, category_toDateTime32};

    factory.registerFunction<detail::FunctionToDateTime32>(documentation_toDateTime32);

    /// toDateTime64 documentation
    FunctionDocumentation::Description description_toDateTime64 = R"(
Converts an input value to a value of type [`DateTime64`](../data-types/datetime64.md).
    )";
    FunctionDocumentation::Syntax syntax_toDateTime64 = "toDateTime64(expr, scale[, timezone])";
    FunctionDocumentation::Arguments arguments_toDateTime64 = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"scale", "Tick size (precision): 10^(-scale) seconds.", {"UInt8"}},
        {"timezone", "Optional. Time zone for the specified `DateTime64` object.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateTime64 = {"Returns a calendar date and time of day, with sub-second precision.", {"DateTime64"}};
    FunctionDocumentation::Examples examples_toDateTime64 = {
    {
        "The value is within the range",
        R"(
SELECT toDateTime64('2025-01-01 00:00:00.000', 3) AS value, toTypeName(value);
        )",
        R"(
┌───────────────────value─┬─toTypeName(toDateTime64('2025-01-01 00:00:00.000', 3))─┐
│ 2025-01-01 00:00:00.000 │ DateTime64(3)                                          │
└─────────────────────────┴────────────────────────────────────────────────────────┘
        )"
    },
    {
        "As decimal with precision",
        R"(
SELECT toDateTime64(1546300800.000, 3) AS value, toTypeName(value);
-- Without the decimal point the value is still treated as Unix Timestamp in seconds
SELECT toDateTime64(1546300800000, 3) AS value, toTypeName(value);
        )",
        R"(
┌───────────────────value─┬─toTypeName(toDateTime64(1546300800000, 3))─┐
│ 2282-12-31 00:00:00.000 │ DateTime64(3)                              │
└─────────────────────────┴────────────────────────────────────────────┘
        )"
    },
    {
        "With timezone",
        R"(
SELECT toDateTime64('2025-01-01 00:00:00', 3, 'Asia/Istanbul') AS value, toTypeName(value);
        )",
        R"(
┌───────────────────value─┬─toTypeName(toDateTime64('2025-01-01 00:00:00', 3, 'Asia/Istanbul'))─┐
│ 2025-01-01 00:00:00.000 │ DateTime64(3, 'Asia/Istanbul')                                      │
└─────────────────────────┴─────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateTime64 = {20, 1};
    FunctionDocumentation::Category category_toDateTime64 = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateTime64 = {description_toDateTime64, syntax_toDateTime64, arguments_toDateTime64, returned_value_toDateTime64, examples_toDateTime64, introduced_in_toDateTime64, category_toDateTime64};

    factory.registerFunction<detail::FunctionToDateTime64>(documentation_toDateTime64);

    /// toUUID documentation
    FunctionDocumentation::Description description_toUUID = R"(
Converts a String value to a UUID value.
    )";
    FunctionDocumentation::Syntax syntax_toUUID = "toUUID(string)";
    FunctionDocumentation::Arguments arguments_toUUID = {
        {"string", "UUID as a string.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUUID = {"Returns a UUID from the string representation of the UUID.", {"UUID"}};
    FunctionDocumentation::Examples examples_toUUID = {
    {
        "Usage example",
        R"(
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
        )",
        R"(
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUUID = {1, 1};
    FunctionDocumentation::Category category_toUUID = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUUID = {description_toUUID, syntax_toUUID, arguments_toUUID, returned_value_toUUID, examples_toUUID, introduced_in_toUUID, category_toUUID};

    factory.registerFunction<detail::FunctionToUUID>(documentation_toUUID);

    /// toIPv4 documentation
    FunctionDocumentation::Description description_toIPv4 = R"(
Converts a string or a UInt32 form of IPv4 address to type IPv4.
It is similar to [`IPv4StringToNum`](/sql-reference/functions/ip-address-functions#IPv4StringToNum) and [`IPv4NumToString`](/sql-reference/functions/ip-address-functions#IPv4NumToString) functions but it supports both string and unsigned integer data types as input arguments.
)";
    FunctionDocumentation::Syntax syntax_toIPv4 = "toIPv4(x)";
    FunctionDocumentation::Arguments arguments_toIPv4 = {
        {"x", "An IPv4 address", {"String", "UInt8/16/32"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIPv4 = {"Returns an IPv4 address.", {"IPv4"}};
    FunctionDocumentation::Examples examples_toIPv4 = {
    {
        "Usage example",
        R"(
SELECT toIPv4('171.225.130.45');
        )",
        R"(
┌─toIPv4('171.225.130.45')─┐
│ 171.225.130.45           │
└──────────────────────────┘
        )"
    },
    {
        "Comparison with IPv4StringToNum and IPv4NumToString functions.",
        R"(
WITH
    '171.225.130.45' AS IPv4_string
SELECT
    hex(IPv4StringToNum(IPv4_string)),
    hex(toIPv4(IPv4_string))
        )",
        R"(
┌─hex(IPv4StringToNum(IPv4_string))─┬─hex(toIPv4(IPv4_string))─┐
│ ABE1822D                          │ ABE1822D                 │
└───────────────────────────────────┴──────────────────────────┘
        )"
    },
    {
        "Conversion from an integer",
        R"(
SELECT toIPv4(2130706433);
        )",
        R"(
┌─toIPv4(2130706433)─┐
│ 127.0.0.1          │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIPv4 = {20, 1};
    FunctionDocumentation::Category category_toIPv4 = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_toIPv4 = {description_toIPv4, syntax_toIPv4, arguments_toIPv4, returned_value_toIPv4, examples_toIPv4, introduced_in_toIPv4, category_toIPv4};

    factory.registerFunction<detail::FunctionToIPv4>(documentation_toIPv4);

    /// toIPv6 documentation
    FunctionDocumentation::Description description_toIPv6 = R"(
onverts a string or a `UInt128` form of IPv6 address to [`IPv6`](../data-types/ipv6.md) type.
For strings, if the IPv6 address has an invalid format, returns an empty value.
Similar to [`IPv6StringToNum`](/sql-reference/functions/ip-address-functions#IPv6StringToNum) and [`IPv6NumToString`](/sql-reference/functions/ip-address-functions#IPv6NumToString) functions, which convert IPv6 address to and from binary format (i.e. `FixedString(16)`).

If the input string contains a valid IPv4 address, then the IPv6 equivalent of the IPv4 address is returned.
)";
    FunctionDocumentation::Syntax syntax_toIPv6 = "toIPv6(x)";
    FunctionDocumentation::Arguments arguments_toIPv6 = {
        {"x", "An IP address.", {"String", "UInt128"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIPv6 = {"Returns an IPv6 address.", {"IPv6"}};
    FunctionDocumentation::Examples examples_toIPv6 = {
    {
        "Usage example",
        R"(
WITH '2001:438:ffff::407d:1bc1' AS IPv6_string
SELECT
    hex(IPv6StringToNum(IPv6_string)),
    hex(toIPv6(IPv6_string));
        )",
        R"(
┌─hex(IPv6StringToNum(IPv6_string))─┬─hex(toIPv6(IPv6_string))─────────┐
│ 20010438FFFF000000000000407D1BC1  │ 20010438FFFF000000000000407D1BC1 │
└───────────────────────────────────┴──────────────────────────────────┘
        )"
    },
    {
        "IPv4-to-IPv6 mapping",
        R"(
SELECT toIPv6('127.0.0.1');
        )",
        R"(
┌─toIPv6('127.0.0.1')─┐
│ ::ffff:127.0.0.1    │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIPv6 = {20, 1};
    FunctionDocumentation::Category category_toIPv6 = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_toIPv6 = {description_toIPv6, syntax_toIPv6, arguments_toIPv6, returned_value_toIPv6, examples_toIPv6, introduced_in_toIPv6, category_toIPv6};

    factory.registerFunction<detail::FunctionToIPv6>(documentation_toIPv6);

    FunctionDocumentation::Description toString_description = R"(
Converts values to their string representation.
For DateTime arguments, the function can take a second String argument containing the name of the time zone.
)";
    FunctionDocumentation::Syntax toString_syntax = "toString(value[, timezone])";
    FunctionDocumentation::Arguments toString_arguments = {
        {"value", "Value to convert to string.", {"Any"}},
        {"timezone", "Optional. Timezone name for DateTime conversion.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue toString_returned_value = {"Returns a string representation of the input value.", {"String"}};
    FunctionDocumentation::Examples toString_examples = {
    {
        "Usage example",
        R"(
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
        )",
        R"(
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toString_introduced_in = {1, 1};
    FunctionDocumentation::Category toString_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toString_documentation = {toString_description, toString_syntax, toString_arguments, toString_returned_value, toString_examples, toString_introduced_in, toString_category};

    factory.registerFunction<detail::FunctionToString>(toString_documentation);

    /// toUnixTimestamp documentation
    FunctionDocumentation::Description description_to_unix_timestamp = R"(
Converts a `String`, `Date`, or `DateTime` to a Unix timestamp (seconds since `1970-01-01 00:00:00 UTC`) as `UInt32`.
    )";
    FunctionDocumentation::Syntax syntax_to_unix_timestamp = R"(
toUnixTimestamp(date, [timezone])
    )";
    FunctionDocumentation::Arguments arguments_to_unix_timestamp = {
        {"date", "Value to convert.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"timezone", "Optional. Timezone to use for conversion. If not specified, the server's timezone is used.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_unix_timestamp = {"Returns the Unix timestamp.", {"UInt32"}};
    FunctionDocumentation::Examples examples_to_unix_timestamp = {
    {
        "Usage example",
        R"(
SELECT
'2017-11-05 08:07:47' AS dt_str,
toUnixTimestamp(dt_str) AS from_str,
toUnixTimestamp(dt_str, 'Asia/Tokyo') AS from_str_tokyo,
toUnixTimestamp(toDateTime(dt_str)) AS from_datetime,
toUnixTimestamp(toDateTime64(dt_str, 0)) AS from_datetime64,
toUnixTimestamp(toDate(dt_str)) AS from_date,
toUnixTimestamp(toDate32(dt_str)) AS from_date32
FORMAT Vertical;
        )",
        R"(
Row 1:
──────
dt_str:          2017-11-05 08:07:47
from_str:        1509869267
from_str_tokyo:  1509836867
from_datetime:   1509869267
from_datetime64: 1509869267
from_date:       1509840000
from_date32:     1509840000
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_unix_timestamp = {1, 1};
    FunctionDocumentation::Category category_to_unix_timestamp = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_unix_timestamp = {description_to_unix_timestamp, syntax_to_unix_timestamp, arguments_to_unix_timestamp, returned_value_to_unix_timestamp, examples_to_unix_timestamp, introduced_in_to_unix_timestamp, category_to_unix_timestamp};

    factory.registerFunction<detail::FunctionToUnixTimestamp>(documentation_to_unix_timestamp);

    /// toUInt8OrZero documentation
    FunctionDocumentation::Description description_toUInt8OrZero = R"(
Like [`toUInt8`](#toUInt8), this function converts an input value to a value of type [`UInt8`](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of ordinary Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt8OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt8`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt8`](#toUInt8).
- [`toUInt8OrNull`](#toUInt8OrNull).
- [`toUInt8OrDefault`](#toUInt8OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt8OrZero = "toUInt8OrZero(x)";
    FunctionDocumentation::Arguments arguments_toUInt8OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt8OrZero = {"Returns a value of type UInt8, otherwise `0` if the conversion is unsuccessful.", {"UInt8"}};
    FunctionDocumentation::Examples examples_toUInt8OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toUInt8OrZero('-8'),
    toUInt8OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt8OrZero('-8'):  0
toUInt8OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt8OrZero = {1, 1};
    FunctionDocumentation::Category category_toUInt8OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt8OrZero = {description_toUInt8OrZero, syntax_toUInt8OrZero, arguments_toUInt8OrZero, returned_value_toUInt8OrZero, examples_toUInt8OrZero, introduced_in_toUInt8OrZero, category_toUInt8OrZero};

    factory.registerFunction<detail::FunctionToUInt8OrZero>(documentation_toUInt8OrZero);

    /// toUInt16OrZero documentation
    FunctionDocumentation::Description description_toUInt16OrZero = R"(
Like [`toUInt16`](#toUInt16), this function converts an input value to a value of type [`UInt16`](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt16OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt16`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt16`](#toUInt16).
- [`toUInt16OrNull`](#toUInt16OrNull).
- [`toUInt16OrDefault`](#toUInt16OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt16OrZero = "toUInt16OrZero(x)";
    FunctionDocumentation::Arguments arguments_toUInt16OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt16OrZero = {"Returns a value of type UInt16, otherwise `0` if the conversion is unsuccessful.", {"UInt16"}};
    FunctionDocumentation::Examples examples_toUInt16OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toUInt16OrZero('16'),
    toUInt16OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt16OrZero('16'):  16
toUInt16OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt16OrZero = {1, 1};
    FunctionDocumentation::Category category_toUInt16OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt16OrZero = {description_toUInt16OrZero, syntax_toUInt16OrZero, arguments_toUInt16OrZero, returned_value_toUInt16OrZero, examples_toUInt16OrZero, introduced_in_toUInt16OrZero, category_toUInt16OrZero};

    factory.registerFunction<detail::FunctionToUInt16OrZero>(documentation_toUInt16OrZero);

    /// toUInt32OrZero documentation
    FunctionDocumentation::Description description_toUInt32OrZero = R"(
Like [`toUInt32`](#toUInt32), this function converts an input value to a value of type [`UInt32`](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt32OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt32`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt32`](#toUInt32).
- [`toUInt32OrNull`](#toUInt32OrNull).
- [`toUInt32OrDefault`](#toUInt32OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt32OrZero = "toUInt32OrZero(x)";
    FunctionDocumentation::Arguments arguments_toUInt32OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt32OrZero = {"Returns a value of type UInt32, otherwise `0` if the conversion is unsuccessful.", {"UInt32"}};
    FunctionDocumentation::Examples examples_toUInt32OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toUInt32OrZero('32'),
    toUInt32OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt32OrZero('32'):  32
toUInt32OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt32OrZero = {1, 1};
    FunctionDocumentation::Category category_toUInt32OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt32OrZero = {description_toUInt32OrZero, syntax_toUInt32OrZero, arguments_toUInt32OrZero, returned_value_toUInt32OrZero, examples_toUInt32OrZero, introduced_in_toUInt32OrZero, category_toUInt32OrZero};

    factory.registerFunction<detail::FunctionToUInt32OrZero>(documentation_toUInt32OrZero);

    /// toUInt64OrZero documentation
    FunctionDocumentation::Description description_toUInt64OrZero = R"(
Like [`toUInt64`](#toUInt64), this function converts an input value to a value of type [`UInt64`](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `0`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt64OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt64`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt64`](#toUInt64).
- [`toUInt64OrNull`](#toUInt64OrNull).
- [`toUInt64OrDefault`](#toUInt64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt64OrZero = "toUInt64OrZero(x)";
    FunctionDocumentation::Arguments arguments_toUInt64OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt64OrZero = {"Returns a value of type UInt64, otherwise `0` if the conversion is unsuccessful.", {"UInt64"}};
    FunctionDocumentation::Examples examples_toUInt64OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toUInt64OrZero('64'),
    toUInt64OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt64OrZero('64'):  64
toUInt64OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt64OrZero = {1, 1};
    FunctionDocumentation::Category category_toUInt64OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt64OrZero = {description_toUInt64OrZero, syntax_toUInt64OrZero, arguments_toUInt64OrZero, returned_value_toUInt64OrZero, examples_toUInt64OrZero, introduced_in_toUInt64OrZero, category_toUInt64OrZero};

    factory.registerFunction<detail::FunctionToUInt64OrZero>(documentation_toUInt64OrZero);

    /// toUInt128OrZero documentation
    FunctionDocumentation::Description description_toUInt128OrZero = R"(
Like [`toUInt128`](#toUInt128), this function converts an input value to a value of type [`UInt128`](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `0`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt128OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt128`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt128`](#toUInt128).
- [`toUInt128OrNull`](#toUInt128OrNull).
- [`toUInt128OrDefault`](#toUInt128OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt128OrZero = "toUInt128OrZero(x)";
    FunctionDocumentation::Arguments arguments_toUInt128OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt128OrZero = {"Returns a value of type UInt128, otherwise `0` if the conversion is unsuccessful.", {"UInt128"}};
    FunctionDocumentation::Examples examples_toUInt128OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toUInt128OrZero('128'),
    toUInt128OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt128OrZero('128'): 128
toUInt128OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt128OrZero = {1, 1};
    FunctionDocumentation::Category category_toUInt128OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt128OrZero = {description_toUInt128OrZero, syntax_toUInt128OrZero, arguments_toUInt128OrZero, returned_value_toUInt128OrZero, examples_toUInt128OrZero, introduced_in_toUInt128OrZero, category_toUInt128OrZero};

    factory.registerFunction<detail::FunctionToUInt128OrZero>(documentation_toUInt128OrZero);

    /// toUInt256OrZero documentation
    FunctionDocumentation::Description description_toUInt256OrZero = R"(
Like [`toUInt256`](#toUInt256), this function converts an input value to a value of type [`UInt256`](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `0`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt256OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt256`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt256`](#toUInt256).
- [`toUInt256OrNull`](#toUInt256OrNull).
- [`toUInt256OrDefault`](#toUInt256OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt256OrZero = "toUInt256OrZero(x)";
    FunctionDocumentation::Arguments arguments_toUInt256OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt256OrZero = {"Returns a value of type UInt256, otherwise `0` if the conversion is unsuccessful.", {"UInt256"}};
    FunctionDocumentation::Examples examples_toUInt256OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toUInt256OrZero('256'),
    toUInt256OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt256OrZero('256'): 256
toUInt256OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt256OrZero = {20, 8};
    FunctionDocumentation::Category category_toUInt256OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt256OrZero = {description_toUInt256OrZero, syntax_toUInt256OrZero, arguments_toUInt256OrZero, returned_value_toUInt256OrZero, examples_toUInt256OrZero, introduced_in_toUInt256OrZero, category_toUInt256OrZero};

    factory.registerFunction<detail::FunctionToUInt256OrZero>(documentation_toUInt256OrZero);

    /// toInt8OrZero documentation
    FunctionDocumentation::Description description_toInt8OrZero = R"(
Like [`toInt8`](#toInt8), this function converts an input value to a value of type [Int8](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `0`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt8OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt8`](#toInt8).
- [`toInt8OrNull`](#toInt8OrNull).
- [`toInt8OrDefault`](#toInt8OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt8OrZero = "toInt8OrZero(x)";
    FunctionDocumentation::Arguments arguments_toInt8OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt8OrZero = {"Returns a value of type Int8, otherwise `0` if the conversion is unsuccessful.", {"Int8"}};
    FunctionDocumentation::Examples examples_toInt8OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toInt8OrZero('8'),
    toInt8OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt8OrZero('8'): 8
toInt8OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt8OrZero = {1, 1};
    FunctionDocumentation::Category category_toInt8OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt8OrZero = {description_toInt8OrZero, syntax_toInt8OrZero, arguments_toInt8OrZero, returned_value_toInt8OrZero, examples_toInt8OrZero, introduced_in_toInt8OrZero, category_toInt8OrZero};

    factory.registerFunction<detail::FunctionToInt8OrZero>(documentation_toInt8OrZero);

    /// toInt16OrZero documentation
    FunctionDocumentation::Description description_toInt16OrZero = R"(
Like [`toInt16`](#toInt16), this function converts an input value to a value of type [Int16](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `0`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt16OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt16`](#toInt16).
- [`toInt16OrNull`](#toInt16OrNull).
- [`toInt16OrDefault`](#toInt16OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt16OrZero = "toInt16OrZero(x)";
    FunctionDocumentation::Arguments arguments_toInt16OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt16OrZero = {"Returns a value of type Int16, otherwise `0` if the conversion is unsuccessful.", {"Int16"}};
    FunctionDocumentation::Examples examples_toInt16OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toInt16OrZero('16'),
    toInt16OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt16OrZero('16'): 16
toInt16OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt16OrZero = {1, 1};
    FunctionDocumentation::Category category_toInt16OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt16OrZero = {description_toInt16OrZero, syntax_toInt16OrZero, arguments_toInt16OrZero, returned_value_toInt16OrZero, examples_toInt16OrZero, introduced_in_toInt16OrZero, category_toInt16OrZero};

    factory.registerFunction<detail::FunctionToInt16OrZero>(documentation_toInt16OrZero);

    /// toInt32OrZero documentation
    FunctionDocumentation::Description description_toInt32OrZero = R"(
Like [`toInt32`](#toInt32), this function converts an input value to a value of type [Int32](../data-types/int-uint.md) but returns `0` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `0`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt32OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int32](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt32`](#toInt32).
- [`toInt32OrNull`](#toInt32OrNull).
- [`toInt32OrDefault`](#toInt32OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt32OrZero = "toInt32OrZero(x)";
    FunctionDocumentation::Arguments arguments_toInt32OrZero = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt32OrZero = {"Returns a value of type Int32, otherwise `0` if the conversion is unsuccessful.", {"Int32"}};
    FunctionDocumentation::Examples examples_toInt32OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toInt32OrZero('32'),
    toInt32OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt32OrZero('32'): 32
toInt32OrZero('abc'): 0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt32OrZero = {1, 1};
    FunctionDocumentation::Category category_toInt32OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt32OrZero = {description_toInt32OrZero, syntax_toInt32OrZero, arguments_toInt32OrZero, returned_value_toInt32OrZero, examples_toInt32OrZero, introduced_in_toInt32OrZero, category_toInt32OrZero};

    factory.registerFunction<detail::FunctionToInt32OrZero>(documentation_toInt32OrZero);

    FunctionDocumentation::Description description_toInt64OrZero = R"(
Converts an input value to type [Int64](/sql-reference/data-types/int-uint) but returns `0` in case of an error.
Like [`toInt64`](#toint64) but returns `0` instead of throwing an exception.

See also:
- [`toInt64`](#toInt64).
- [`toInt64OrNull`](#toInt64OrNull).
- [`toInt64OrDefault`](#toInt64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt64OrZero = "toInt64OrZero(x)";
    FunctionDocumentation::Arguments arguments_toInt64OrZero = {
        {"x", "Input value to convert.", {"String", "FixedString", "Float*", "Decimal", "(U)Int*", "Date", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt64OrZero = {"Returns the converted input value, otherwise `0` if conversion fails.", {"Int64"}};
    FunctionDocumentation::Examples examples_toInt64OrZero = {
    {
        "Usage example",
        R"(
SELECT toInt64OrZero('123')
        )",
        R"(
123
        )"
    },
    {
        "Failed conversion returns zero",
        R"(
SELECT toInt64OrZero('abc')
        )",
        R"(
0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt64OrZero = {1, 1};
    FunctionDocumentation::Category category_toInt64OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt64OrZero = {description_toInt64OrZero, syntax_toInt64OrZero, arguments_toInt64OrZero, returned_value_toInt64OrZero, examples_toInt64OrZero, introduced_in_toInt64OrZero, category_toInt64OrZero};

    factory.registerFunction<detail::FunctionToInt64OrZero>(documentation_toInt64OrZero);

    FunctionDocumentation::Description description_toInt128OrZero = R"(
Converts an input value to type [Int128](/sql-reference/data-types/int-uint) but returns `0` in case of an error.
Like [`toInt128`](#toint128) but returns `0` instead of throwing an exception.

See also:
- [`toInt128`](#toInt128).
- [`toInt128OrNull`](#toInt128OrNull).
- [`toInt128OrDefault`](#toInt128OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt128OrZero = "toInt128OrZero(x)";
    FunctionDocumentation::Arguments arguments_toInt128OrZero = {
        {"x", "Input value to convert.", {"String", "FixedString", "Float*", "Decimal", "(U)Int*", "Date", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt128OrZero = {"Returns the converted input value, otherwise `0` if conversion fails.", {"Int128"}};
    FunctionDocumentation::Examples examples_toInt128OrZero = {
    {
        "Usage example",
        R"(
SELECT toInt128OrZero('123')
        )",
        R"(
123
        )"
    },
    {
        "Failed conversion returns zero",
        R"(
SELECT toInt128OrZero('abc')
        )",
        R"(
0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt128OrZero = {20, 8};
    FunctionDocumentation::Category category_toInt128OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt128OrZero = {description_toInt128OrZero, syntax_toInt128OrZero, arguments_toInt128OrZero, returned_value_toInt128OrZero, examples_toInt128OrZero, introduced_in_toInt128OrZero, category_toInt128OrZero};

    factory.registerFunction<detail::FunctionToInt128OrZero>(documentation_toInt128OrZero);

    FunctionDocumentation::Description description_toInt256OrZero = R"(
Converts an input value to type [Int256](/sql-reference/data-types/int-uint) but returns `0` in case of an error.
Like [`toInt256`](#toint256) but returns `0` instead of throwing an exception.

See also:
- [`toInt256`](#toInt256).
- [`toInt256OrNull`](#toInt256OrNull).
- [`toInt256OrDefault`](#toInt256OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt256OrZero = "toInt256OrZero(x)";
    FunctionDocumentation::Arguments arguments_toInt256OrZero = {
        {"x", "Input value to convert.", {"String", "FixedString", "Float*", "Decimal", "(U)Int*", "Date", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt256OrZero = {"Returns the converted input value, otherwise `0` if conversion fails.", {"Int256"}};
    FunctionDocumentation::Examples examples_toInt256OrZero = {
    {
        "Usage example",
        R"(
SELECT toInt256OrZero('123')
        )",
        R"(
123
        )"
    },
    {
        "Failed conversion returns zero",
        R"(
SELECT toInt256OrZero('abc')
        )",
        R"(
0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt256OrZero = {20, 8};
    FunctionDocumentation::Category category_toInt256OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt256OrZero = {description_toInt256OrZero, syntax_toInt256OrZero, arguments_toInt256OrZero, returned_value_toInt256OrZero, examples_toInt256OrZero, introduced_in_toInt256OrZero, category_toInt256OrZero};

    factory.registerFunction<detail::FunctionToInt256OrZero>(documentation_toInt256OrZero);

    /// toBFloat16OrZero documentation
    FunctionDocumentation::Description toBFloat16OrZero_description = R"(
Converts a String input value to a value of type BFloat16.
If the string does not represent a floating point value, the function returns zero.

Supported arguments:
- String representations of numeric values.

Unsupported arguments (return `0`):
- String representations of binary and hexadecimal values.
- Numeric values.

:::note
The function allows a silent loss of precision while converting from the string representation.
:::

See also:
- [`toBFloat16`](#toBFloat16).
- [`toBFloat16OrNull`](#toBFloat16OrNull).
    )";
    FunctionDocumentation::Syntax toBFloat16OrZero_syntax = "toBFloat16OrZero(x)";
    FunctionDocumentation::Arguments toBFloat16OrZero_arguments = {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue toBFloat16OrZero_returned_value = {"Returns a 16-bit brain-float value, otherwise `0`.", {"BFloat16"}};
    FunctionDocumentation::Examples toBFloat16OrZero_examples = {
    {
        "Usage example",
        R"(
SELECT toBFloat16OrZero('0x5E'), -- unsupported arguments
       toBFloat16OrZero('12.3'), -- typical use
       toBFloat16OrZero('12.3456789') -- silent loss of precision
        )",
        R"(
0
12.25
12.3125
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toBFloat16OrZero_introduced_in = {1, 1};
    FunctionDocumentation::Category toBFloat16OrZero_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toBFloat16OrZero_documentation = {toBFloat16OrZero_description, toBFloat16OrZero_syntax, toBFloat16OrZero_arguments, toBFloat16OrZero_returned_value, toBFloat16OrZero_examples, toBFloat16OrZero_introduced_in, toBFloat16OrZero_category};

    factory.registerFunction<detail::FunctionToBFloat16OrZero>(toBFloat16OrZero_documentation);

    /// toFloat32OrZero documentation
    FunctionDocumentation::Description description_toFloat32OrZero = R"(
Converts an input value to a value of type [Float32](../data-types/float.md) but returns `0` in case of an error.
Like [`toFloat32`](#tofloat32) but returns `0` instead of throwing an exception on conversion errors.

See also:
- [`toFloat32`](#toFloat32).
- [`toFloat32OrNull`](#toFloat32OrNull).
- [`toFloat32OrDefault`](#toFloat32OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toFloat32OrZero = "toFloat32OrZero(x)";
    FunctionDocumentation::Arguments arguments_toFloat32OrZero = {
        {"x", "A string representation of a number.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_toFloat32OrZero = {"Returns a 32-bit Float value if successful, otherwise `0`.", {"Float32"}};
    FunctionDocumentation::Examples examples_toFloat32OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toFloat32OrZero('42.7'),
    toFloat32OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toFloat32OrZero('42.7'): 42.7
toFloat32OrZero('abc'):  0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toFloat32OrZero = {1, 1};
    FunctionDocumentation::Category category_toFloat32OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toFloat32OrZero = {description_toFloat32OrZero, syntax_toFloat32OrZero, arguments_toFloat32OrZero, returned_value_toFloat32OrZero, examples_toFloat32OrZero, introduced_in_toFloat32OrZero, category_toFloat32OrZero};

    factory.registerFunction<detail::FunctionToFloat32OrZero>(documentation_toFloat32OrZero);

    /// toFloat64OrZero documentation
    FunctionDocumentation::Description description_toFloat64OrZero = R"(
Converts an input value to a value of type [Float64](../data-types/float.md) but returns `0` in case of an error.
Like [`toFloat64`](#toFloat64) but returns `0` instead of throwing an exception on conversion errors.

See also:
- [`toFloat64`](#toFloat64).
- [`toFloat64OrNull`](#toFloat64OrNull).
- [`toFloat64OrDefault`](#toFloat64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toFloat64OrZero = "toFloat64OrZero(x)";
    FunctionDocumentation::Arguments arguments_toFloat64OrZero = {
        {"x", "A string representation of a number.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_toFloat64OrZero = {"Returns a 64-bit Float value if successful, otherwise `0`.", {"Float64"}};
    FunctionDocumentation::Examples examples_toFloat64OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toFloat64OrZero('42.7'),
    toFloat64OrZero('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toFloat64OrZero('42.7'): 42.7
toFloat64OrZero('abc'):  0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toFloat64OrZero = {1, 1};
    FunctionDocumentation::Category category_toFloat64OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toFloat64OrZero = {description_toFloat64OrZero, syntax_toFloat64OrZero, arguments_toFloat64OrZero, returned_value_toFloat64OrZero, examples_toFloat64OrZero, introduced_in_toFloat64OrZero, category_toFloat64OrZero};

    factory.registerFunction<detail::FunctionToFloat64OrZero>(documentation_toFloat64OrZero);

    /// toDateOrZero documentation
    FunctionDocumentation::Description description_toDateOrZero = R"(
Converts an input value to a value of type [`Date`](../data-types/date.md) but returns the lower boundary of [`Date`](../data-types/date.md) if an invalid argument is received.
The same as [toDate](#todate) but returns lower boundary of [`Date`](../data-types/date.md) if an invalid argument is received.

See also:
- [`toDate`](#toDate)
- [`toDateOrNull`](#toDateOrNull)
- [`toDateOrDefault`](#toDateOrDefault)
    )";
    FunctionDocumentation::Syntax syntax_toDateOrZero = "toDateOrZero(x)";
    FunctionDocumentation::Arguments arguments_toDateOrZero = {
        {"x", "A string representation of a date.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateOrZero = {"Returns a Date value if successful, otherwise the lower boundary of Date (`1970-01-01`).", {"Date"}};
    FunctionDocumentation::Examples examples_toDateOrZero = {
    {
        "Usage example",
        R"(
SELECT toDateOrZero('2025-12-30'), toDateOrZero('')
        )",
        R"(
┌─toDateOrZero('2025-12-30')─┬─toDateOrZero('')─┐
│                 2025-12-30 │       1970-01-01 │
└────────────────────────────┴──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateOrZero = {1, 1};
    FunctionDocumentation::Category category_toDateOrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateOrZero = {description_toDateOrZero, syntax_toDateOrZero, arguments_toDateOrZero, returned_value_toDateOrZero, examples_toDateOrZero, introduced_in_toDateOrZero, category_toDateOrZero};

    factory.registerFunction<detail::FunctionToDateOrZero>(documentation_toDateOrZero);

    /// toDate32OrZero documentation
    FunctionDocumentation::Description description_toDate32OrZero = R"(
Converts an input value to a value of type [Date32](../data-types/date32.md) but returns the lower boundary of [Date32](../data-types/date32.md) if an invalid argument is received.
The same as [toDate32](#toDate32) but returns lower boundary of [Date32](../data-types/date32.md) if an invalid argument is received.

See also:
- [`toDate32`](#toDate32)
- [`toDate32OrNull`](#toDate32OrNull)
- [`toDate32OrDefault`](#toDate32OrDefault)
    )";
    FunctionDocumentation::Syntax syntax_toDate32OrZero = "toDate32OrZero(x)";
    FunctionDocumentation::Arguments arguments_toDate32OrZero = {
        {"x", "A string representation of a date.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_toDate32OrZero = {"Returns a Date32 value if successful, otherwise the lower boundary of Date32 (`1900-01-01`).", {"Date32"}};
    FunctionDocumentation::Examples examples_toDate32OrZero = {
    {
        "Usage example",
        R"(
SELECT toDate32OrZero('2025-01-01'), toDate32OrZero('')
        )",
        R"(
┌─toDate32OrZero('2025-01-01')─┬─toDate32OrZero('')─┐
│                   2025-01-01 │         1900-01-01 │
└──────────────────────────────┴────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDate32OrZero = {21, 9};
    FunctionDocumentation::Category category_toDate32OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDate32OrZero = {description_toDate32OrZero, syntax_toDate32OrZero, arguments_toDate32OrZero, returned_value_toDate32OrZero, examples_toDate32OrZero, introduced_in_toDate32OrZero, category_toDate32OrZero};

    factory.registerFunction<detail::FunctionToDate32OrZero>(documentation_toDate32OrZero);

    /// toTimeOrZero documentation
    FunctionDocumentation::Description description_toTimeOrZero = R"(
Converts an input value to a value of type Time but returns `00:00:00` in case of an error.
Like toTime but returns `00:00:00` instead of throwing an exception on conversion errors.
    )";
    FunctionDocumentation::Syntax syntax_toTimeOrZero = "toTimeOrZero(x)";
    FunctionDocumentation::Arguments arguments_toTimeOrZero = {
        {"x", "A string representation of a time.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_toTimeOrZero = {"Returns a Time value if successful, otherwise `00:00:00`.", {"Time"}};
    FunctionDocumentation::Examples examples_toTimeOrZero = {
    {
        "Usage example",
        R"(
SELECT toTimeOrZero('12:30:45'), toTimeOrZero('invalid')
        )",
        R"(
┌─toTimeOrZero('12:30:45')─┬─toTimeOrZero('invalid')─┐
│                 12:30:45 │                00:00:00 │
└──────────────────────────┴─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toTimeOrZero = {1, 1};
    FunctionDocumentation::Category category_toTimeOrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toTimeOrZero = {description_toTimeOrZero, syntax_toTimeOrZero, arguments_toTimeOrZero, returned_value_toTimeOrZero, examples_toTimeOrZero, introduced_in_toTimeOrZero, category_toTimeOrZero};

    factory.registerFunction<detail::FunctionToTimeOrZero>(documentation_toTimeOrZero);

    /// toTime64OrZero documentation
    FunctionDocumentation::Description description_toTime64OrZero = R"(
Converts an input value to a value of type Time64 but returns `00:00:00.000` in case of an error.
Like [`toTime64`](#toTime54) but returns `00:00:00.000` instead of throwing an exception on conversion errors.
)";
    FunctionDocumentation::Syntax syntax_toTime64OrZero = "toTime64OrZero(x)";
    FunctionDocumentation::Arguments arguments_toTime64OrZero = {
        {"x", "A string representation of a time with subsecond precision.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_toTime64OrZero = {"Returns a Time64 value if successful, otherwise `00:00:00.000`.", {"Time64"}};
    FunctionDocumentation::Examples examples_toTime64OrZero = {
    {
    "Usage example",
    R"(
SELECT toTime64OrZero('12:30:45.123'), toTime64OrZero('invalid')
    )",
    R"(
┌─toTime64OrZero('12:30:45.123')─┬─toTime64OrZero('invalid')─┐
│                   12:30:45.123 │             00:00:00.000 │
└────────────────────────────────┴──────────────────────────┘
    )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toTime64OrZero = {25, 6};
    FunctionDocumentation::Category category_toTime64OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toTime64OrZero = {description_toTime64OrZero, syntax_toTime64OrZero, arguments_toTime64OrZero, returned_value_toTime64OrZero, examples_toTime64OrZero, introduced_in_toTime64OrZero, category_toTime64OrZero};

    factory.registerFunction<detail::FunctionToTime64OrZero>(documentation_toTime64OrZero);

    /// toDateTimeOrZero documentation
    FunctionDocumentation::Description description_toDateTimeOrZero = R"(
Converts an input value to a value of type [DateTime](../data-types/datetime.md) but returns the lower boundary of [DateTime](../data-types/datetime.md) if an invalid argument is received.
The same as [toDateTime](#todatetime) but returns lower boundary of [DateTime](../data-types/datetime.md) if an invalid argument is received.
    )";
    FunctionDocumentation::Syntax syntax_toDateTimeOrZero = "toDateTimeOrZero(x)";
    FunctionDocumentation::Arguments arguments_toDateTimeOrZero = {
        {"x", "A string representation of a date with time.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateTimeOrZero = {"Returns a DateTime value if successful, otherwise the lower boundary of DateTime (`1970-01-01 00:00:00`).", {"DateTime"}};
    FunctionDocumentation::Examples examples_toDateTimeOrZero = {
    {
        "Usage example",
        R"(
SELECT toDateTimeOrZero('2025-12-30 13:44:17'), toDateTimeOrZero('invalid')
        )",
        R"(
┌─toDateTimeOrZero('2025-12-30 13:44:17')─┬─toDateTimeOrZero('invalid')─┐
│                     2025-12-30 13:44:17 │         1970-01-01 00:00:00 │
└─────────────────────────────────────────┴─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateTimeOrZero = {1, 1};
    FunctionDocumentation::Category category_toDateTimeOrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateTimeOrZero = {description_toDateTimeOrZero, syntax_toDateTimeOrZero, arguments_toDateTimeOrZero, returned_value_toDateTimeOrZero, examples_toDateTimeOrZero, introduced_in_toDateTimeOrZero, category_toDateTimeOrZero};

    factory.registerFunction<detail::FunctionToDateTimeOrZero>(documentation_toDateTimeOrZero);

    /// toDateTime64OrZero documentation
    FunctionDocumentation::Description description_toDateTime64OrZero = R"(
Converts an input value to a value of type [DateTime64](../data-types/datetime64.md) but returns the lower boundary of [DateTime64](../data-types/datetime64.md) if an invalid argument is received.
The same as [toDateTime64](#todatetime64) but returns lower boundary of [DateTime64](../data-types/datetime64.md) if an invalid argument is received.

See also:
- [toDateTime64](#toDateTime64).
- [toDateTime64OrNull](#toDateTime64OrNull).
- [toDateTime64OrDefault](#toDateTime64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toDateTime64OrZero = "toDateTime64OrZero(x)";
    FunctionDocumentation::Arguments arguments_toDateTime64OrZero = {
        {"x", "A string representation of a date with time and subsecond precision.", {"String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateTime64OrZero = {"Returns a DateTime64 value if successful, otherwise the lower boundary of DateTime64 (`1970-01-01 00:00:00.000`).", {"DateTime64"}};
    FunctionDocumentation::Examples examples_toDateTime64OrZero = {
    {
        "Usage example",
        R"(
SELECT toDateTime64OrZero('2025-12-30 13:44:17.123'), toDateTime64OrZero('invalid')
        )",
        R"(
┌─toDateTime64OrZero('2025-12-30 13:44:17.123')─┬─toDateTime64OrZero('invalid')─┐
│                         2025-12-30 13:44:17.123 │             1970-01-01 00:00:00.000 │
└─────────────────────────────────────────────────┴─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateTime64OrZero = {20, 1};
    FunctionDocumentation::Category category_toDateTime64OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateTime64OrZero = {description_toDateTime64OrZero, syntax_toDateTime64OrZero, arguments_toDateTime64OrZero, returned_value_toDateTime64OrZero, examples_toDateTime64OrZero, introduced_in_toDateTime64OrZero, category_toDateTime64OrZero};

    factory.registerFunction<detail::FunctionToDateTime64OrZero>(documentation_toDateTime64OrZero);

    /// toDecimal32OrZero documentation
    FunctionDocumentation::Description description_toDecimal32OrZero = R"(
Converts an input value to a value of type [Decimal(9, S)](../data-types/decimal.md) but returns `0` in case of an error.
Like [`toDecimal32`](#todecimal32) but returns `0` instead of throwing an exception on conversion errors.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments (return `0`):
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values.

:::note
If the input value exceeds the bounds of `Decimal32`:`(-1*10^(9 - S), 1*10^(9 - S))`, the function returns `0`.
:::
    )";
    FunctionDocumentation::Syntax syntax_toDecimal32OrZero = "toDecimal32OrZero(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal32OrZero = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 9, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal32OrZero = {"Returns a Decimal(9, S) value if successful, otherwise `0`.", {"Decimal32(S)"}};
    FunctionDocumentation::Examples examples_toDecimal32OrZero = {
    {
        "Usage example",
        R"(
SELECT toDecimal32OrZero('42.7', 2), toDecimal32OrZero('invalid', 2)
        )",
        R"(
┌─toDecimal32OrZero('42.7', 2)─┬─toDecimal32OrZero('invalid', 2)─┐
│                        42.70 │                            0.00 │
└──────────────────────────────┴─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal32OrZero = {20, 1};
    FunctionDocumentation::Category category_toDecimal32OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal32OrZero = {description_toDecimal32OrZero, syntax_toDecimal32OrZero, arguments_toDecimal32OrZero, returned_value_toDecimal32OrZero, examples_toDecimal32OrZero, introduced_in_toDecimal32OrZero, category_toDecimal32OrZero};

    factory.registerFunction<detail::FunctionToDecimal32OrZero>(documentation_toDecimal32OrZero);

    /// toDecimal64OrZero documentation
    FunctionDocumentation::Description description_toDecimal64OrZero = R"(
Converts an input value to a value of type [Decimal(18, S)](../data-types/decimal.md) but returns `0` in case of an error.
Like [`toDecimal64`](#todecimal64) but returns `0` instead of throwing an exception on conversion errors.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments (return `0`):
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values.

:::note
If the input value exceeds the bounds of `Decimal64`:`(-1*10^(18 - S), 1*10^(18 - S))`, the function returns `0`.
:::

See also:
- [`toDecimal64`](#toDecimal64).
- [`toDecimal64OrNull`](#toDecimal64OrNull).
- [`toDecimal64OrDefault`](#toDecimal64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toDecimal64OrZero = "toDecimal64OrZero(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal64OrZero = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 18, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal64OrZero = {"Returns a Decimal(18, S) value if successful, otherwise `0`.", {"Decimal64(S)"}};
    FunctionDocumentation::Examples examples_toDecimal64OrZero = {
    {
        "Usage example",
        R"(
SELECT toDecimal64OrZero('42.7', 2), toDecimal64OrZero('invalid', 2)
        )",
        R"(
┌─toDecimal64OrZero('42.7', 2)─┬─toDecimal64OrZero('invalid', 2)─┐
│                        42.70 │                            0.00 │
└──────────────────────────────┴─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal64OrZero = {20, 1};
    FunctionDocumentation::Category category_toDecimal64OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal64OrZero = {description_toDecimal64OrZero, syntax_toDecimal64OrZero, arguments_toDecimal64OrZero, returned_value_toDecimal64OrZero, examples_toDecimal64OrZero, introduced_in_toDecimal64OrZero, category_toDecimal64OrZero};

    factory.registerFunction<detail::FunctionToDecimal64OrZero>(documentation_toDecimal64OrZero);

    /// toDecimal128OrZero documentation
    FunctionDocumentation::Description description_toDecimal128OrZero = R"(
Converts an input value to a value of type [Decimal(38, S)](../data-types/decimal.md) but returns `0` in case of an error.
Like [`toDecimal128`](#todecimal128) but returns `0` instead of throwing an exception on conversion errors.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments (return `0`):
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values.

:::note
If the input value exceeds the bounds of `Decimal128`:`(-1*10^(38 - S), 1*10^(38 - S))`, the function returns `0`.
:::
    )";
    FunctionDocumentation::Syntax syntax_toDecimal128OrZero = "toDecimal128OrZero(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal128OrZero = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 38, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal128OrZero = {"Returns a Decimal(38, S) value if successful, otherwise `0`.", {"Decimal128(S)"}};
    FunctionDocumentation::Examples examples_toDecimal128OrZero = {
    {
        "Basic usage",
        R"(
SELECT toDecimal128OrZero('42.7', 2), toDecimal128OrZero('invalid', 2)
        )",
        R"(
┌─toDecimal128OrZero('42.7', 2)─┬─toDecimal128OrZero('invalid', 2)─┐
│                         42.70 │                             0.00 │
└───────────────────────────────┴──────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal128OrZero = {20, 1};
    FunctionDocumentation::Category category_toDecimal128OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal128OrZero = {description_toDecimal128OrZero, syntax_toDecimal128OrZero, arguments_toDecimal128OrZero, returned_value_toDecimal128OrZero, examples_toDecimal128OrZero, introduced_in_toDecimal128OrZero, category_toDecimal128OrZero};

    factory.registerFunction<detail::FunctionToDecimal128OrZero>(documentation_toDecimal128OrZero);

    /// toDecimal256OrZero documentation
    FunctionDocumentation::Description description_toDecimal256OrZero = R"(
Converts an input value to a value of type [Decimal(76, S)](../data-types/decimal.md) but returns `0` in case of an error.
Like [`toDecimal256`](#todecimal256) but returns `0` instead of throwing an exception on conversion errors.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments (return `0`):
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values.

:::note
If the input value exceeds the bounds of `Decimal256`:`(-1*10^(76 - S), 1*10^(76 - S))`, the function returns `0`.
:::

See also:
- [`toDecimal256`](#toDecimal256).
- [`toDecimal256OrNull`](#toDecimal256OrNull).
- [`toDecimal256OrDefault`](#toDecimal256OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toDecimal256OrZero = "toDecimal256OrZero(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal256OrZero = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 76, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal256OrZero = {"Returns a Decimal(76, S) value if successful, otherwise `0`.", {"Decimal256(S)"}};
    FunctionDocumentation::Examples examples_toDecimal256OrZero = {
    {
        "Usage example",
        R"(
SELECT toDecimal256OrZero('42.7', 2), toDecimal256OrZero('invalid', 2)
        )",
        R"(
┌─toDecimal256OrZero('42.7', 2)─┬─toDecimal256OrZero('invalid', 2)─┐
│                         42.70 │                             0.00 │
└───────────────────────────────┴──────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal256OrZero = {20, 8};
    FunctionDocumentation::Category category_toDecimal256OrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal256OrZero = {description_toDecimal256OrZero, syntax_toDecimal256OrZero, arguments_toDecimal256OrZero, returned_value_toDecimal256OrZero, examples_toDecimal256OrZero, introduced_in_toDecimal256OrZero, category_toDecimal256OrZero};

    factory.registerFunction<detail::FunctionToDecimal256OrZero>(documentation_toDecimal256OrZero);

    /// toUUIDOrZero documentation
    FunctionDocumentation::Description description_toUUIDOrZero = R"(
Converts an input value to a value of type [UUID](../data-types/uuid.md) but returns zero UUID in case of an error.
Like [`toUUID`](#touuid) but returns zero UUID (`00000000-0000-0000-0000-000000000000`) instead of throwing an exception on conversion errors.

Supported arguments:
- String representations of UUID in standard format (8-4-4-4-12 hexadecimal digits).
- String representations of UUID without hyphens (32 hexadecimal digits).

Unsupported arguments (return zero UUID):
- Invalid string formats.
- Non-string types.
    )";
    FunctionDocumentation::Syntax syntax_toUUIDOrZero = "toUUIDOrZero(x)";
    FunctionDocumentation::Arguments arguments_toUUIDOrZero =
    {
        {"x", "A string representation of a UUID.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUUIDOrZero = {"Returns a UUID value if successful, otherwise zero UUID (`00000000-0000-0000-0000-000000000000`).", {"UUID"}};
    FunctionDocumentation::Examples examples_toUUIDOrZero = {
    {
        "Usage example",
        R"(
SELECT
    toUUIDOrZero('550e8400-e29b-41d4-a716-446655440000') AS valid_uuid,
    toUUIDOrZero('invalid-uuid') AS invalid_uuid
        )",
        R"(
┌─valid_uuid───────────────────────────┬─invalid_uuid─────────────────────────┐
│ 550e8400-e29b-41d4-a716-446655440000 │ 00000000-0000-0000-0000-000000000000 │
└──────────────────────────────────────┴──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUUIDOrZero = {20, 12};
    FunctionDocumentation::Category category_toUUIDOrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUUIDOrZero = {description_toUUIDOrZero, syntax_toUUIDOrZero, arguments_toUUIDOrZero, returned_value_toUUIDOrZero, examples_toUUIDOrZero, introduced_in_toUUIDOrZero, category_toUUIDOrZero};

    factory.registerFunction<detail::FunctionToUUIDOrZero>(documentation_toUUIDOrZero);

    /// toIPv4OrZero documentation
    FunctionDocumentation::Description description_toIPv4OrZero = R"(
Converts an input value to a value of type [IPv4](../data-types/ipv4.md) but returns zero IPv4 address in case of an error.
Like [`toIPv4`](#toIPv4) but returns zero IPv4 address (`0.0.0.0`) instead of throwing an exception on conversion errors.

Supported arguments:
- String representations of IPv4 addresses in dotted decimal notation.
- Integer representations of IPv4 addresses.

Unsupported arguments (return zero IPv4):
- Invalid IP address formats.
- IPv6 addresses.
- Out-of-range values.
    )";
    FunctionDocumentation::Syntax syntax_toIPv4OrZero = "toIPv4OrZero(x)";
    FunctionDocumentation::Arguments arguments_toIPv4OrZero =
    {
        {"x", "A string or integer representation of an IPv4 address.", {"String", "Integer"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIPv4OrZero = {"Returns an IPv4 address if successful, otherwise zero IPv4 address (`0.0.0.0`).", {"IPv4"}};
    FunctionDocumentation::Examples examples_toIPv4OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toIPv4OrZero('192.168.1.1') AS valid_ip,
    toIPv4OrZero('invalid.ip') AS invalid_ip
        )",
        R"(
┌─valid_ip────┬─invalid_ip─┐
│ 192.168.1.1 │ 0.0.0.0    │
└─────────────┴────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIPv4OrZero = {23, 1};
    FunctionDocumentation::Category category_toIPv4OrZero = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_toIPv4OrZero = {description_toIPv4OrZero, syntax_toIPv4OrZero, arguments_toIPv4OrZero, returned_value_toIPv4OrZero, examples_toIPv4OrZero,introduced_in_toIPv4OrZero, category_toIPv4OrZero};

    factory.registerFunction<detail::FunctionToIPv4OrZero>(documentation_toIPv4OrZero);

    /// toIPv6OrZero documentation
    FunctionDocumentation::Description description_toIPv6OrZero = R"(
Converts an input value to a value of type [IPv6](../data-types/ipv6.md) but returns zero IPv6 address in case of an error.
Like [`toIPv6`](#toIPv6) but returns zero IPv6 address (`::`) instead of throwing an exception on conversion errors.

Supported arguments:
- String representations of IPv6 addresses in standard notation.
- String representations of IPv4 addresses (converted to IPv4-mapped IPv6).
- Binary representations of IPv6 addresses.

Unsupported arguments (return zero IPv6):
- Invalid IP address formats.
- Malformed IPv6 addresses.
- Out-of-range values.
    )";
    FunctionDocumentation::Syntax syntax_toIPv6OrZero = "toIPv6OrZero(x)";
    FunctionDocumentation::Arguments arguments_toIPv6OrZero =
    {
        {"x", "A string representation of an IPv6 or IPv4 address.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIPv6OrZero = {"Returns an IPv6 address if successful, otherwise zero IPv6 address (`::`). ", {"IPv6"}};
    FunctionDocumentation::Examples examples_toIPv6OrZero = {
    {
        "Usage example",
        R"(
SELECT
    toIPv6OrZero('2001:0db8:85a3:0000:0000:8a2e:0370:7334') AS valid_ipv6,
    toIPv6OrZero('invalid::ip') AS invalid_ipv6
        )",
        R"(
┌─valid_ipv6──────────────────────────┬─invalid_ipv6─┐
│ 2001:db8:85a3::8a2e:370:7334        │ ::           │
└─────────────────────────────────────┴──────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIPv6OrZero = {23, 1};
    FunctionDocumentation::Category category_toIPv6OrZero = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_toIPv6OrZero = {description_toIPv6OrZero, syntax_toIPv6OrZero, arguments_toIPv6OrZero, returned_value_toIPv6OrZero, examples_toIPv6OrZero, introduced_in_toIPv6OrZero, category_toIPv6OrZero};

    factory.registerFunction<detail::FunctionToIPv6OrZero>(documentation_toIPv6OrZero);

    /// toUInt8OrNull documentation
    FunctionDocumentation::Description description_toUInt8OrNull = R"(
Like [`toUInt8`](#toUInt8), this function converts an input value to a value of type [`UInt8`](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `NULL`):
- String representations of ordinary Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt8OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt8`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt8`](#toUInt8).
- [`toUInt8OrZero`](#toUInt8OrZero).
- [`toUInt8OrDefault`](#toUInt8OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt8OrNull = "toUInt8OrNull(x)";
    FunctionDocumentation::Arguments arguments_toUInt8OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt8OrNull = {"Returns a value of type UInt8, otherwise `NULL` if the conversion is unsuccessful.", {"UInt8", "NULL"}};
    FunctionDocumentation::Examples examples_toUInt8OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toUInt8OrNull('42'),
    toUInt8OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt8OrNull('42'):  42
toUInt8OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::Category category_toUInt8OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation::IntroducedIn introduced_in_toUInt8OrNull = {1, 1};
    FunctionDocumentation documentation_toUInt8OrNull = {description_toUInt8OrNull, syntax_toUInt8OrNull, arguments_toUInt8OrNull, returned_value_toUInt8OrNull, examples_toUInt8OrNull, introduced_in_toUInt8OrNull, category_toUInt8OrNull};

    factory.registerFunction<detail::FunctionToUInt8OrNull>(documentation_toUInt8OrNull);

    /// toUInt16OrNull documentation
    FunctionDocumentation::Description description_toUInt16OrNull = R"(
Like [`toUInt16`](#toUInt16), this function converts an input value to a value of type [`UInt16`](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt16OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt16`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt16`](#toUInt16).
- [`toUInt16OrZero`](#toUInt16OrZero).
- [`toUInt16OrDefault`](#toUInt16OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt16OrNull = "toUInt16OrNull(x)";
    FunctionDocumentation::Arguments arguments_toUInt16OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt16OrNull = {"Returns a value of type `UInt16`, otherwise `NULL` if the conversion is unsuccessful.", {"UInt16", "NULL"}};
    FunctionDocumentation::Examples examples_toUInt16OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toUInt16OrNull('16'),
    toUInt16OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt16OrNull('16'):  16
toUInt16OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt16OrNull = {1, 1};
    FunctionDocumentation::Category category_toUInt16OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt16OrNull = {description_toUInt16OrNull, syntax_toUInt16OrNull, arguments_toUInt16OrNull, returned_value_toUInt16OrNull, examples_toUInt16OrNull, introduced_in_toUInt16OrNull, category_toUInt16OrNull};

    factory.registerFunction<detail::FunctionToUInt16OrNull>(documentation_toUInt16OrNull);

    /// toUInt32OrNull documentation
    FunctionDocumentation::Description description_toUInt32OrNull = R"(
Like [`toUInt32`](#toUInt32), this function converts an input value to a value of type [`UInt32`](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt32OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt32`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt32`](#toUInt32).
- [`toUInt32OrZero`](#toUInt32OrZero).
- [`toUInt32OrDefault`](#toUInt32OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt32OrNull = "toUInt32OrNull(x)";
    FunctionDocumentation::Arguments arguments_toUInt32OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt32OrNull = {"Returns a value of type `UInt32`, otherwise `NULL` if the conversion is unsuccessful.", {"UInt32", "NULL"}};
    FunctionDocumentation::Examples examples_toUInt32OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toUInt32OrNull('32'),
    toUInt32OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt32OrNull('32'):  32
toUInt32OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt32OrNull = {1, 1};
    FunctionDocumentation::Category category_toUInt32OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt32OrNull = {description_toUInt32OrNull, syntax_toUInt32OrNull, arguments_toUInt32OrNull, returned_value_toUInt32OrNull, examples_toUInt32OrNull, introduced_in_toUInt32OrNull, category_toUInt32OrNull};

    factory.registerFunction<detail::FunctionToUInt32OrNull>(documentation_toUInt32OrNull);

    /// toUInt64OrNull documentation
    FunctionDocumentation::Description description_toUInt64OrNull = R"(
Like [`toUInt64`](#toUInt64), this function converts an input value to a value of type [`UInt64`](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt64OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt64`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt64`](#toUInt64).
- [`toUInt64OrZero`](#toUInt64OrZero).
- [`toUInt64OrDefault`](#toUInt64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt64OrNull = "toUInt64OrNull(x)";
    FunctionDocumentation::Arguments arguments_toUInt64OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt64OrNull = {"Returns a value of type UInt64, otherwise `NULL` if the conversion is unsuccessful.", {"UInt64", "NULL"}};
    FunctionDocumentation::Examples examples_toUInt64OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toUInt64OrNull('64'),
    toUInt64OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt64OrNull('64'):  64
toUInt64OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUint64OrNull = {1, 1};
    FunctionDocumentation::Category category_toUInt64OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt64OrNull = {description_toUInt64OrNull, syntax_toUInt64OrNull, arguments_toUInt64OrNull, returned_value_toUInt64OrNull, examples_toUInt64OrNull, introduced_in_toUint64OrNull, category_toUInt64OrNull};

    factory.registerFunction<detail::FunctionToUInt64OrNull>(documentation_toUInt64OrNull);

    /// toUInt128OrNull documentation
    FunctionDocumentation::Description description_toUInt128OrNull = R"(
Like [`toUInt128`](#toUInt128), this function converts an input value to a value of type [`UInt128`](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt128OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt128`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt128`](#toUInt128).
- [`toUInt128OrZero`](#toUInt128OrZero).
- [`toUInt128OrDefault`](#toUInt128OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt128OrNull = "toUInt128OrNull(x)";
    FunctionDocumentation::Arguments arguments_toUInt128OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt128OrNull = {"Returns a value of type UInt128, otherwise `NULL` if the conversion is unsuccessful.", {"UInt128", "NULL"}};
    FunctionDocumentation::Examples examples_toUInt128OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toUInt128OrNull('128'),
    toUInt128OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt128OrNull('128'): 128
toUInt128OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt128OrNull = {21, 6};
    FunctionDocumentation::Category category_toUInt128OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt128OrNull = {description_toUInt128OrNull, syntax_toUInt128OrNull, arguments_toUInt128OrNull, returned_value_toUInt128OrNull, examples_toUInt128OrNull, introduced_in_toUInt128OrNull, category_toUInt128OrNull};

    factory.registerFunction<detail::FunctionToUInt128OrNull>(documentation_toUInt128OrNull);

    /// toUInt256OrNull documentation
    FunctionDocumentation::Description description_toUInt256OrNull = R"(
Like [`toUInt256`](#toUInt256), this function converts an input value to a value of type [`UInt256`](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt256OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [`UInt256`](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toUInt256`](#toUInt256).
- [`toUInt256OrZero`](#toUInt256OrZero).
- [`toUInt256OrDefault`](#toUInt256OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toUInt256OrNull = "toUInt256OrNull(x)";
    FunctionDocumentation::Arguments arguments_toUInt256OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUInt256OrNull = {"Returns a value of type UInt256, otherwise `NULL` if the conversion is unsuccessful.", {"UInt256", "NULL"}};
    FunctionDocumentation::Examples examples_toUInt256OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toUInt256OrNull('256'),
    toUInt256OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toUInt256OrNull('256'): 256
toUInt256OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUInt256OrNull = {20, 8};
    FunctionDocumentation::Category category_toUInt256OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUInt256OrNull = {description_toUInt256OrNull, syntax_toUInt256OrNull, arguments_toUInt256OrNull, returned_value_toUInt256OrNull, examples_toUInt256OrNull, introduced_in_toUInt256OrNull, category_toUInt256OrNull};

    factory.registerFunction<detail::FunctionToUInt256OrNull>(documentation_toUInt256OrNull);

    /// toInt8OrNull documentation
    FunctionDocumentation::Description description_toInt8OrNull = R"(
Like [`toInt8`](#toInt8), this function converts an input value to a value of type [Int8](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt8OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt8`](#toInt8).
- [`toInt8OrZero`](#toInt8OrZero).
- [`toInt8OrDefault`](#toInt8OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt8OrNull = "toInt8OrNull(x)";
    FunctionDocumentation::Arguments arguments_toInt8OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt8OrNull = {"Returns a value of type Int8, otherwise `NULL` if the conversion is unsuccessful.", {"Int8", "NULL"}};
    FunctionDocumentation::Examples examples_toInt8OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toInt8OrNull('-8'),
    toInt8OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt8OrNull('-8'):  -8
toInt8OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt8OrNull = {1, 1};
    FunctionDocumentation::Category category_toInt8OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt8OrNull = {description_toInt8OrNull, syntax_toInt8OrNull, arguments_toInt8OrNull, returned_value_toInt8OrNull, examples_toInt8OrNull, introduced_in_toInt8OrNull, category_toInt8OrNull};

    factory.registerFunction<detail::FunctionToInt8OrNull>(documentation_toInt8OrNull);

    /// toInt16OrNull documentation
    FunctionDocumentation::Description description_toInt16OrNull = R"(
Like [`toInt16`](#toInt16), this function converts an input value to a value of type [Int16](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt16OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt16`](#toInt16).
- [`toInt16OrZero`](#toInt16OrZero).
- [`toInt16OrDefault`](#toInt16OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt16OrNull = "toInt16OrNull(x)";
    FunctionDocumentation::Arguments arguments_toInt16OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt16OrNull = {"Returns a value of type `Int16`, otherwise `NULL` if the conversion is unsuccessful.", {"Int16", "NULL"}};
    FunctionDocumentation::Examples examples_toInt16OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toInt16OrNull('-16'),
    toInt16OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt16OrNull('-16'): -16
toInt16OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt16OrNull = {1, 1};
    FunctionDocumentation::Category category_toInt16OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt16OrNull = {description_toInt16OrNull, syntax_toInt16OrNull, arguments_toInt16OrNull, returned_value_toInt16OrNull, examples_toInt16OrNull, introduced_in_toInt16OrNull, category_toInt16OrNull};

    factory.registerFunction<detail::FunctionToInt16OrNull>(documentation_toInt16OrNull);

    /// toInt32OrNull documentation
    FunctionDocumentation::Description description_toInt32OrNull = R"(
Like [`toInt32`](#toInt32), this function converts an input value to a value of type [Int32](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt32OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int32](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt32`](#toInt32).
- [`toInt32OrZero`](#toInt32OrZero).
- [`toInt32OrDefault`](#toInt32OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt32OrNull = "toInt32OrNull(x)";
    FunctionDocumentation::Arguments arguments_toInt32OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt32OrNull = {"Returns a value of type Int32, otherwise `NULL` if the conversion is unsuccessful.", {"Int32", "NULL"}};
    FunctionDocumentation::Examples examples_toInt32OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toInt32OrNull('-32'),
    toInt32OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt32OrNull('-32'): -32
toInt32OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt32OrNull = {1, 1};
    FunctionDocumentation::Category category_toInt32OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt32OrNull = {description_toInt32OrNull, syntax_toInt32OrNull, arguments_toInt32OrNull, returned_value_toInt32OrNull, examples_toInt32OrNull, introduced_in_toInt32OrNull, category_toInt32OrNull};

    factory.registerFunction<detail::FunctionToInt32OrNull>(documentation_toInt32OrNull);

    /// toInt64OrNull documentation
    FunctionDocumentation::Description description_toInt64OrNull = R"(
Like [`toInt64`](#toInt64), this function converts an input value to a value of type [Int64](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt64OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int64](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt64`](#toInt64).
- [`toInt64OrZero`](#toInt64OrZero).
- [`toInt64OrDefault`](#toInt64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt64OrNull = "toInt64OrNull(x)";
    FunctionDocumentation::Arguments arguments_toInt64OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt64OrNull = {"Returns a value of type Int64, otherwise `NULL` if the conversion is unsuccessful.", {"Int64", "NULL"}};
    FunctionDocumentation::Examples examples_toInt64OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toInt64OrNull('-64'),
    toInt64OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt64OrNull('-64'): -64
toInt64OrNull('abc'): \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt64OrNull = {1, 1};
    FunctionDocumentation::Category category_toInt64OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt64OrNull = {description_toInt64OrNull, syntax_toInt64OrNull, arguments_toInt64OrNull, returned_value_toInt64OrNull, examples_toInt64OrNull, introduced_in_toInt64OrNull, category_toInt64OrNull};

    factory.registerFunction<detail::FunctionToInt64OrNull>(documentation_toInt64OrNull);

    /// toInt128OrNull documentation
    FunctionDocumentation::Description description_toInt128OrNull = R"(
Like [`toInt128`](#toInt128), this function converts an input value to a value of type [Int128](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt128OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int128](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt128`](#toInt128).
- [`toInt128OrZero`](#toInt128OrZero).
- [`toInt128OrDefault`](#toInt128OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt128OrNull = "toInt128OrNull(x)";
    FunctionDocumentation::Arguments arguments_toInt128OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt128OrNull = {"Returns a value of type Int128, otherwise `NULL` if the conversion is unsuccessful.", {"Int128", "NULL"}};
    FunctionDocumentation::Examples examples_toInt128OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toInt128OrNull('-128'),
    toInt128OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt128OrNull('-128'): -128
toInt128OrNull('abc'):  \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt128OrNull = {20, 8};
    FunctionDocumentation::Category category_toInt128OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt128OrNull = {description_toInt128OrNull, syntax_toInt128OrNull, arguments_toInt128OrNull, returned_value_toInt128OrNull, examples_toInt128OrNull, introduced_in_toInt128OrNull, category_toInt128OrNull};

    factory.registerFunction<detail::FunctionToInt128OrNull>(documentation_toInt128OrNull);

    /// toInt256OrNull documentation
    FunctionDocumentation::Description description_toInt256OrNull = R"(
Like [`toInt256`](#toInt256), this function converts an input value to a value of type [Int256](../data-types/int-uint.md) but returns `NULL` in case of an error.

Supported arguments:
- String representations of (U)Int*.

Unsupported arguments (return `NULL`):
- String representations of Float* values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt256OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int256](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

See also:
- [`toInt256`](#toInt256).
- [`toInt256OrZero`](#toInt256OrZero).
- [`toInt256OrDefault`](#toInt256OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toInt256OrNull = "toInt256OrNull(x)";
    FunctionDocumentation::Arguments arguments_toInt256OrNull =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toInt256OrNull = {"Returns a value of type Int256, otherwise `NULL` if the conversion is unsuccessful.", {"Int256", "NULL"}};
    FunctionDocumentation::Examples examples_toInt256OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toInt256OrNull('-256'),
    toInt256OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toInt256OrNull('-256'): -256
toInt256OrNull('abc'):  \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toInt256OrNull = {20, 8};
    FunctionDocumentation::Category category_toInt256OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toInt256OrNull = {description_toInt256OrNull, syntax_toInt256OrNull, arguments_toInt256OrNull, returned_value_toInt256OrNull, examples_toInt256OrNull, introduced_in_toInt256OrNull, category_toInt256OrNull};

    factory.registerFunction<detail::FunctionToInt256OrNull>(documentation_toInt256OrNull);

    /// toBFloat16OrNull documentation
    FunctionDocumentation::Description toBFloat16OrNull_description = R"(
Converts a String input value to a value of type BFloat16.
If the string does not represent a floating point value, the function returns NULL.

Supported arguments:
- String representations of numeric values.

Unsupported arguments (return `NULL`):
- String representations of binary and hexadecimal values.
- Numeric values.

:::note
The function allows a silent loss of precision while converting from the string representation.
:::

See also:
- [`toBFloat16`](#toBFloat16).
- [`toBFloat16OrZero`](#toBFloat16OrZero).
    )";
    FunctionDocumentation::Syntax toBFloat16OrNull_syntax = "toBFloat16OrNull(x)";
    FunctionDocumentation::Arguments toBFloat16OrNull_arguments =
    {
        {"x", "A String representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue toBFloat16OrNull_returned_value = {"Reurns a 16-bit brain-float value, otherwise `NULL`.", {"BFloat16", "NULL"}};
    FunctionDocumentation::Examples toBFloat16OrNull_examples = {
    {
        "Usage example",
        R"(
SELECT toBFloat16OrNull('0x5E'), -- unsupported arguments
       toBFloat16OrNull('12.3'), -- typical use
       toBFloat16OrNull('12.3456789') -- silent loss of precision
        )",
        R"(
\N
12.25
12.3125
        )"
    }
    };
    FunctionDocumentation::IntroducedIn toBFloat16OrNull_introduced_in = {1, 1};
    FunctionDocumentation::Category toBFloat16OrNull_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation toBFloat16OrNull_documentation = {toBFloat16OrNull_description, toBFloat16OrNull_syntax, toBFloat16OrNull_arguments, toBFloat16OrNull_returned_value, toBFloat16OrNull_examples, toBFloat16OrNull_introduced_in, toBFloat16OrNull_category};

    factory.registerFunction<detail::FunctionToBFloat16OrNull>(toBFloat16OrNull_documentation);

    /// toFloat32OrNull documentation
    FunctionDocumentation::Description description_toFloat32OrNull = R"(
Converts an input value to a value of type [Float32](../data-types/float.md) but returns `NULL` in case of an error.
Like [`toFloat32`](#toFloat32) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- Values of type (U)Int*.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float*, including `NaN` and `Inf`.
- String representations of Float*, including `NaN` and `Inf` (case-insensitive).

Unsupported arguments (return `NULL`):
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat32OrNull('0xc0fe');`.
- Invalid string formats.

See also:
- [`toFloat32`](#toFloat32).
- [`toFloat32OrZero`](#toFloat32OrZero).
- [`toFloat32OrDefault`](#toFloat32OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toFloat32OrNull = "toFloat32OrNull(x)";
    FunctionDocumentation::Arguments arguments_toFloat32OrNull =
    {
        {"x", "A string representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toFloat32OrNull = {"Returns a 32-bit Float value if successful, otherwise `NULL`.", {"Float32", "NULL"}};
    FunctionDocumentation::Examples examples_toFloat32OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toFloat32OrNull('42.7'),
    toFloat32OrNull('NaN'),
    toFloat32OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toFloat32OrNull('42.7'): 42.7
toFloat32OrNull('NaN'):  nan
toFloat32OrNull('abc'):  \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toFloat32OrNull = {1, 1};
    FunctionDocumentation::Category category_toFloat32OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toFloat32OrNull = {description_toFloat32OrNull, syntax_toFloat32OrNull, arguments_toFloat32OrNull, returned_value_toFloat32OrNull, examples_toFloat32OrNull, introduced_in_toFloat32OrNull, category_toFloat32OrNull};

    factory.registerFunction<detail::FunctionToFloat32OrNull>(documentation_toFloat32OrNull);

    /// toFloat64OrNull documentation
    FunctionDocumentation::Description description_toFloat64OrNull = R"(
Converts an input value to a value of type [Float64](../data-types/float.md) but returns `NULL` in case of an error.
Like [`toFloat64`](#tofloat64) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- Values of type (U)Int*.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float*, including `NaN` and `Inf`.
- String representations of type Float*, including `NaN` and `Inf` (case-insensitive).

Unsupported arguments (return `NULL`):
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat64OrNull('0xc0fe');`.
- Invalid string formats.

See also:
- [`toFloat64`](#toFloat64).
- [`toFloat64OrZero`](#toFloat64OrZero).
- [`toFloat64OrDefault`](#toFloat64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toFloat64OrNull = "toFloat64OrNull(x)";
    FunctionDocumentation::Arguments arguments_toFloat64OrNull =
    {
        {"x", "A string representation of a number.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toFloat64OrNull = {"Returns a 64-bit Float value if successful, otherwise `NULL`.", {"Float64", "NULL"}};
    FunctionDocumentation::Examples examples_toFloat64OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toFloat64OrNull('42.7'),
    toFloat64OrNull('NaN'),
    toFloat64OrNull('abc')
FORMAT Vertical
        )",
        R"(
Row 1:
──────
toFloat64OrNull('42.7'): 42.7
toFloat64OrNull('NaN'):  nan
toFloat64OrNull('abc'):  \N
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toFloat64OrNull = {1, 1};
    FunctionDocumentation::Category category_toFloat64OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toFloat64OrNull = {description_toFloat64OrNull, syntax_toFloat64OrNull, arguments_toFloat64OrNull, returned_value_toFloat64OrNull, examples_toFloat64OrNull, introduced_in_toFloat64OrNull, category_toFloat64OrNull};

    factory.registerFunction<detail::FunctionToFloat64OrNull>(documentation_toFloat64OrNull);

    /// toDateOrNull documentation
    FunctionDocumentation::Description description_toDateOrNull = R"(
Converts an input value to a value of type `Date` but returns `NULL` if an invalid argument is received.
The same as [`toDate`](#toDate) but returns `NULL` if an invalid argument is received.
    )";
    FunctionDocumentation::Syntax syntax_toDateOrNull = "toDateOrNull(x)";
    FunctionDocumentation::Arguments arguments_toDateOrNull =
    {
        {"x", "A string representation of a date.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateOrNull = {"Returns a Date value if successful, otherwise `NULL`.", {"Date", "NULL"}};
    FunctionDocumentation::Examples examples_toDateOrNull = {
    {
        "Usage example",
        R"(
SELECT toDateOrNull('2025-12-30'), toDateOrNull('invalid')
        )",
        R"(
┌─toDateOrNull('2025-12-30')─┬─toDateOrNull('invalid')─┐
│                 2025-12-30 │                   ᴺᵁᴸᴸ │
└────────────────────────────┴────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateOrNull = {1, 1};
    FunctionDocumentation::Category category_toDateOrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateOrNull = {description_toDateOrNull, syntax_toDateOrNull, arguments_toDateOrNull, returned_value_toDateOrNull, examples_toDateOrNull, introduced_in_toDateOrNull, category_toDateOrNull};

    factory.registerFunction<detail::FunctionToDateOrNull>(documentation_toDateOrNull);

    /// toDate32OrNull documentation
    FunctionDocumentation::Description description_toDate32OrNull = R"(
Converts an input value to a value of type Date32 but returns `NULL` if an invalid argument is received.
The same as [`toDate32`](#toDate32) but returns `NULL` if an invalid argument is received.
    )";
    FunctionDocumentation::Syntax syntax_toDate32OrNull = "toDate32OrNull(x)";
    FunctionDocumentation::Arguments arguments_toDate32OrNull =
    {
        {"x", "A string representation of a date.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDate32OrNull = {"Returns a Date32 value if successful, otherwise `NULL`.", {"Date32", "NULL"}};
    FunctionDocumentation::Examples examples_toDate32OrNull = {
    {
        "Usage example",
        R"(
SELECT toDate32OrNull('2025-01-01'), toDate32OrNull('invalid')
        )",
        R"(
┌─toDate32OrNull('2025-01-01')─┬─toDate32OrNull('invalid')─┐
│                   2025-01-01 │                      ᴺᵁᴸᴸ │
└──────────────────────────────┴───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDate32OrNull = {21, 9};
    FunctionDocumentation::Category category_toDate32OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDate32OrNull = {description_toDate32OrNull, syntax_toDate32OrNull, arguments_toDate32OrNull, returned_value_toDate32OrNull, examples_toDate32OrNull, introduced_in_toDate32OrNull, category_toDate32OrNull};

    factory.registerFunction<detail::FunctionToDate32OrNull>(documentation_toDate32OrNull);

    /// toTimeOrNull documentation
    FunctionDocumentation::Description description_toTimeOrNull = R"(
Converts an input value to a value of type Time but returns `NULL` in case of an error.
Like [`toTime`](#toTime) but returns `NULL` instead of throwing an exception on conversion errors.

See also:
- [`toTime`](#toTime)
- [`toTimeOrZero`](#toTimeOrZero)
    )";
    FunctionDocumentation::Syntax syntax_toTimeOrNull = "toTimeOrNull(x)";
    FunctionDocumentation::Arguments arguments_toTimeOrNull =
    {
        {"x", "A string representation of a time.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toTimeOrNull = {"Returns a Time value if successful, otherwise `NULL`.", {"Time", "NULL"}};
    FunctionDocumentation::Examples examples_toTimeOrNull = {
    {
        "Usage example",
        R"(
SELECT toTimeOrNull('12:30:45'), toTimeOrNull('invalid')
        )",
        R"(
┌─toTimeOrNull('12:30:45')─┬─toTimeOrNull('invalid')─┐
│                 12:30:45 │                    ᴺᵁᴸᴸ │
└──────────────────────────┴─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toTimeOrNull = {1, 1};
    FunctionDocumentation::Category category_toTimeOrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toTimeOrNull = {description_toTimeOrNull, syntax_toTimeOrNull, arguments_toTimeOrNull, returned_value_toTimeOrNull, examples_toTimeOrNull, introduced_in_toTimeOrNull, category_toTimeOrNull};

    factory.registerFunction<detail::FunctionToTimeOrNull>(documentation_toTimeOrNull);

    /// toTime64OrNull documentation
    FunctionDocumentation::Description description_toTime64OrNull = R"(
Converts an input value to a value of type `Time64` but returns `NULL` in case of an error.
Like [`toTime64`](#toTime64) but returns `NULL` instead of throwing an exception on conversion errors.

See also:
- [`toTime64`](#toTime64)
- [`toTime64OrZero`](#toTime64OrZero)
    )";
    FunctionDocumentation::Syntax syntax_toTime64OrNull = "toTime64OrNull(x)";
    FunctionDocumentation::Arguments arguments_toTime64OrNull =
    {
        {"x", "A string representation of a time with subsecond precision.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toTime64OrNull = {"Returns a Time64 value if successful, otherwise `NULL`.", {"Time64", "NULL"}};
    FunctionDocumentation::Examples examples_toTime64OrNull = {
    {
        "Usage example",
        R"(
SELECT toTime64OrNull('12:30:45.123'), toTime64OrNull('invalid')
        )",
        R"(
┌─toTime64OrNull('12:30:45.123')─┬─toTime64OrNull('invalid')─┐
│                   12:30:45.123 │                      ᴺᵁᴸᴸ │
└────────────────────────────────┴───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toTime64OrNull = {25, 6};
    FunctionDocumentation::Category category_toTime64OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toTime64OrNull = {description_toTime64OrNull, syntax_toTime64OrNull, arguments_toTime64OrNull, returned_value_toTime64OrNull, examples_toTime64OrNull, introduced_in_toTime64OrNull, category_toTime64OrNull};

    factory.registerFunction<detail::FunctionToTime64OrNull>(documentation_toTime64OrNull);

    /// toDateTimeOrNull documentation
    FunctionDocumentation::Description description_toDateTimeOrNull = R"(
Converts an input value to a value of type `DateTime` but returns `NULL` if an invalid argument is received.
The same as [`toDateTime`](#toDateTime) but returns `NULL` if an invalid argument is received.
    )";
    FunctionDocumentation::Syntax syntax_toDateTimeOrNull = "toDateTimeOrNull(x)";
    FunctionDocumentation::Arguments arguments_toDateTimeOrNull =
    {
        {"x", "A string representation of a date with time.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateTimeOrNull = {"Returns a `DateTime` value if successful, otherwise `NULL`.", {"DateTime", "NULL"}};
    FunctionDocumentation::Examples examples_toDateTimeOrNull = {
    {
        "Usage example",
        R"(
SELECT toDateTimeOrNull('2025-12-30 13:44:17'), toDateTimeOrNull('invalid')
        )",
        R"(
┌─toDateTimeOrNull('2025-12-30 13:44:17')─┬─toDateTimeOrNull('invalid')─┐
│                     2025-12-30 13:44:17 │                        ᴺᵁᴸᴸ │
└─────────────────────────────────────────┴─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateTimeOrNull = {1, 1};
    FunctionDocumentation::Category category_toDateTimeOrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateTimeOrNull = {description_toDateTimeOrNull, syntax_toDateTimeOrNull, arguments_toDateTimeOrNull, returned_value_toDateTimeOrNull, examples_toDateTimeOrNull, introduced_in_toDateTimeOrNull, category_toDateTimeOrNull};

    factory.registerFunction<detail::FunctionToDateTimeOrNull>(documentation_toDateTimeOrNull);

    /// toDateTime64OrNull documentation
    FunctionDocumentation::Description description_toDateTime64OrNull = R"(
Converts an input value to a value of type `DateTime64` but returns `NULL` if an invalid argument is received.
The same as `toDateTime64` but returns `NULL` if an invalid argument is received.
    )";
    FunctionDocumentation::Syntax syntax_toDateTime64OrNull = "toDateTime64OrNull(x)";
    FunctionDocumentation::Arguments arguments_toDateTime64OrNull =
    {
        {"x", "A string representation of a date with time and subsecond precision.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDateTime64OrNull = {"Returns a DateTime64 value if successful, otherwise `NULL`.", {"DateTime64", "NULL"}};
    FunctionDocumentation::Examples examples_toDateTime64OrNull = {
    {
        "Usage example",
        R"(
SELECT toDateTime64OrNull('2025-12-30 13:44:17.123'), toDateTime64OrNull('invalid')
        )",
        R"(
┌─toDateTime64OrNull('2025-12-30 13:44:17.123')─┬─toDateTime64OrNull('invalid')─┐
│                         2025-12-30 13:44:17.123 │                          ᴺᵁᴸᴸ │
└─────────────────────────────────────────────────┴───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDateTime64OrNull = {20, 1};
    FunctionDocumentation::Category category_toDateTime64OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDateTime64OrNull = {description_toDateTime64OrNull, syntax_toDateTime64OrNull, arguments_toDateTime64OrNull, returned_value_toDateTime64OrNull, examples_toDateTime64OrNull, introduced_in_toDateTime64OrNull, category_toDateTime64OrNull};

    factory.registerFunction<detail::FunctionToDateTime64OrNull>(documentation_toDateTime64OrNull);

    /// toDecimal32OrNull documentation
    FunctionDocumentation::Description description_toDecimal32OrNull = R"(
Converts an input value to a value of type [`Decimal(9, S)`](../data-types/decimal.md) but returns `NULL` in case of an error.
Like [`toDecimal32`](#toDecimal32) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments (return `NULL`):
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values.
- Values that exceed the bounds of `Decimal32`:`(-1*10^(9 - S), 1*10^(9 - S))`.

See also:
- [`toDecimal32`](#toDecimal32).
- [`toDecimal32OrZero`](#toDecimal32OrZero).
- [`toDecimal32OrDefault`](#toDecimal32OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toDecimal32OrNull = "toDecimal32OrNull(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal32OrNull =
    {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 9, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal32OrNull = {"Returns a Decimal(9, S) value if successful, otherwise `NULL`.", {"Decimal32(S)", "NULL"}};
    FunctionDocumentation::Examples examples_toDecimal32OrNull = {
    {
        "Usage example",
        R"(
SELECT toDecimal32OrNull('42.7', 2), toDecimal32OrNull('invalid', 2)
        )",
        R"(
┌─toDecimal32OrNull('42.7', 2)─┬─toDecimal32OrNull('invalid', 2)─┐
│                        42.70 │                            ᴺᵁᴸᴸ │
└──────────────────────────────┴─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal32OrNull = {20, 1};
    FunctionDocumentation::Category category_toDecimal32OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal32OrNull = {description_toDecimal32OrNull, syntax_toDecimal32OrNull, arguments_toDecimal32OrNull, returned_value_toDecimal32OrNull, examples_toDecimal32OrNull, introduced_in_toDecimal32OrNull, category_toDecimal32OrNull};

    factory.registerFunction<detail::FunctionToDecimal32OrNull>(documentation_toDecimal32OrNull);

    /// toDecimal64OrNull documentation
    FunctionDocumentation::Description description_toDecimal64OrNull = R"(
Converts an input value to a value of type [Decimal(18, S)](../data-types/decimal.md) but returns `NULL` in case of an error.
Like [`toDecimal64`](#todecimal64) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments (return `NULL`):
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values.
- Values that exceed the bounds of `Decimal64`:`(-1*10^(18 - S), 1*10^(18 - S))`.

See also:
- [`toDecimal64`](#toDecimal64).
- [`toDecimal64OrZero`](#toDecimal64OrZero).
- [`toDecimal64OrDefault`](#toDecimal64OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toDecimal64OrNull = "toDecimal64OrNull(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal64OrNull =
    {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 18, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal64OrNull = {"Returns a Decimal(18, S) value if successful, otherwise `NULL`.", {"Decimal64(S)", "NULL"}};
    FunctionDocumentation::Examples examples_toDecimal64OrNull = {
    {
        "Usage example",
        R"(
SELECT toDecimal64OrNull('42.7', 2), toDecimal64OrNull('invalid', 2)
        )",
        R"(
┌─toDecimal64OrNull('42.7', 2)─┬─toDecimal64OrNull('invalid', 2)─┐
│                        42.70 │                            ᴺᵁᴸᴸ │
└──────────────────────────────┴─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal64OrNull = {20, 1};
    FunctionDocumentation::Category category_toDecimal64OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal64OrNull = {description_toDecimal64OrNull, syntax_toDecimal64OrNull, arguments_toDecimal64OrNull, returned_value_toDecimal64OrNull, examples_toDecimal64OrNull, introduced_in_toDecimal64OrNull, category_toDecimal64OrNull};

    factory.registerFunction<detail::FunctionToDecimal64OrNull>(documentation_toDecimal64OrNull);

    /// toDecimal128OrNull documentation
    FunctionDocumentation::Description description_toDecimal128OrNull = R"(
Converts an input value to a value of type [`Decimal(38, S)`](../data-types/decimal.md) but returns `NULL` in case of an error.
Like [`toDecimal128`](#toDecimal128) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments (return `NULL`):
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values.
- Values that exceed the bounds of `Decimal128`:`(-1*10^(38 - S), 1*10^(38 - S))`.

See also:
- [`toDecimal128`](#toDecimal128).
- [`toDecimal128OrZero`](#toDecimal128OrZero).
- [`toDecimal128OrDefault`](#toDecimal128OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toDecimal128OrNull = "toDecimal128OrNull(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal128OrNull = {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 38, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal128OrNull = {"Returns a Decimal(38, S) value if successful, otherwise `NULL`.", {"Decimal128(S)", "NULL"}};
    FunctionDocumentation::Examples examples_toDecimal128OrNull = {
    {
        "Usage example",
        R"(
SELECT toDecimal128OrNull('42.7', 2), toDecimal128OrNull('invalid', 2)
        )",
        R"(
┌─toDecimal128OrNull('42.7', 2)─┬─toDecimal128OrNull('invalid', 2)─┐
│                         42.70 │                             ᴺᵁᴸᴸ │
└───────────────────────────────┴──────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal128OrNull = {20, 1};
    FunctionDocumentation::Category category_toDecimal128OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal128OrNull = {description_toDecimal128OrNull, syntax_toDecimal128OrNull, arguments_toDecimal128OrNull, returned_value_toDecimal128OrNull, examples_toDecimal128OrNull, introduced_in_toDecimal128OrNull, category_toDecimal128OrNull};

    factory.registerFunction<detail::FunctionToDecimal128OrNull>(documentation_toDecimal128OrNull);

    /// toDecimal256OrNull documentation
    FunctionDocumentation::Description description_toDecimal256OrNull = R"(
Converts an input value to a value of type [`Decimal(76, S)`](../data-types/decimal.md) but returns `NULL` in case of an error.
Like [`toDecimal256`](#toDecimal256) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- Values or string representations of type (U)Int*.
- Values or string representations of type Float*.

Unsupported arguments (return `NULL`):
- Values or string representations of Float* values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values.
- Values that exceed the bounds of `Decimal256`: `(-1 * 10^(76 - S), 1 * 10^(76 - S))`.

See also:
- [`toDecimal256`](#toDecimal256).
- [`toDecimal256OrZero`](#toDecimal256OrZero).
- [`toDecimal256OrDefault`](#toDecimal256OrDefault).
    )";
    FunctionDocumentation::Syntax syntax_toDecimal256OrNull = "toDecimal256OrNull(expr, S)";
    FunctionDocumentation::Arguments arguments_toDecimal256OrNull =
    {
        {"expr", "Expression returning a number or a string representation of a number.", {"Expression"}},
        {"S", "Scale parameter between 0 and 76, specifying how many digits the fractional part of a number can have.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDecimal256OrNull = {"Returns a Decimal(76, S) value if successful, otherwise `NULL`.", {"Decimal256(S)", "NULL"}};
    FunctionDocumentation::Examples examples_toDecimal256OrNull = {
    {
        "Usage example",
        R"(
SELECT toDecimal256OrNull('42.7', 2), toDecimal256OrNull('invalid', 2)
        )",
        R"(
┌─toDecimal256OrNull('42.7', 2)─┬─toDecimal256OrNull('invalid', 2)─┐
│                         42.70 │                             ᴺᵁᴸᴸ │
└───────────────────────────────┴──────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDecimal256OrNull = {20, 8};
    FunctionDocumentation::Category category_toDecimal256OrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toDecimal256OrNull = {description_toDecimal256OrNull, syntax_toDecimal256OrNull, arguments_toDecimal256OrNull, returned_value_toDecimal256OrNull, examples_toDecimal256OrNull, introduced_in_toDecimal256OrNull, category_toDecimal256OrNull};

    factory.registerFunction<detail::FunctionToDecimal256OrNull>(documentation_toDecimal256OrNull);

    /// toUUIDOrNull documentation
    FunctionDocumentation::Description description_toUUIDOrNull = R"(
Converts an input value to a value of type `UUID` but returns `NULL` in case of an error.
Like [`toUUID`](#touuid) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- String representations of UUID in standard format (8-4-4-4-12 hexadecimal digits).
- String representations of UUID without hyphens (32 hexadecimal digits).

Unsupported arguments (return `NULL`):
- Invalid string formats.
- Non-string types.
- Malformed UUIDs.
    )";
    FunctionDocumentation::Syntax syntax_toUUIDOrNull = "toUUIDOrNull(x)";
    FunctionDocumentation::Arguments arguments_toUUIDOrNull =
    {
        {"x", "A string representation of a UUID.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUUIDOrNull = {"Returns a UUID value if successful, otherwise `NULL`.", {"UUID", "NULL"}};
    FunctionDocumentation::Examples examples_toUUIDOrNull = {
    {
        "Usage examples",
        R"(
SELECT
    toUUIDOrNull('550e8400-e29b-41d4-a716-446655440000') AS valid_uuid,
    toUUIDOrNull('invalid-uuid') AS invalid_uuid
        )",
        R"(
┌─valid_uuid───────────────────────────┬─invalid_uuid─┐
│ 550e8400-e29b-41d4-a716-446655440000 │         ᴺᵁᴸᴸ │
└──────────────────────────────────────┴──────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUUIDOrNull = {20, 12};
    FunctionDocumentation::Category category_toUUIDOrNull = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_toUUIDOrNull = {description_toUUIDOrNull, syntax_toUUIDOrNull, arguments_toUUIDOrNull, returned_value_toUUIDOrNull, examples_toUUIDOrNull, introduced_in_toUUIDOrNull, category_toUUIDOrNull};

    factory.registerFunction<detail::FunctionToUUIDOrNull>(documentation_toUUIDOrNull);

    /// toIPv4OrNull documentation
    FunctionDocumentation::Description description_toIPv4OrNull = R"(
Converts an input value to a value of type `IPv4` but returns `NULL` in case of an error.
Like [`toIPv4`](#toIPv4) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- String representations of IPv4 addresses in dotted decimal notation.
- Integer representations of IPv4 addresses.

Unsupported arguments (return `NULL`):
- Invalid IP address formats.
- IPv6 addresses.
- Out-of-range values.
- Malformed addresses.
    )";
    FunctionDocumentation::Syntax syntax_toIPv4OrNull = "toIPv4OrNull(x)";
    FunctionDocumentation::Arguments arguments_toIPv4OrNull =
    {
        {"x", "A string or integer representation of an IPv4 address.", {"String", "Integer"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIPv4OrNull = {"Returns an IPv4 address if successful, otherwise `NULL`.", {"IPv4", "NULL"}};
    FunctionDocumentation::Examples examples_toIPv4OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toIPv4OrNull('192.168.1.1') AS valid_ip,
    toIPv4OrNull('invalid.ip') AS invalid_ip
        )",
        R"(
┌─valid_ip────┬─invalid_ip─┐
│ 192.168.1.1 │       ᴺᵁᴸᴸ │
└─────────────┴────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIPv4OrNull = {22, 3};
    FunctionDocumentation::Category category_toIPv4OrNull = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_toIPv4OrNull = {description_toIPv4OrNull, syntax_toIPv4OrNull, arguments_toIPv4OrNull, returned_value_toIPv4OrNull, examples_toIPv4OrNull, introduced_in_toIPv4OrNull, category_toIPv4OrNull};

    factory.registerFunction<detail::FunctionToIPv4OrNull>(documentation_toIPv4OrNull);

    /// toIPv6OrNull documentation
    FunctionDocumentation::Description description_toIPv6OrNull = R"(
Converts an input value to a value of type `IPv6` but returns `NULL` in case of an error.
Like [`toIPv6`](#toIPv6) but returns `NULL` instead of throwing an exception on conversion errors.

Supported arguments:
- String representations of IPv6 addresses in standard notation.
- String representations of IPv4 addresses (converted to IPv4-mapped IPv6).
- Binary representations of IPv6 addresses.

Unsupported arguments (return `NULL`):
- Invalid IP address formats.
- Malformed IPv6 addresses.
- Out-of-range values.
- Invalid notation.
    )";
    FunctionDocumentation::Syntax syntax_toIPv6OrNull = "toIPv6OrNull(x)";
    FunctionDocumentation::Arguments arguments_toIPv6OrNull =
    {
        {"x", "A string representation of an IPv6 or IPv4 address.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIPv6OrNull = {"Returns an IPv6 address if successful, otherwise `NULL`.", {"IPv6", "NULL"}};
    FunctionDocumentation::Examples examples_toIPv6OrNull = {
    {
        "Usage example",
        R"(
SELECT
    toIPv6OrNull('2001:0db8:85a3:0000:0000:8a2e:0370:7334') AS valid_ipv6,
    toIPv6OrNull('invalid::ip') AS invalid_ipv6
        )",
        R"(
┌─valid_ipv6──────────────────────────┬─invalid_ipv6─┐
│ 2001:db8:85a3::8a2e:370:7334        │         ᴺᵁᴸᴸ │
└─────────────────────────────────────┴──────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIPv6OrNull = {22, 3};
    FunctionDocumentation::Category category_toIPv6OrNull = FunctionDocumentation::Category::IPAddress;
    FunctionDocumentation documentation_toIPv6OrNull = {description_toIPv6OrNull, syntax_toIPv6OrNull, arguments_toIPv6OrNull, returned_value_toIPv6OrNull, examples_toIPv6OrNull, introduced_in_toIPv6OrNull, category_toIPv6OrNull};

    factory.registerFunction<detail::FunctionToIPv6OrNull>(documentation_toIPv6OrNull);

    /// parseDateTimeBestEffort documentation
    FunctionDocumentation::Description parseDateTimeBestEffort_description = R"(
Converts a date and time in the String representation to DateTime data type.
The function parses [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html), [RFC 1123 - 5.2.14 RFC-822](https://datatracker.ietf.org/doc/html/rfc822) Date and Time Specification, ClickHouse's and some other date and time formats.

Supported non-standard formats:
- A string containing 9..10 digit unix timestamp.
- A string with a date and a time component: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
- A string with a date, but no time component: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` etc.
- A string with a day and time: `DD`, `DD hh`, `DD hh:mm`. In this case `MM` is substituted by `01`.
- A string that includes the date and time along with time zone offset information: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc.
- A syslog timestamp: `Mmm dd hh:mm:ss`. For example, `Jun  9 14:20:32`.

For all of the formats with separator the function parses months names expressed by their full name or by the first three letters of a month name.
If the year is not specified, it is considered to be equal to the current year.
    )";
    FunctionDocumentation::Syntax parseDateTimeBestEffort_syntax = "parseDateTimeBestEffort(time_string[, time_zone])";
    FunctionDocumentation::Arguments parseDateTimeBestEffort_arguments =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue parseDateTimeBestEffort_returned_value = {"Returns `time_string` as a `DateTime`.", {"DateTime"}};
    FunctionDocumentation::Examples parseDateTimeBestEffort_examples =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTimeBestEffort('23/10/2025 12:12:57') AS parseDateTimeBestEffort
        )",
        R"(
┌─parseDateTimeBestEffort─┐
│     2025-10-23 12:12:57 │
└─────────────────────────┘
        )"
    },
    {
        "With timezone",
        R"(
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2025 07:22:16 GMT', 'Asia/Istanbul') AS parseDateTimeBestEffort
        )",
        R"(
┌─parseDateTimeBestEffort─┐
│     2025-08-18 10:22:16 │
└─────────────────────────┘
        )"
    },
    {
        "Unix timestamp",
        R"(
SELECT parseDateTimeBestEffort('1735689600') AS parseDateTimeBestEffort
        )",
        R"(
┌─parseDateTimeBestEffort─┐
│     2025-01-01 00:00:00 │
└─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn parseDateTimeBestEffort_introduced_in = {1, 1};
    FunctionDocumentation::Category parseDateTimeBestEffort_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTimeBestEffort_documentation = {parseDateTimeBestEffort_description, parseDateTimeBestEffort_syntax, parseDateTimeBestEffort_arguments, parseDateTimeBestEffort_returned_value, parseDateTimeBestEffort_examples, parseDateTimeBestEffort_introduced_in, parseDateTimeBestEffort_category};

    factory.registerFunction<detail::FunctionParseDateTimeBestEffort>(parseDateTimeBestEffort_documentation);

    /// parseDateTimeBestEffortOrZero documentation
    FunctionDocumentation::Description parseDateTimeBestEffortOrZero_description = R"(
Same as [`parseDateTimeBestEffort`](#parseDateTimeBestEffort) except that it returns a zero date or a zero date time when it encounters a date format that cannot be processed.
The function parses [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123 - 5.2.14 RFC-822 Date and Time Specification](https://tools.ietf.org/html/rfc1123#page-55), ClickHouse's and some other date and time formats.

Supported non-standard formats:
- A string containing 9..10 digit unix timestamp.
- A string with a date and a time component: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
- A string with a date, but no time component: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` etc.
- A string with a day and time: `DD`, `DD hh`, `DD hh:mm`. In this case `MM` is substituted by `01`.
- A string that includes the date and time along with time zone offset information: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc.
- A syslog timestamp: `Mmm dd hh:mm:ss`. For example, `Jun  9 14:20:32`.

For all of the formats with separator the function parses months names expressed by their full name or by the first three letters of a month name.
If the year is not specified, it is considered to be equal to the current year.
    )";
    FunctionDocumentation::Syntax parseDateTimeBestEffortOrZero_syntax = "parseDateTimeBestEffortOrZero(time_string[, time_zone])";
    FunctionDocumentation::Arguments parseDateTimeBestEffortOrZero_arguments =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue parseDateTimeBestEffortOrZero_returned_value = {"Returns `time_string` as a `DateTime`, or zero date/datetime (`1970-01-01` or `1970-01-01 00:00:00`) if the input cannot be parsed.", {"DateTime"}};
    FunctionDocumentation::Examples parseDateTimeBestEffortOrZero_examples =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTimeBestEffortOrZero('23/10/2025 12:12:57') AS valid,
       parseDateTimeBestEffortOrZero('invalid') AS invalid
        )",
        R"(
┌─valid───────────────┬─invalid─────────────┐
│ 2025-10-23 12:12:57 │ 1970-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn parseDateTimeBestEffortOrZero_introduced_in = {1, 1};
    FunctionDocumentation::Category parseDateTimeBestEffortOrZero_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTimeBestEffortOrZero_documentation = {parseDateTimeBestEffortOrZero_description, parseDateTimeBestEffortOrZero_syntax, parseDateTimeBestEffortOrZero_arguments, parseDateTimeBestEffortOrZero_returned_value, parseDateTimeBestEffortOrZero_examples, parseDateTimeBestEffortOrZero_introduced_in, parseDateTimeBestEffortOrZero_category};

    factory.registerFunction<detail::FunctionParseDateTimeBestEffortOrZero>(parseDateTimeBestEffortOrZero_documentation);

    /// parseDateTimeBestEffortOrNull documentation
    FunctionDocumentation::Description parseDateTimeBestEffortOrNull_description = R"(
The same as [`parseDateTimeBestEffort`](#parseDateTimeBestEffort) except that it returns `NULL` when it encounters a date format that cannot be processed.
The function parses [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123 - 5.2.14 RFC-822 Date and Time Specification](https://tools.ietf.org/html/rfc1123#page-55), ClickHouse's and some other date and time formats.

Supported non-standard formats:
- A string containing 9..10 digit unix timestamp.
- A string with a date and a time component: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
- A string with a date, but no time component: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` etc.
- A string with a day and time: `DD`, `DD hh`, `DD hh:mm`. In this case `MM` is substituted by `01`.
- A string that includes the date and time along with time zone offset information: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc.
- A syslog timestamp: `Mmm dd hh:mm:ss`. For example, `Jun  9 14:20:32`.

For all of the formats with separator the function parses months names expressed by their full name or by the first three letters of a month name.
If the year is not specified, it is considered to be equal to the current year.
    )";
    FunctionDocumentation::Syntax parseDateTimeBestEffortOrNull_syntax = "parseDateTimeBestEffortOrNull(time_string[, time_zone])";
    FunctionDocumentation::Arguments parseDateTimeBestEffortOrNull_arguments =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue parseDateTimeBestEffortOrNull_returned_value = {"Returns `time_string` as a DateTime, or `NULL` if the input cannot be parsed.", {"DateTime", "NULL"}};
    FunctionDocumentation::Examples parseDateTimeBestEffortOrNull_examples =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTimeBestEffortOrNull('23/10/2025 12:12:57') AS valid,
       parseDateTimeBestEffortOrNull('invalid') AS invalid
        )",
        R"(
┌─valid───────────────┬─invalid─┐
│ 2025-10-23 12:12:57 │    ᴺᵁᴸᴸ │
└─────────────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn parseDateTimeBestEffortOrNull_introduced_in = {1, 1};
    FunctionDocumentation::Category parseDateTimeBestEffortOrNull_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTimeBestEffortOrNull_documentation = {parseDateTimeBestEffortOrNull_description, parseDateTimeBestEffortOrNull_syntax, parseDateTimeBestEffortOrNull_arguments, parseDateTimeBestEffortOrNull_returned_value, parseDateTimeBestEffortOrNull_examples, parseDateTimeBestEffortOrNull_introduced_in, parseDateTimeBestEffortOrNull_category};

    factory.registerFunction<detail::FunctionParseDateTimeBestEffortOrNull>(parseDateTimeBestEffortOrNull_documentation);

    /// parseDateTimeBestEffortUS documentation
    FunctionDocumentation::Description parseDateTimeBestEffortUS_description = R"(
This function behaves like [`parseDateTimeBestEffort`](#parseDateTimeBestEffort) for ISO date formats, e.g. `YYYY-MM-DD hh:mm:ss`, and other date formats where the month and date components can be unambiguously extracted, e.g. `YYYYMMDDhhmmss`, `YYYY-MM`, `DD hh`, or `YYYY-MM-DD hh:mm:ss ±h:mm`.
If the month and the date components cannot be unambiguously extracted, e.g. `MM/DD/YYYY`, `MM-DD-YYYY`, or `MM-DD-YY`, it prefers the US date format instead of `DD/MM/YYYY`, `DD-MM-YYYY`, or `DD-MM-YY`.
As an exception to the previous statement, if the month is bigger than 12 and smaller or equal than 31, this function falls back to the behavior of [`parseDateTimeBestEffort`](#parseDateTimeBestEffort), e.g. `15/08/2020` is parsed as `2020-08-15`.
    )";
    FunctionDocumentation::Syntax parseDateTimeBestEffortUS_syntax = "parseDateTimeBestEffortUS(time_string[, time_zone])";
    FunctionDocumentation::Arguments parseDateTimeBestEffortUS_arguments =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue parseDateTimeBestEffortUS_returned_value = {"Returns `time_string` as a `DateTime` using US date format preference for ambiguous cases.", {"DateTime"}};
    FunctionDocumentation::Examples parseDateTimeBestEffortUS_examples =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTimeBestEffortUS('02/10/2025') AS us_format,
       parseDateTimeBestEffortUS('15/08/2025') AS fallback_to_standard
        )",
        R"(
┌─us_format───────────┬─fallback_to_standard─┐
│ 2025-02-10 00:00:00 │  2025-08-15 00:00:00 │
└─────────────────────┴──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn parseDateTimeBestEffortUS_introduced_in = {1, 1};
    FunctionDocumentation::Category parseDateTimeBestEffortUS_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTimeBestEffortUS_documentation = {parseDateTimeBestEffortUS_description, parseDateTimeBestEffortUS_syntax, parseDateTimeBestEffortUS_arguments, parseDateTimeBestEffortUS_returned_value, parseDateTimeBestEffortUS_examples, parseDateTimeBestEffortUS_introduced_in, parseDateTimeBestEffortUS_category};

    factory.registerFunction<detail::FunctionParseDateTimeBestEffortUS>(parseDateTimeBestEffortUS_documentation);

    /// parseDateTimeBestEffortUSOrZero documentation
    FunctionDocumentation::Description parseDateTimeBestEffortUSOrZero_description = R"(
Same as [`parseDateTimeBestEffortUS`](#parseDateTimeBestEffortUS) function except that it returns zero date (`1970-01-01`) or zero date with time (`1970-01-01 00:00:00`) when it encounters a date format that cannot be processed.

This function behaves like [`parseDateTimeBestEffort`](#parseDateTimeBestEffort) for ISO date formats, but prefers the US date format for ambiguous cases, with zero return on parsing errors.
    )";
    FunctionDocumentation::Syntax parseDateTimeBestEffortUSOrZero_syntax = "parseDateTimeBestEffortUSOrZero(time_string[, time_zone])";
    FunctionDocumentation::Arguments parseDateTimeBestEffortUSOrZero_arguments =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue parseDateTimeBestEffortUSOrZero_returned_value = {"Returns `time_string` as a `DateTime` using US format preference, or zero date/datetime (`1970-01-01` or `1970-01-01 00:00:00`) if the input cannot be parsed.", {"DateTime"}};
    FunctionDocumentation::Examples parseDateTimeBestEffortUSOrZero_examples =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTimeBestEffortUSOrZero('02/10/2025') AS valid_us,
       parseDateTimeBestEffortUSOrZero('invalid') AS invalid
        )",
        R"(
┌─valid_us────────────┬─invalid─────────────┐
│ 2025-02-10 00:00:00 │ 1970-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn parseDateTimeBestEffortUSOrZero_introduced_in = {1, 1};
    FunctionDocumentation::Category parseDateTimeBestEffortUSOrZero_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTimeBestEffortUSOrZero_documentation = {parseDateTimeBestEffortUSOrZero_description, parseDateTimeBestEffortUSOrZero_syntax, parseDateTimeBestEffortUSOrZero_arguments, parseDateTimeBestEffortUSOrZero_returned_value, parseDateTimeBestEffortUSOrZero_examples, parseDateTimeBestEffortUSOrZero_introduced_in, parseDateTimeBestEffortUSOrZero_category};

    factory.registerFunction<detail::FunctionParseDateTimeBestEffortUSOrZero>(parseDateTimeBestEffortUSOrZero_documentation);

    /// parseDateTimeBestEffortUSOrNull documentation
    FunctionDocumentation::Description parseDateTimeBestEffortUSOrNull_description = R"(
Same as [`parseDateTimeBestEffortUS`](#parseDateTimeBestEffortUS) function except that it returns `NULL` when it encounters a date format that cannot be processed.

This function behaves like [`parseDateTimeBestEffort`](#parseDateTimeBestEffort) for ISO date formats, but prefers the US date format for ambiguous cases, with `NULL` return on parsing errors.
    )";
    FunctionDocumentation::Syntax parseDateTimeBestEffortUSOrNull_syntax = "parseDateTimeBestEffortUSOrNull(time_string[, time_zone])";
    FunctionDocumentation::Arguments parseDateTimeBestEffortUSOrNull_arguments =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue parseDateTimeBestEffortUSOrNull_returned_value = {"Returns `time_string` as a DateTime using US format preference, or `NULL` if the input cannot be parsed.", {"DateTime", "NULL"}};
    FunctionDocumentation::Examples parseDateTimeBestEffortUSOrNull_examples =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTimeBestEffortUSOrNull('02/10/2025') AS valid_us,
       parseDateTimeBestEffortUSOrNull('invalid') AS invalid
        )",
        R"(
┌─valid_us────────────┬─invalid─┐
│ 2025-02-10 00:00:00 │    ᴺᵁᴸᴸ │
└─────────────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn parseDateTimeBestEffortUSOrNull_introduced_in = {1, 1};
    FunctionDocumentation::Category parseDateTimeBestEffortUSOrNull_category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTimeBestEffortUSOrNull_documentation = {parseDateTimeBestEffortUSOrNull_description, parseDateTimeBestEffortUSOrNull_syntax, parseDateTimeBestEffortUSOrNull_arguments, parseDateTimeBestEffortUSOrNull_returned_value, parseDateTimeBestEffortUSOrNull_examples, parseDateTimeBestEffortUSOrNull_introduced_in, parseDateTimeBestEffortUSOrNull_category};

    factory.registerFunction<detail::FunctionParseDateTimeBestEffortUSOrNull>(parseDateTimeBestEffortUSOrNull_documentation);

    /// parseDateTime32BestEffort documentation
    FunctionDocumentation::Description description_parseDateTime32BestEffort = R"(
Converts a string representation of a date and time to the [`DateTime`](/sql-reference/data-types/datetime) data type.

The function parses [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123 - 5.2.14 RFC-822 Date and Time Specification](https://tools.ietf.org/html/rfc1123#page-55), ClickHouse's and some other date and time formats.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime32BestEffort = "parseDateTime32BestEffort(time_string[, time_zone])";
    FunctionDocumentation::Arguments arguments_parseDateTime32BestEffort =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime32BestEffort = {"Returns `time_string` as a `DateTime`.", {"DateTime"}};
    FunctionDocumentation::Examples examples_parseDateTime32BestEffort =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTime32BestEffort('23/10/2025 12:12:57')
AS parseDateTime32BestEffort
        )",
        R"(
┌─parseDateTime32BestEffort─┐
│       2025-10-23 12:12:57 │
└───────────────────────────┘
        )"
    },
    {
        "With timezone",
        R"(
SELECT parseDateTime32BestEffort('Sat, 18 Aug 2025 07:22:16 GMT', 'Asia/Istanbul')
AS parseDateTime32BestEffort
        )",
        R"(
┌─parseDateTime32BestEffort─┐
│       2025-08-18 10:22:16 │
└───────────────────────────┘
        )"
    },
    {
        "Unix timestamp",
        R"(
SELECT parseDateTime32BestEffort('1284101485')
AS parseDateTime32BestEffort
        )",
        R"(
┌─parseDateTime32BestEffort─┐
│       2015-07-07 12:04:41 │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime32BestEffort = {20, 9};
    FunctionDocumentation::Category category_parseDateTime32BestEffort = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_parseDateTime32BestEffort = {description_parseDateTime32BestEffort, syntax_parseDateTime32BestEffort, arguments_parseDateTime32BestEffort, returned_value_parseDateTime32BestEffort, examples_parseDateTime32BestEffort, introduced_in_parseDateTime32BestEffort, category_parseDateTime32BestEffort};

    factory.registerFunction<detail::FunctionParseDateTime32BestEffort>(documentation_parseDateTime32BestEffort);

    /// parseDateTime32BestEffortOrZero documentation
    FunctionDocumentation::Description description_parseDateTime32BestEffortOrZero = R"(
Same as [`parseDateTime32BestEffort`](#parseDateTime32BestEffort) except that it returns a zero date or a zero date time when it encounters a date format that cannot be processed.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime32BestEffortOrZero = "parseDateTime32BestEffortOrZero(time_string[, time_zone])";
    FunctionDocumentation::Arguments arguments_parseDateTime32BestEffortOrZero =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime32BestEffortOrZero = {"Returns a `DateTime` object parsed from the string, or zero date (`1970-01-01 00:00:00`) if the parsing fails.", {"DateTime"}};
    FunctionDocumentation::Examples examples_parseDateTime32BestEffortOrZero =
    {
    {
    "Usage example",
    R"(
SELECT
    parseDateTime32BestEffortOrZero('23/10/2025 12:12:57') AS valid,
    parseDateTime32BestEffortOrZero('invalid date') AS invalid
    )",
    R"(
┌─valid───────────────┬─invalid─────────────┐
│ 2025-10-23 12:12:57 │ 1970-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
    )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime32BestEffortOrZero = {20, 9};
    FunctionDocumentation::Category category_parseDateTime32BestEffortOrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_parseDateTime32BestEffortOrZero = {description_parseDateTime32BestEffortOrZero, syntax_parseDateTime32BestEffortOrZero, arguments_parseDateTime32BestEffortOrZero, returned_value_parseDateTime32BestEffortOrZero, examples_parseDateTime32BestEffortOrZero, introduced_in_parseDateTime32BestEffortOrZero, category_parseDateTime32BestEffortOrZero};

    factory.registerFunction<detail::FunctionParseDateTime32BestEffortOrZero>(documentation_parseDateTime32BestEffortOrZero);

    /// parseDateTime32BestEffortOrNull documentation
    FunctionDocumentation::Description description_parseDateTime32BestEffortOrNull = R"(
Same as [`parseDateTime32BestEffort`](#parseDateTime32BestEffort) except that it returns `NULL` when it encounters a date format that cannot be processed.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime32BestEffortOrNull = "parseDateTime32BestEffortOrNull(time_string[, time_zone])";
    FunctionDocumentation::Arguments arguments_parseDateTime32BestEffortOrNull =
    {
        {"time_string", "String containing a date and time to convert.", {"String"}},
        {"time_zone", "Optional. Time zone according to which `time_string` is parsed.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime32BestEffortOrNull = {"Returns a `DateTime` object parsed from the string, or `NULL` if the parsing fails.", {"DateTime"}};
    FunctionDocumentation::Examples examples_parseDateTime32BestEffortOrNull =
    {
    {
        "Usage example",
        R"(
SELECT
    parseDateTime32BestEffortOrNull('23/10/2025 12:12:57') AS valid,
    parseDateTime32BestEffortOrNull('invalid date') AS invalid
        )",
        R"(
┌─valid───────────────┬─invalid─┐
│ 2025-10-23 12:12:57 │    ᴺᵁᴸᴸ │
└─────────────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime32BestEffortOrNull = {20, 9};
    FunctionDocumentation::Category category_parseDateTime32BestEffortOrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_parseDateTime32BestEffortOrNull = {description_parseDateTime32BestEffortOrNull, syntax_parseDateTime32BestEffortOrNull, arguments_parseDateTime32BestEffortOrNull, returned_value_parseDateTime32BestEffortOrNull, examples_parseDateTime32BestEffortOrNull, introduced_in_parseDateTime32BestEffortOrNull, category_parseDateTime32BestEffortOrNull};

    factory.registerFunction<detail::FunctionParseDateTime32BestEffortOrNull>(documentation_parseDateTime32BestEffortOrNull);

    /// parseDateTime64BestEffort documentation
    FunctionDocumentation::Description description_parseDateTime64BestEffort = R"(
Same as [`parseDateTimeBestEffort`](#parsedatetimebesteffort) function but also parse milliseconds and microseconds and returns [`DateTime64`](../../sql-reference/data-types/datetime64.md) data type.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime64BestEffort = "parseDateTime64BestEffort(time_string[, precision[, time_zone]])";
    FunctionDocumentation::Arguments arguments_parseDateTime64BestEffort =
    {
        {"time_string", "String containing a date or date with time to convert.", {"String"}},
        {"precision", "Optional. Required precision. `3` for milliseconds, `6` for microseconds. Default: `3`.", {"UInt8"}},
        {"time_zone", "Optional. Timezone. The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime64BestEffort = {"Returns `time_string` converted to the [`DateTime64`](../../sql-reference/data-types/datetime64.md) data type.", {"DateTime64"}};
    FunctionDocumentation::Examples examples_parseDateTime64BestEffort =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTime64BestEffort('2025-01-01') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2025-01-01 01:01:00.12346') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2025-01-01 01:01:00.12346',6) AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2025-01-01 01:01:00.12346',3,'Asia/Istanbul') AS a, toTypeName(a) AS t
FORMAT PrettyCompactMonoBlock
        )",
        R"(
┌──────────────────────────a─┬─t──────────────────────────────┐
│ 2025-01-01 01:01:00.123000 │ DateTime64(3)                  │
│ 2025-01-01 00:00:00.000000 │ DateTime64(3)                  │
│ 2025-01-01 01:01:00.123460 │ DateTime64(6)                  │
│ 2025-12-31 22:01:00.123000 │ DateTime64(3, 'Asia/Istanbul') │
└────────────────────────────┴────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime64BestEffort = {20, 1};
    FunctionDocumentation::Category category_parseDateTime64BestEffort = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTime64BestEffort_documentation = {description_parseDateTime64BestEffort, syntax_parseDateTime64BestEffort, arguments_parseDateTime64BestEffort, returned_value_parseDateTime64BestEffort, examples_parseDateTime64BestEffort, introduced_in_parseDateTime64BestEffort, category_parseDateTime64BestEffort};

    factory.registerFunction<detail::FunctionParseDateTime64BestEffort>(parseDateTime64BestEffort_documentation);

    /// parseDateTime64BestEffortOrZero documentation
    FunctionDocumentation::Description description_parseDateTime64BestEffortOrZero = R"(
Same as [`parseDateTime64BestEffort`](#parsedatetime64besteffort) except that it returns zero date or zero date time when it encounters a date format that cannot be processed.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime64BestEffortOrZero = "parseDateTime64BestEffortOrZero(time_string[, precision[, time_zone]])";
    FunctionDocumentation::Arguments arguments_parseDateTime64BestEffortOrZero =
    {
        {"time_string", "String containing a date or date with time to convert.", {"String"}},
        {"precision", "Optional. Required precision. `3` for milliseconds, `6` for microseconds. Default: `3`.", {"UInt8"}},
        {"time_zone", "Optional. Timezone. The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime64BestEffortOrZero = {"Returns `time_string` converted to [`DateTime64`](../../sql-reference/data-types/datetime64.md), or zero date/datetime (`1970-01-01 00:00:00.000`) if the input cannot be parsed.", {"DateTime64"}};
    FunctionDocumentation::Examples examples_parseDateTime64BestEffortOrZero =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTime64BestEffortOrZero('2025-01-01 01:01:00.123') AS valid,
       parseDateTime64BestEffortOrZero('invalid') AS invalid
        )",
        R"(
┌─valid───────────────────┬─invalid─────────────────┐
│ 2025-01-01 01:01:00.123 │ 1970-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime64BestEffortOrZero = {20, 1};
    FunctionDocumentation::Category category_parseDateTime64BestEffortOrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTime64BestEffortOrZero_documentation = {description_parseDateTime64BestEffortOrZero, syntax_parseDateTime64BestEffortOrZero, arguments_parseDateTime64BestEffortOrZero, returned_value_parseDateTime64BestEffortOrZero, examples_parseDateTime64BestEffortOrZero, introduced_in_parseDateTime64BestEffortOrZero, category_parseDateTime64BestEffortOrZero};

    factory.registerFunction<detail::FunctionParseDateTime64BestEffortOrZero>(parseDateTime64BestEffortOrZero_documentation);

    /// parseDateTime64BestEffortOrNull documentation
    FunctionDocumentation::Description description_parseDateTime64BestEffortOrNull = R"(
Same as [`parseDateTime64BestEffort`](#parsedatetime64besteffort) except that it returns `NULL` when it encounters a date format that cannot be processed.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime64BestEffortOrNull = "parseDateTime64BestEffortOrNull(time_string[, precision[, time_zone]])";
    FunctionDocumentation::Arguments arguments_parseDateTime64BestEffortOrNull =
    {
        {"time_string", "String containing a date or date with time to convert.", {"String"}},
        {"precision", "Optional. Required precision. `3` for milliseconds, `6` for microseconds. Default: `3`.", {"UInt8"}},
        {"time_zone", "Optional. Timezone. The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime64BestEffortOrNull = {"Returns `time_string` converted to [`DateTime64`](../../sql-reference/data-types/datetime64.md), or `NULL` if the input cannot be parsed.", {"DateTime64", "NULL"}};
    FunctionDocumentation::Examples examples_parseDateTime64BestEffortOrNull =
    {
    {
    "Usage example",
    R"(
SELECT parseDateTime64BestEffortOrNull('2025-01-01 01:01:00.123') AS valid,
       parseDateTime64BestEffortOrNull('invalid') AS invalid
    )",
    R"(
┌─valid───────────────────┬─invalid─┐
│ 2025-01-01 01:01:00.123 │    ᴺᵁᴸᴸ │
└─────────────────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime64BestEffortOrNull = {20, 1};
    FunctionDocumentation::Category category_parseDateTime64BestEffortOrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTime64BestEffortOrNull_documentation = {description_parseDateTime64BestEffortOrNull, syntax_parseDateTime64BestEffortOrNull, arguments_parseDateTime64BestEffortOrNull, returned_value_parseDateTime64BestEffortOrNull, examples_parseDateTime64BestEffortOrNull, introduced_in_parseDateTime64BestEffortOrNull, category_parseDateTime64BestEffortOrNull};

    factory.registerFunction<detail::FunctionParseDateTime64BestEffortOrNull>(parseDateTime64BestEffortOrNull_documentation);

    /// parseDateTime64BestEffortUS documentation
    FunctionDocumentation::Description description_parseDateTime64BestEffortUS = R"(
Same as [`parseDateTime64BestEffort`](#parsedatetime64besteffort), except that this function prefers US date format (`MM/DD/YYYY` etc.) in case of ambiguity.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime64BestEffortUS = "parseDateTime64BestEffortUS(time_string [, precision [, time_zone]])";
    FunctionDocumentation::Arguments arguments_parseDateTime64BestEffortUS =
    {
        {"time_string", "String containing a date or date with time to convert.", {"String"}},
        {"precision", "Optional. Required precision. `3` for milliseconds, `6` for microseconds. Default: `3`.", {"UInt8"}},
        {"time_zone", "Optional. Timezone. The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime64BestEffortUS = {"Returns `time_string` converted to [`DateTime64`](../../sql-reference/data-types/datetime64.md) using US date format preference for ambiguous cases.", {"DateTime64"}};
    FunctionDocumentation::Examples examples_parseDateTime64BestEffortUS =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTime64BestEffortUS('02/10/2025 12:30:45.123') AS us_format,
       parseDateTime64BestEffortUS('15/08/2025 10:15:30.456') AS fallback_to_standard
        )",
        R"(
┌─us_format───────────────┬─fallback_to_standard────┐
│ 2025-02-10 12:30:45.123 │ 2025-08-15 10:15:30.456 │
└─────────────────────────┴─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime64BestEffortUS = {22, 8};
    FunctionDocumentation::Category category_parseDateTime64BestEffortUS = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTime64BestEffortUS_documentation = {description_parseDateTime64BestEffortUS, syntax_parseDateTime64BestEffortUS, arguments_parseDateTime64BestEffortUS, returned_value_parseDateTime64BestEffortUS, examples_parseDateTime64BestEffortUS, introduced_in_parseDateTime64BestEffortUS, category_parseDateTime64BestEffortUS};

    factory.registerFunction<detail::FunctionParseDateTime64BestEffortUS>(parseDateTime64BestEffortUS_documentation);

    /// parseDateTime64BestEffortUSOrZero documentation
    FunctionDocumentation::Description description_parseDateTime64BestEffortUSOrZero = R"(
Same as [`parseDateTime64BestEffort`](#parsedatetime64besteffort), except that this function prefers US date format (`MM/DD/YYYY` etc.) in case of ambiguity and returns zero date or zero date time when it encounters a date format that cannot be processed.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime64BestEffortUSOrZero = "parseDateTime64BestEffortUSOrZero(time_string [, precision [, time_zone]])";
    FunctionDocumentation::Arguments arguments_parseDateTime64BestEffortUSOrZero =
    {
        {"time_string", "String containing a date or date with time to convert.", {"String"}},
        {"precision", "Optional. Required precision. `3` for milliseconds, `6` for microseconds. Default: `3`.", {"UInt8"}},
        {"time_zone", "Optional. Timezone. The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime64BestEffortUSOrZero = {"Returns `time_string` converted to [`DateTime64`](../../sql-reference/data-types/datetime64.md) using US format preference, or zero date/datetime (`1970-01-01 00:00:00.000`) if the input cannot be parsed.", {"DateTime64"}};
    FunctionDocumentation::Examples examples_parseDateTime64BestEffortUSOrZero =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTime64BestEffortUSOrZero('02/10/2025 12:30:45.123') AS valid_us,
       parseDateTime64BestEffortUSOrZero('invalid') AS invalid
        )",
        R"(
┌─valid_us────────────────┬─invalid─────────────────┐
│ 2025-02-10 12:30:45.123 │ 1970-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime64BestEffortUSOrZero = {22, 8};
    FunctionDocumentation::Category category_parseDateTime64BestEffortUSOrZero = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTime64BestEffortUSOrZero_documentation = {description_parseDateTime64BestEffortUSOrZero, syntax_parseDateTime64BestEffortUSOrZero, arguments_parseDateTime64BestEffortUSOrZero, returned_value_parseDateTime64BestEffortUSOrZero, examples_parseDateTime64BestEffortUSOrZero, introduced_in_parseDateTime64BestEffortUSOrZero, category_parseDateTime64BestEffortUSOrZero};

    factory.registerFunction<detail::FunctionParseDateTime64BestEffortUSOrZero>(parseDateTime64BestEffortUSOrZero_documentation);

    /// parseDateTime64BestEffortUSOrNull documentation
    FunctionDocumentation::Description description_parseDateTime64BestEffortUSOrNull = R"(
Same as [`parseDateTime64BestEffort`](#parsedatetime64besteffort), except that this function prefers US date format (`MM/DD/YYYY` etc.) in case of ambiguity and returns `NULL` when it encounters a date format that cannot be processed.
    )";
    FunctionDocumentation::Syntax syntax_parseDateTime64BestEffortUSOrNull = "parseDateTime64BestEffortUSOrNull(time_string[, precision[, time_zone]])";
    FunctionDocumentation::Arguments arguments_parseDateTime64BestEffortUSOrNull =
    {
        {"time_string", "String containing a date or date with time to convert.", {"String"}},
        {"precision", "Optional. Required precision. `3` for milliseconds, `6` for microseconds. Default: `3`.", {"UInt8"}},
        {"time_zone", "Optional. Timezone. The function parses `time_string` according to the timezone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_parseDateTime64BestEffortUSOrNull = {"Returns `time_string` converted to [`DateTime64`](../../sql-reference/data-types/datetime64.md) using US format preference, or `NULL` if the input cannot be parsed.", {"DateTime64", "NULL"}};
    FunctionDocumentation::Examples examples_parseDateTime64BestEffortUSOrNull =
    {
    {
        "Usage example",
        R"(
SELECT parseDateTime64BestEffortUSOrNull('02/10/2025 12:30:45.123') AS valid_us,
       parseDateTime64BestEffortUSOrNull('invalid') AS invalid
        )",
        R"(
┌─valid_us────────────────┬─invalid─┐
│ 2025-02-10 12:30:45.123 │    ᴺᵁᴸᴸ │
└─────────────────────────┴─────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_parseDateTime64BestEffortUSOrNull = {22, 8};
    FunctionDocumentation::Category category_parseDateTime64BestEffortUSOrNull = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation parseDateTime64BestEffortUSOrNull_documentation = {description_parseDateTime64BestEffortUSOrNull, syntax_parseDateTime64BestEffortUSOrNull, arguments_parseDateTime64BestEffortUSOrNull, returned_value_parseDateTime64BestEffortUSOrNull, examples_parseDateTime64BestEffortUSOrNull, introduced_in_parseDateTime64BestEffortUSOrNull, category_parseDateTime64BestEffortUSOrNull};

    factory.registerFunction<detail::FunctionParseDateTime64BestEffortUSOrNull>(parseDateTime64BestEffortUSOrNull_documentation);

    /// toIntervalSecond documentation
    FunctionDocumentation::Description description_toIntervalSecond = R"(
Returns an interval of `n` seconds of data type [`IntervalSecond`](../data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalSecond = "toIntervalSecond(n)";
    FunctionDocumentation::Arguments arguments_toIntervalSecond = {
        {"n", "Number of seconds. Integer numbers or string representations thereof, and float numbers.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalSecond = {"Returns an interval of `n` seconds.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalSecond = {
    {
        "Usage example",
        R"(
WITH
    toDate('2025-06-15') AS date,
    toIntervalSecond(30) AS interval_to_seconds
SELECT date + interval_to_seconds AS result
        )",
        R"(
┌──────────────result─┐
│ 2025-06-15 00:00:30 │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalSecond = {1, 1};
    FunctionDocumentation::Category category_toIntervalSecond = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalSecond = {description_toIntervalSecond, syntax_toIntervalSecond, arguments_toIntervalSecond, returned_value_toIntervalSecond, examples_toIntervalSecond, introduced_in_toIntervalSecond, category_toIntervalSecond};

    /// toIntervalMinute documentation
    FunctionDocumentation::Description description_toIntervalMinute = R"(
Returns an interval of `n` minutes of data type [`IntervalMinute`](../data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalMinute = "toIntervalMinute(n)";
    FunctionDocumentation::Arguments arguments_toIntervalMinute = {
        {"n", "Number of minutes. Integer numbers or string representations thereof, and float numbers.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalMinute = {"Returns an interval of `n` minutes.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalMinute = {
    {
        "Usage example",
        R"(
WITH
    toDate('2025-06-15') AS date,
    toIntervalMinute(12) AS interval_to_minutes
SELECT date + interval_to_minutes AS result
        )",
        R"(
┌──────────────result─┐
│ 2025-06-15 00:12:00 │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalMinute = {1, 1};
    FunctionDocumentation::Category category_toIntervalMinute = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalMinute = {description_toIntervalMinute, syntax_toIntervalMinute, arguments_toIntervalMinute, returned_value_toIntervalMinute, examples_toIntervalMinute, introduced_in_toIntervalMinute, category_toIntervalMinute};

    /// toIntervalHour documentation
    FunctionDocumentation::Description description_toIntervalHour = R"(
Returns an interval of `n` hours of data type [`IntervalHour`](../data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalHour = "toIntervalHour(n)";
    FunctionDocumentation::Arguments arguments_toIntervalHour = {
        {"n", "Number of hours. Integer numbers or string representations thereof, and float numbers.", {"Int*", "UInt*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalHour = {"Returns an interval of `n` hours.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalHour = {
    {
        "Usage example",
        R"(
WITH
    toDate('2025-06-15') AS date,
    toIntervalHour(12) AS interval_to_hours
SELECT date + interval_to_hours AS result
        )",
        R"(
┌──────────────result─┐
│ 2025-06-15 12:00:00 │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalHour = {1, 1};
    FunctionDocumentation::Category category_toIntervalHour = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalHour = {description_toIntervalHour, syntax_toIntervalHour, arguments_toIntervalHour, returned_value_toIntervalHour, examples_toIntervalHour, introduced_in_toIntervalHour, category_toIntervalHour};

    /// toIntervalDay documentation
    FunctionDocumentation::Description description_toIntervalDay = R"(
Returns an interval of `n` days of data type [`IntervalDay`](../data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalDay = "toIntervalDay(n)";
    FunctionDocumentation::Arguments arguments_toIntervalDay = {
        {"n", "Number of days. Integer numbers or string representations thereof, and float numbers.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalDay = {"Returns an interval of `n` days.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalDay = {
    {
        "Usage example",
        R"(
WITH
    toDate('2025-06-15') AS date,
    toIntervalDay(5) AS interval_to_days
SELECT date + interval_to_days AS result
        )",
        R"(
┌─────result─┐
│ 2025-06-20 │
└────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalDay = {1, 1};
    FunctionDocumentation::Category category_toIntervalDay = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalDay = {description_toIntervalDay, syntax_toIntervalDay, arguments_toIntervalDay, returned_value_toIntervalDay, examples_toIntervalDay, introduced_in_toIntervalDay, category_toIntervalDay};

    /// toIntervalNanosecond documentation
    FunctionDocumentation::Description description_toIntervalNanosecond = R"(
Returns an interval of `n` nanoseconds of data type [`IntervalNanosecond`](../../sql-reference/data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalNanosecond = "toIntervalNanosecond(n)";
    FunctionDocumentation::Arguments arguments_toIntervalNanosecond = {
        {"n", "Number of nanoseconds.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalNanosecond = {"Returns an interval of `n` nanoseconds.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalNanosecond = {
    {
        "Usage example",
        R"(
WITH
    toDateTime('2025-06-15') AS date,
    toIntervalNanosecond(30) AS interval_to_nanoseconds
SELECT date + interval_to_nanoseconds AS result
        )",
        R"(
┌────────────────────────result─┐
│ 2025-06-15 00:00:00.000000030 │
└───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalNanosecond = {22, 6};
    FunctionDocumentation::Category category_toIntervalNanosecond = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalNanosecond = {description_toIntervalNanosecond, syntax_toIntervalNanosecond, arguments_toIntervalNanosecond, returned_value_toIntervalNanosecond, examples_toIntervalNanosecond, introduced_in_toIntervalNanosecond, category_toIntervalNanosecond};

    /// toIntervalMicrosecond documentation
    FunctionDocumentation::Description description_toIntervalMicrosecond = R"(
Returns an interval of `n` microseconds of data type [`IntervalMicrosecond`](../../sql-reference/data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalMicrosecond = "toIntervalMicrosecond(n)";
    FunctionDocumentation::Arguments arguments_toIntervalMicrosecond = {
        {"n", "Number of microseconds.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalMicrosecond = {"Returns an interval of `n` microseconds.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalMicrosecond = {
    {
        "Usage example",
        R"(
WITH
    toDateTime('2025-06-15') AS date,
    toIntervalMicrosecond(30) AS interval_to_microseconds
SELECT date + interval_to_microseconds AS result
        )",
        R"(
┌─────────────────────result─┐
│ 2025-06-15 00:00:00.000030 │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalMicrosecond = {22, 6};
    FunctionDocumentation::Category category_toIntervalMicrosecond = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalMicrosecond = {description_toIntervalMicrosecond, syntax_toIntervalMicrosecond, arguments_toIntervalMicrosecond, returned_value_toIntervalMicrosecond, examples_toIntervalMicrosecond, introduced_in_toIntervalMicrosecond, category_toIntervalMicrosecond};

    /// toIntervalMillisecond documentation
    FunctionDocumentation::Description description_toIntervalMillisecond = R"(
Returns an interval of `n` milliseconds of data type [IntervalMillisecond](../../sql-reference/data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalMillisecond = "toIntervalMillisecond(n)";
    FunctionDocumentation::Arguments arguments_toIntervalMillisecond = {
        {"n", "Number of milliseconds.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalMillisecond = {"Returns an interval of `n` milliseconds.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalMillisecond = {
    {
        "Usage example",
        R"(
WITH
    toDateTime('2025-06-15') AS date,
    toIntervalMillisecond(30) AS interval_to_milliseconds
SELECT date + interval_to_milliseconds AS result
        )",
        R"(
┌──────────────────result─┐
│ 2025-06-15 00:00:00.030 │
└─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalMillisecond = {22, 6};
    FunctionDocumentation::Category category_toIntervalMillisecond = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalMillisecond = {description_toIntervalMillisecond, syntax_toIntervalMillisecond, arguments_toIntervalMillisecond, returned_value_toIntervalMillisecond, examples_toIntervalMillisecond, introduced_in_toIntervalMillisecond, category_toIntervalMillisecond};

    /// toIntervalWeek documentation
    FunctionDocumentation::Description description_toIntervalWeek = R"(
Returns an interval of `n` weeks of data type [`IntervalWeek`](../../sql-reference/data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalWeek = "toIntervalWeek(n)";
    FunctionDocumentation::Arguments arguments_toIntervalWeek = {
        {"n", "Number of weeks.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalWeek = {"Returns an interval of `n` weeks.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalWeek = {
    {
        "Usage example",
        R"(
WITH
    toDate('2025-06-15') AS date,
    toIntervalWeek(1) AS interval_to_week
SELECT date + interval_to_week AS result
        )",
        R"(
┌─────result─┐
│ 2025-06-22 │
└────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalWeek = {1, 1};
    FunctionDocumentation::Category category_toIntervalWeek = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalWeek = {description_toIntervalWeek, syntax_toIntervalWeek, arguments_toIntervalWeek, returned_value_toIntervalWeek, examples_toIntervalWeek, introduced_in_toIntervalWeek, category_toIntervalWeek};

    /// toIntervalMonth documentation
    FunctionDocumentation::Description description_toIntervalMonth = R"(
Returns an interval of `n` months of data type [`IntervalMonth`](../../sql-reference/data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalMonth = "toIntervalMonth(n)";
    FunctionDocumentation::Arguments arguments_toIntervalMonth = {
        {"n", "Number of months.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalMonth = {"Returns an interval of `n` months.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalMonth = {
    {
        "Usage example",
        R"(
WITH
    toDate('2025-06-15') AS date,
    toIntervalMonth(1) AS interval_to_month
SELECT date + interval_to_month AS result
        )",
        R"(
┌─────result─┐
│ 2025-07-15 │
└────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalMonth = {1, 1};
    FunctionDocumentation::Category category_toIntervalMonth = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalMonth = {description_toIntervalMonth, syntax_toIntervalMonth, arguments_toIntervalMonth, returned_value_toIntervalMonth, examples_toIntervalMonth, introduced_in_toIntervalMonth, category_toIntervalMonth};

    /// toIntervalQuarter documentation
    FunctionDocumentation::Description description_toIntervalQuarter = R"(
Returns an interval of `n` quarters of data type [`IntervalQuarter`](../../sql-reference/data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalQuarter = "toIntervalQuarter(n)";
    FunctionDocumentation::Arguments arguments_toIntervalQuarter = {
        {"n", "Number of quarters.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalQuarter = {"Returns an interval of `n` quarters.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalQuarter = {
    {
        "Usage example",
        R"(
WITH
    toDate('2025-06-15') AS date,
    toIntervalQuarter(1) AS interval_to_quarter
SELECT date + interval_to_quarter AS result
        )",
        R"(
┌─────result─┐
│ 2025-09-15 │
└────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalQuarter = {1, 1};
    FunctionDocumentation::Category category_toIntervalQuarter = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalQuarter = {description_toIntervalQuarter, syntax_toIntervalQuarter, arguments_toIntervalQuarter, returned_value_toIntervalQuarter, examples_toIntervalQuarter, introduced_in_toIntervalQuarter, category_toIntervalQuarter};

    /// toIntervalYear documentation
    FunctionDocumentation::Description description_toIntervalYear = R"(
Returns an interval of `n` years of data type [`IntervalYear`](../../sql-reference/data-types/special-data-types/interval.md).
    )";
    FunctionDocumentation::Syntax syntax_toIntervalYear = "toIntervalYear(n)";
    FunctionDocumentation::Arguments arguments_toIntervalYear = {
        {"n", "Number of years.", {"(U)Int*", "Float*", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toIntervalYear = {"Returns an interval of `n` years.", {"Interval"}};
    FunctionDocumentation::Examples examples_toIntervalYear = {
    {
        "Usage example",
        R"(
WITH
    toDate('2024-06-15') AS date,
    toIntervalYear(1) AS interval_to_year
SELECT date + interval_to_year AS result
        )",
        R"(
┌─────result─┐
│ 2025-06-15 │
└────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toIntervalYear = {1, 1};
    FunctionDocumentation::Category category_toIntervalYear = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toIntervalYear = {description_toIntervalYear, syntax_toIntervalYear, arguments_toIntervalYear, returned_value_toIntervalYear, examples_toIntervalYear, introduced_in_toIntervalYear, category_toIntervalYear};

    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalNanosecond, detail::PositiveMonotonicity>>(documentation_toIntervalNanosecond);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalMicrosecond, detail::PositiveMonotonicity>>(documentation_toIntervalMicrosecond);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalMillisecond, detail::PositiveMonotonicity>>(documentation_toIntervalMillisecond);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalSecond, detail::PositiveMonotonicity>>(documentation_toIntervalSecond);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalMinute, detail::PositiveMonotonicity>>(documentation_toIntervalMinute);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalHour, detail::PositiveMonotonicity>>(documentation_toIntervalHour);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalDay, detail::PositiveMonotonicity>>(documentation_toIntervalDay);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalWeek, detail::PositiveMonotonicity>>(documentation_toIntervalWeek);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalMonth, detail::PositiveMonotonicity>>(documentation_toIntervalMonth);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalQuarter, detail::PositiveMonotonicity>>(documentation_toIntervalQuarter);
    factory.registerFunction<detail::FunctionConvert<DataTypeInterval, detail::NameToIntervalYear, detail::PositiveMonotonicity>>(documentation_toIntervalYear);
}

}
