#include <Functions/hilbertEncode.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(HilbertEncode)
{
    factory.registerFunction<FunctionHilbertEncode>(FunctionDocumentation{
    .description=R"(
Calculates code for Hilbert Curve for a list of unsigned integers

The function has two modes of operation:
- Simple
- Expanded

Simple: accepts up to 2 unsigned integers as arguments and produces a UInt64 code.
[example:simple]

Expanded: accepts a range mask (tuple) as a first argument and up to 2 unsigned integers as other arguments.
Each number in mask configures the amount of bits that corresponding argument will be shifted left
[example:range_expanded]
Note: tuple size must be equal to the number of the other arguments

Range expansion can be beneficial when you need a similar distribution for arguments with wildly different ranges (or cardinality)
For example: 'IP Address' (0...FFFFFFFF) and 'Country code' (0...FF)

Hilbert encoding for one argument is always the argument itself.
[example:identity]
Produces: `1`

You can expand one argument too:
[example:identity_expanded]
Produces: `512`

The function also accepts columns as arguments:
[example:from_table]

But the range tuple must still be a constant:
[example:from_table_range]

Please note that you can fit only so much bits of information into Morton code as UInt64 has.
Two arguments will have a range of maximum 2^32 (64/2) each
All overflow will be clamped to zero
)",
    .examples{
        {"simple", "SELECT hilbertEncode(1, 2, 3)", ""},
        {"range_expanded", "SELECT hilbertEncode((1,6), 1024, 16)", ""},
        {"identity", "SELECT hilbertEncode(1)", ""},
        {"identity_expanded", "SELECT hilbertEncode(tuple(2), 128)", ""},
        {"from_table", "SELECT hilbertEncode(n1, n2) FROM table", ""},
        {"from_table_range", "SELECT hilbertEncode((1,2), n1, n2) FROM table", ""},
    },
    .categories {"Hilbert coding", "Hilbert Curve"}
    });
}

}
