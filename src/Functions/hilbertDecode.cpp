#include <Functions/hilbertDecode.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(HilbertDecode)
{
    factory.registerFunction<FunctionHilbertDecode>(FunctionDocumentation{
    .description=R"(
Decodes Hilbert Curve code into the corresponding unsigned integer tuple

The function has two modes of operation:
- Simple
- Expanded

Simple: accepts a resulting tuple size as a first argument and the code as a second argument.
[example:simple]
Will decode into: `(8, 0)`
The resulting tuple size cannot be more than 2

Expanded: accepts a range mask (tuple) as a first argument and the code as a second argument.
Each number in mask configures the amount of bits that corresponding argument will be shifted right
[example:range_shrank]
Note: see hilbertEncode() docs on why range change might be beneficial.
Still limited to 2 numbers at most.

Hilbert code for one argument is always the argument itself (as a tuple).
[example:identity]
Produces: `(1)`

You can shrink one argument too:
[example:identity_shrank]
Produces: `(128)`

The function accepts a column of codes as a second argument:
[example:from_table]

The range tuple must be a constant:
[example:from_table_range]
)",
        .examples{
            {"simple", "SELECT hilbertDecode(2, 64)", ""},
            {"range_shrank", "SELECT hilbertDecode((1,2), 1572864)", ""},
            {"identity", "SELECT hilbertDecode(1, 1)", ""},
            {"identity_shrank", "SELECT hilbertDecode(tuple(2), 512)", ""},
            {"from_table", "SELECT hilbertDecode(2, code) FROM table", ""},
            {"from_table_range", "SELECT hilbertDecode((1,2), code) FROM table", ""},
            },
        .categories {"Hilbert coding", "Hilbert Curve"}
    });
}

}
