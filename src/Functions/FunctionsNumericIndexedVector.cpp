#include <Functions/FunctionFactory.h>

/// Include this last — see the reason inside
#include <Functions/FunctionsNumericIndexedVector.h>

namespace DB
{

REGISTER_FUNCTION(NumericIndexedVector)
{
    /// numericIndexedVectorBuild
    {
        FunctionDocumentation::Description description = R"(
Creates a NumericIndexedVector from a map. The map's keys represent the vector's index and map's value represents the vector's value.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorBuild(map)";
        FunctionDocumentation::Arguments arguments = {
            {"map", "A mapping from index to value.", {"Map"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a NumericIndexedVector object.", {"AggregateFunction"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])) AS res, toTypeName(res);
            )",
            R"(
┌─res─┬─toTypeName(res)────────────────────────────────────────────┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │
└─────┴────────────────────────────────────────────────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorBuild>(documentation);
    }
    /// numericIndexedVectorPointwiseAdd
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise addition between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseAdd(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec1, 2)) AS res2;
            )",
            R"(
┌─res1──────────────────┬─res2─────────────┐
│ {1:10,2:30,3:50,4:30} │ {1:12,2:22,3:32} │
└───────────────────────┴──────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseAdd>(documentation);
    }
    /// numericIndexedVectorPointwiseSubtract
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise subtraction between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseSubtract(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec1, 2)) AS res2;
            )",
            R"(
┌─res1───────────────────┬─res2────────────┐
│ {1:10,2:10,3:10,4:-30} │ {1:8,2:18,3:28} │
└────────────────────────┴─────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseSubtract>(documentation);
    }
    /// numericIndexedVectorPointwiseMultiply
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise multiplication between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseMultiply(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
            {"",
                R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec1, 2)) AS res2;
                )",
                R"(
┌─res1──────────┬─res2─────────────┐
│ {2:200,3:600} │ {1:20,2:40,3:60} │
└───────────────┴──────────────────┘
                )"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseMultiply>(documentation);
    }
    /// numericIndexedVectorPointwiseDivide
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise division between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseDivide(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec1, 2)) AS res2;
            )",
            R"(
┌─res1────────┬─res2────────────┐
│ {2:2,3:1.5} │ {1:5,2:10,3:15} │
└─────────────┴─────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseDivide>(documentation);
    }
    /// numericIndexedVectorPointwiseEqual
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise comparison between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
The result is a numericIndexedVector containing the indices where the values are equal, with all corresponding values set to 1.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseEqual(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {"",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec1, 20)) AS res2;
            )",
            R"(
┌─res1──┬─res2──┐
│ {2:1} │ {2:1} │
└───────┴───────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseEqual>(documentation);
    }
    /// numericIndexedVectorPointwiseNotEqual
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise comparison between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
The result is a numericIndexedVector containing the indices where the values are not equal, with all corresponding values set to 1.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseNotEqual(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec1, 20)) AS res2;
            )",
            R"(
┌─res1──────────┬─res2──────┐
│ {1:1,3:1,4:1} │ {1:1,3:1} │
└───────────────┴───────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseNotEqual>(documentation);
    }
    /// numericIndexedVectorPointwiseLess
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise comparison between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
The result is a numericIndexedVector containing the indices where the first vector's value is less than the second vector's value, with all corresponding values set to 1.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseLess(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec1, 20)) AS res2;
            )",
            R"(
┌─res1──────┬─res2──┐
│ {3:1,4:1} │ {1:1} │
└───────────┴───────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseLess>(documentation);
    }
    /// numericIndexedVectorPointwiseLessEqual
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise comparison between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
The result is a numericIndexedVector containing the indices where the first vector's value is less than or equal to the second vector's value, with all corresponding values set to 1.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseLessEqual(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec1, 20)) AS res2;
            )",
            R"(
┌─res1──────────┬─res2──────┐
│ {2:1,3:1,4:1} │ {1:1,2:1} │
└───────────────┴───────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseLessEqual>(documentation);
    }
    /// numericIndexedVectorPointwiseGreater
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise comparison between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
The result is a numericIndexedVector containing the indices where the first vector's value is greater than the second vector's value, with all corresponding values set to 1.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseGreater(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec1, 20)) AS res2;
            )",
            R"(
┌─res1──────┬─res2──┐
│ {1:1,3:1} │ {3:1} │
└───────────┴───────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseGreater>(documentation);
    }
    /// numericIndexedVectorPointwiseGreaterEqual
    {
        FunctionDocumentation::Description description = R"(
Performs pointwise comparison between a numericIndexedVector and either another numericIndexedVector or a numeric constant.
The result is a numericIndexedVector containing the indices where the first vector's value is greater than or equal to the second vector's value, with all corresponding values set to 1.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorPointwiseGreaterEqual(v1, v2)";
        FunctionDocumentation::Arguments arguments = {
            {"v1", "", {"numericIndexedVector"}},
            {"v2", "A numeric constant or numericIndexedVector object.", {"(U)Int*", "Float*", "numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a new numericIndexedVector object.", {"numericIndexedVector"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec1, 20)) AS res2;
            )",
            R"(
┌─res1──────────┬─res2──────┐
│ {1:1,2:1,3:1} │ {2:1,3:1} │
└───────────────┴───────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorPointwiseGreaterEqual>(documentation);
    }
    /// numericIndexedVectorGetValue
    {
        FunctionDocumentation::Description description = R"(
Retrieves the value corresponding to a specified index from a numericIndexedVector.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorGetValue(v, i)";
        FunctionDocumentation::Arguments arguments = {
            {"v", "", {"numericIndexedVector"}},
            {"i", "The index for which the value is to be retrieved.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"A numeric value with the same type as the value type of NumericIndexedVector.", {"(U)Int*", "Float*"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT numericIndexedVectorGetValue(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])), 3) AS res;
            )",
            R"(
┌─res─┐
│  30 │
└─────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorGetValueImpl>(documentation);
    }
    /// numericIndexedVectorCardinality
    {
        FunctionDocumentation::Description description = R"(
Returns the cardinality (number of unique indexes) of the numericIndexedVector.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorCardinality(v)";
        FunctionDocumentation::Arguments arguments = {
            {"v", "", {"numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of unique indexes.", {"UInt64"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT numericIndexedVectorCardinality(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
            )",
            R"(
┌─res─┐
│  3  │
└─────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorCardinality>(documentation);
    }
    /// numericIndexedVectorAllValueSum
    {
        FunctionDocumentation::Description description = R"(
Returns the sum of all values in the numericIndexedVector.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorAllValueSum(v)";
        FunctionDocumentation::Arguments arguments = {
            {"v", "", {"numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the sum.", {"Float64"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT numericIndexedVectorAllValueSum(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
            )",
            R"(
┌─res─┐
│  60 │
└─────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorAllValueSum>(documentation);
    }
    /// numericIndexedVectorShortDebugString
    {
        FunctionDocumentation::Description description = R"(
Returns internal information of the numericIndexedVector in JSON format.
This function is primarily used for debugging purposes.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorShortDebugString(v)";
        FunctionDocumentation::Arguments arguments = {
            {"v", "", {"numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a JSON string containing debug information.", {"String"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT numericIndexedVectorShortDebugString(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res\G;
            )",
            R"(
Row 1:
──────
res: {"vector_type":"BSI","index_type":"char8_t","value_type":"char8_t","integer_bit_num":8,"fraction_bit_num":0,"zero_indexes_info":{"cardinality":"0"},"non_zero_indexes_info":{"total_cardinality":"3","all_value_sum":60,"number_of_bitmaps":"8","bitmap_info":{"cardinality":{"0":"0","1":"2","2":"2","3":"2","4":"2","5":"0","6":"0","7":"0"}}}}
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorShortDebugString>(documentation);
    }
    /// numericIndexedVectorToMap
    {
        FunctionDocumentation::Description description = R"(
Converts a numericIndexedVector to a map.
        )";
        FunctionDocumentation::Syntax syntax = "numericIndexedVectorToMap(v)";
        FunctionDocumentation::Arguments arguments = {
            {"v", "", {"numericIndexedVector"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a map with index-value pairs.", {"Map"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT numericIndexedVectorToMap(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
            )",
            R"(
┌─res──────────────┐
│ {1:10,2:20,3:30} │
└──────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::NumericIndexedVector;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionNumericIndexedVectorToMap>(documentation);
    }
}
}
