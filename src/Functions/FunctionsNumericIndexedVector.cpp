#include <Functions/FunctionFactory.h>

/// Include this last — see the reason inside
#include <Functions/FunctionsNumericIndexedVector.h>

namespace DB
{

REGISTER_FUNCTION(NumericIndexedVector)
{
    factory.registerFunction<FunctionNumericIndexedVectorBuild>(FunctionDocumentation{
        .description
        = R"(Creates a NumericIndexedVector from a map. The map’s keys represent the vector's index and map's value represents the vector's value.)",
        .syntax = "SELECT numericIndexedVectorBuild(map)",
        .arguments = {{"map", "A mapping from index to value."}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
            "SELECT numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])) AS res, toTypeName(res);",
            R"(
┌─res─┬─toTypeName(res)────────────────────────────────────────────┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │
└─────┴────────────────────────────────────────────────────────────┘
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseAdd>(FunctionDocumentation{
        .description
        = R"(Performs pointwise addition between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector.)",
        .syntax = "numericIndexedVectorPointwiseAdd(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec1, 2)) AS res2;
            )",
            R"(
┌─res1──────────────────┬─res2─────────────┐
│ {1:10,2:30,3:50,4:30} │ {1:12,2:22,3:32} │
└───────────────────────┴──────────────────┘
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseSubtract>(FunctionDocumentation{
        .description
        = R"(Performs pointwise subtraction between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector.)",
        .syntax = "numericIndexedVectorPointwiseSubtract(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
            R"(
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec1, 2)) AS res2;
            )",
            R"(
┌─res1───────────────────┬─res2────────────┐
│ {1:10,2:10,3:10,4:-30} │ {1:8,2:18,3:28} │
└────────────────────────┴─────────────────┘
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseMultiply>(FunctionDocumentation{
        .description
        = R"(Performs pointwise multiplication between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector.)",
        .syntax = "numericIndexedVectorPointwiseMultiply(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
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
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseDivide>(FunctionDocumentation{
        .description
        = R"(Performs pointwise division between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector.)",
        .syntax = "numericIndexedVectorPointwiseDivide(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
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
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseEqual>(FunctionDocumentation{
        .description
        = R"(Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the values are equal, with all corresponding value set to 1.)",
        .syntax = "numericIndexedVectorPointwiseEqual(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
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
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseNotEqual>(FunctionDocumentation{
        .description
        = R"(Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant.  The result is a NumericIndexedVector containing the indices where the values are not equal, with all corresponding value set to 1.)",
        .syntax = "numericIndexedVectorPointwiseNotEqual(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
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
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseLess>(FunctionDocumentation{
        .description
        = R"(Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is less than the second vector’s value, with all corresponding value set to 1.)",
        .syntax = "numericIndexedVectorPointwiseLess(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
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
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseLessEqual>(FunctionDocumentation{
        .description
        = R"(Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is less than or equal to the second vector’s value, with all corresponding value set to 1.)",
        .syntax = "numericIndexedVectorPointwiseLessEqual(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
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
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseGreater>(FunctionDocumentation{
        .description
        = R"(Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is greater than the second vector’s value, with all corresponding value set to 1.)",
        .syntax = "numericIndexedVectorPointwiseGreater(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
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
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorPointwiseGreaterEqual>(FunctionDocumentation{
        .description
        = R"(Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is greater than or equal to the second vector’s value, with all corresponding value set to 1.)",
        .syntax = "numericIndexedVectorPointwiseGreaterEqual(numericIndexedVector, numericIndexedVector | numeric)",
        .arguments = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"numeric", "A numeric constant"}},
        .returned_value = {"NumericIndexedVector object."},
        .examples
        = {{"",
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
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorGetValueImpl>(FunctionDocumentation{
        .description = R"(Retrieves the value corresponding to a specified index.)",
        .syntax = "numericIndexedVectorGetValue(numericIndexedVector, index)",
        .arguments
        = {{"numericIndexedVector", "A NumericIndexedVector object."}, {"index", "The index for which the value is to be retrieved."}},
        .returned_value = {"A Numeric value with the same type as the value type of NumericIndexedVector."},
        .examples
        = {{"",
            R"(
SELECT numericIndexedVectorGetValue(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])), 3) AS res;
            )",
            R"(
┌─res─┐
│  30 │
└─────┘
            )"}},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorCardinality>(FunctionDocumentation{
        .description = R"(Returns the cardinality (number of unique indexes) of the NumericIndexedVector.)",
        .syntax = "numericIndexedVectorCardinality(numericIndexedVector)",
        .arguments = {
            {"numericIndexedVector", "A NumericIndexedVector object."},
            },
        .returned_value = {"Numeric value which type is UInt"},
        .examples = {{
            "",
            R"(
SELECT numericIndexedVectorCardinality(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
            )",
            R"(
┌─res─┐
│  3  │
└─────┘
            )"
        }},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorAllValueSum>(FunctionDocumentation{
        .description = R"(Returns sum of all the value in NumericIndexedVector.)",
        .syntax = "numericIndexedVectorAllValueSum(numericIndexedVector)",
        .arguments = {
            {"numericIndexedVector", "A NumericIndexedVector object."},
            },
        .returned_value = {"Numeric value which type is Float64"},
        .examples = {{
            "",
            R"(
SELECT numericIndexedVectorAllValueSum(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
            )",
            R"(
┌─res─┐
│  60 │
└─────┘
            )"
        }},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorShortDebugString>(FunctionDocumentation{
        .description = R"(Returns internal information of the NumericIndexedVector in a json format. This function is primarily used for debugging purposes.)",
        .syntax = "numericIndexedVectorShortDebugString(numericIndexedVector)",
        .arguments = {
            {"numericIndexedVector", "A NumericIndexedVector object."},
            },
        .returned_value = {"String"},
        .examples = {{
            "",
            R"(
SELECT numericIndexedVectorShortDebugString(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res\G;
            )",
            R"(
Row 1:
──────
res: {"vector_type":"BSI","index_type":"char8_t","value_type":"char8_t","integer_bit_num":8,"fraction_bit_num":0,"zero_indexes_info":{"cardinality":"0"},"non_zero_indexes_info":{"total_cardinality":"3","all_value_sum":60,"number_of_bitmaps":"8","bitmap_info":{"cardinality":{"0":"0","1":"2","2":"2","3":"2","4":"2","5":"0","6":"0","7":"0"}}}}
            )"
        }},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
    factory.registerFunction<FunctionNumericIndexedVectorToMap>(FunctionDocumentation{
        .description = R"(Converts a NumericIndexedVector to a map.)",
        .syntax = "numericIndexedVectorToMap(numericIndexedVector)",
        .arguments = {
            {"numericIndexedVector", "A NumericIndexedVector object."},
            },
        .returned_value = {"Map(IndexType, ValueType)"},
        .examples = {{
            "",
            R"(
SELECT numericIndexedVectorToMap(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
            )",
            R"(
┌─res──────────────┐
│ {1:10,2:20,3:30} │
└──────────────────┘
            )"
        }},
        .introduced_in = {25, 7},
        .category = FunctionDocumentation::Category::Arithmetic,
    });
}
}
