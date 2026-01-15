#pragma once

#include <Common/VersionNumber.h>
#include <base/types.h>
#include <vector>


namespace DB
{

/** Embedded reference documentation for functions.
  *
  * The advantages of embedded documentation are:
  * - it is easy to write and update with code;
  * - it is easy to make sure that the documentation exists;
  * - the documentation can be introspected by the queries on running server;
  * - the documentation can be extracted by external tools such as SQL editors
  *   in machine-readable form and presented in human readable form;
  * - the documentation can be generated in various formats;
  * - it is easy to generate documentation with information about the version when the feature appeared or changed;
  * - it is easy to point to the source code from the documentation;
  * - it is easy to point to the tests that covered every component, and order the tests by relevance;
  * - it is easy to point to the authors of every feature;
  * - the examples from the documentation can be automatically validated to be always correct and serve the role of tests;
  * - no cross-team communication impedance;
  *
  * The disadvantages of embedded documentation are:
  * - it is only suitable for uniform components of the system and not suitable for tutorials and overviews;
  * - it is more difficult to edit by random readers;
  *
  * The documentation can contain:
  * - description (the main text);
  * - examples (queries that can be referenced from the text by names);
  * - a category (for example "Mathematical" or "Array Processing");
  *
  * The description should be represented in Markdown (or just plaintext).
  * Some extensions for Markdown are added:
  * - [example:name] will reference to an example with the corresponding name.
  *
  * Documentation does not support multiple languages.
  * The only available language is English.
  */
struct FunctionDocumentation
{
    using Description = String;

    using Syntax = String;

    struct Argument
    {
        String name;                     /// E.g. "y"
        String description;              /// E.g. "The divisor."
        std::vector<String> types = {};  /// E.g. {"(U)Int*", "Float*"}
                                         /// Default initialized only during a transition period, see 'argumentsAsString'.
    };

    using Arguments = std::vector<Argument>;  /// For all functions
    using Parameters = std::vector<Argument>; /// For aggregate functions

    struct ReturnedValue
    {
        String description;              /// E.g. "Returns the remainder of the division."
        std::vector<String> types = {};  /// E.g. {"Float32"}
                                         /// Default initialized only during a transition period
    };

    struct Example
    {
        String name;
        String query;
        String result;
    };
    using Examples = std::vector<Example>;

    using IntroducedIn = VersionNumber;
    static constexpr VersionNumber VERSION_UNKNOWN;

    enum class Category : uint8_t
    {
        /// Default category
        Unknown,

        /// Regular functions
        Arithmetic,
        Array,
        Bit,
        Bitmap,
        Comparison,
        Conditional,
        DateAndTime,
        Decimal,
        Dictionary,
        Dynamic,
        Distance,
        EmbeddedDictionary,
        Geo,
        Encoding,
        Encryption,
        Financial,
        Hash,
        IPAddress,
        Introspection,
        JSON,
        Logical,
        MachineLearning,
        Map,
        Mathematical,
        NLP,
        Null,
        NumericIndexedVector,
        Other,
        QBit,
        RandomNumber,
        Rounding,
        StringReplacement,
        StringSearch,
        StringSplitting,
        String,
        TimeSeries,
        TimeWindow,
        Tuple,
        TypeConversion,
        ULID,
        URL,
        UUID,
        UniqTheta,
        Variant,

        /// Other types of functions
        AggregateFunction,
        TableFunction
    };

    using Related = std::vector<String>;

    /// TODO Fields with {} initialization are optional. We should make all fields non-optional.
    Description description;                      /// E.g. "Returns the position (in bytes, starting at 1) of a substring needle in a string haystack."
    Syntax syntax {};                             /// E.g. "position(haystack, needle)"
    Arguments arguments {};                       /// E.g. {{"haystack", "String in which the search is performed.", {"String"}},
                                                  ///       {"needle", "Substring to be searched.", {"String"}}}
    /// Parameters parameters {};
    ReturnedValue returned_value {};              /// E.g. {"Starting position in bytes and counting from 1, if the substring was found.", {"(U)Int*"}}
    Examples examples {};                         ///
    IntroducedIn introduced_in {VERSION_UNKNOWN}; /// E.g. {25, 5}
    Category category;                            /// E.g. Category::DatesAndTimes

    String syntaxAsString() const;
    String argumentsAsString() const;
    String parametersAsString() const;
    String returnedValueAsString() const;
    String examplesAsString() const;
    String introducedInAsString() const;
    String categoryAsString() const;
};
}
