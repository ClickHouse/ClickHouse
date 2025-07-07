#pragma once

#include <set>
#include <string>
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
  * - it is easy to generate a documentation with information about the version when the feature appeared or changed;
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
  * - categories - one or a few text strings like {"Mathematical", "Array Processing"};
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
    using Description = std::string;

    using Syntax = std::string;

    struct Argument
    {
        std::string name;
        std::string description;
    };
    using Arguments = std::vector<Argument>;

    using ReturnedValue = std::string;

    struct Example
    {
        std::string name;
        std::string query;
        std::string result;
    };
    using Examples = std::vector<Example>;

    using Category = std::string;
    using Categories = std::set<Category>;

    using Related = std::string;

    Description description;        /// E.g. "Returns the position (in bytes, starting at 1) of a substring needle in a string haystack."
    Syntax syntax = {};             /// E.g. "position(haystack, needle)"
    Arguments arguments {};         /// E.g. ["haystack — String in which the search is performed. String.", "needle — Substring to be searched. String."]
    ReturnedValue returned_value {};/// E.g. "Starting position in bytes and counting from 1, if the substring was found."
    Examples examples {};           ///
    Categories categories {};       /// E.g. {"String Search"}

    std::string argumentsAsString() const;
    std::string examplesAsString() const;
    std::string categoriesAsString() const;
};

}
