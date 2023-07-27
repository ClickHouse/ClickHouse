#pragma once

#include <string>
#include <vector>
#include <map>


namespace DB
{

/** Embedded reference documentation for high-level server components,
  * such as SQL functions, table functions, data types, table engines, etc.
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
  * Only the description is mandatory.
  *
  * The description should be represented in Markdown (or just plaintext).
  * Some extensions for Markdown are added:
  * - [example:name] will reference to an example with the corresponding name.
  *
  * Documentation does not support multiple languages.
  * The only available language is English.
  */
struct Documentation
{
    using Description = std::string;
    using ExampleName = std::string;
    using ExampleQuery = std::string;
    using Examples = std::map<ExampleName, ExampleQuery>;
    using Category = std::string;
    using Categories = std::vector<Category>;

    Description description;
    Examples examples;
    Categories categories;

    Documentation(Description description_) : description(std::move(description_)) {}
    Documentation(Description description_, Examples examples_) : description(std::move(description_)), examples(std::move(examples_)) {}
    Documentation(Description description_, Examples examples_, Categories categories_)
        : description(std::move(description_)), examples(std::move(examples_)), categories(std::move(categories_)) {}

    /// TODO: Please remove this constructor. Documentation should always be non-empty.
    Documentation() {}
};

}
