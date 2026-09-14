#pragma once

#include <Common/VersionNumber.h>
#include <base/types.h>
#include <vector>


namespace DB
{

/** Embedded reference documentation for uniform components of the system,
  * such as table engines, database engines, data types, and formats.
  *
  * It plays the same role for these components as `FunctionDocumentation` does for functions,
  * but with a smaller set of fields, because these components do not have arguments,
  * parameters or a return value in the same sense as functions do.
  *
  * The advantages of embedded documentation are described in `FunctionDocumentation.h`.
  *
  * The documentation can contain:
  * - description (the main text);
  * - syntax (how the component is referenced in a query);
  * - examples (queries that can be referenced from the text by names);
  * - the version when the component was introduced;
  * - a list of related components.
  *
  * The list of related components is documentation cross-reference metadata: it may name components
  * that are not available in the current build (e.g. disabled by a compile-time option), so it should
  * not be interpreted as a guarantee that every referenced component is registered in this server.
  *
  * The description should be represented in Markdown (or just plaintext).
  *
  * Documentation does not support multiple languages.
  * The only available language is English.
  */
struct Documentation
{
    using Description = String;
    using Syntax = String;

    struct Example
    {
        String name;
        String query;
        String result;
    };
    using Examples = std::vector<Example>;

    using IntroducedIn = VersionNumber;
    static constexpr VersionNumber VERSION_UNKNOWN;

    using Related = std::vector<String>;

    Description description;                       /// E.g. "The most universal and functional table engine for high-load tasks."
    Syntax syntax {};                              /// E.g. "ENGINE = MergeTree() ORDER BY expr"
    Examples examples {};                          ///
    IntroducedIn introduced_in {VERSION_UNKNOWN};  /// E.g. {25, 5}
    Related related {};                            /// E.g. {"ReplicatedMergeTree"}

    String syntaxAsString() const;
    String examplesAsString() const;
    String introducedInAsString() const;
};

}
