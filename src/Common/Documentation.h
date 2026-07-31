#pragma once

#include <Common/VersionNumber.h>
#include <base/types.h>
#include <source_location>
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
  *
  * Every documentation object captures the path of the source file where it is defined (the `source` field).
  * It is initialized by default to the location of the (aggregate) initialization of the object, which is the
  * registration site of the corresponding component, so it points to the source code that documents it. The path
  * is as produced by the compiler; `system.documentation` exposes it normalized to be relative to the repository root.
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

    /// The source file where this documentation is defined. Captured automatically at the construction site;
    /// do not set it explicitly. See the note in the class comment above.
    /// NOTE: this only works when the object is initialized at its construction site, i.e. with aggregate/designated
    /// initialization (`Documentation doc{...}`) or value-initialization (`Documentation doc{}`). A default-initialized
    /// object (`Documentation doc;`, without braces) records this header instead, so always use braces when building
    /// the documentation field by field afterwards (or set `source` explicitly).
    const char * source = std::source_location::current().file_name();

    String syntaxAsString() const;
    String examplesAsString() const;
    String introducedInAsString() const;
};

}
