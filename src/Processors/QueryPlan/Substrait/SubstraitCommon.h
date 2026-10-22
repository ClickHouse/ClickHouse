#pragma once

#include "config.h"

#if USE_SUBSTRAIT

#include <expected>
#include <map>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <base/types.h>
#include <substrait/plan.pb.h>
#include <substrait/type.pb.h>

/// Objects shared by all layers of the Substrait serializer, and the Substrait vocabulary they are built on.
/// * Uniform Resource Name (URN). A name that identifies one extension YAML, for example "extension:io.substrait:functions_arithmetic".
/// * Anchor. An integer a plan assigns to one extension function or type. The plan header declares each name with its anchor and URN once.
///   Call sites then carry the anchor only. Anchors start at 1. The spec discourages 0 because it reads as unset.
namespace DB
{

/// Options of one function call, as (option name, value) pairs. An option names a behavior choice, for example overflow handling.
using FunctionOptions = std::vector<std::pair<std::string_view, std::string_view>>;

/// Writes the options onto a function call message.
template <typename FunctionMessage>
void addFunctionOptions(const FunctionOptions & options, FunctionMessage & function)
{
    for (const auto & [name, preference] : options)
    {
        auto * option = function.add_options();
        option->set_name(name);
        option->add_preference(preference);
    }
}

/// The blocking reason as an error, for expected-returning converters.
inline std::unexpected<String> blocked(String reason) { return std::unexpected(std::move(reason)); }

namespace SubstraitExtensionURN
{
static constexpr std::string_view arithmetic = "extension:io.substrait:functions_arithmetic";
static constexpr std::string_view boolean = "extension:io.substrait:functions_boolean";
static constexpr std::string_view comparison = "extension:io.substrait:functions_comparison";
static constexpr std::string_view string = "extension:io.substrait:functions_string";
static constexpr std::string_view rounding = "extension:io.substrait:functions_rounding";
static constexpr std::string_view logarithmic = "extension:io.substrait:functions_logarithmic";
static constexpr std::string_view aggregate_generic = "extension:io.substrait:functions_aggregate_generic";
static constexpr std::string_view unsigned_integers = "extension:io.substrait:unsigned_integers";
}

/// The extension declarations of one plan, accumulated while the plan converts. Starts empty.
/// The first use of a function or type assigns its anchor, so call sites can embed the number.
/// declareIn then writes the collected declarations into the plan header. One instance per plan, shared by all converters.
class SubstraitPlanExtensions
{
public:
    /// `signature` identifies one implementation, for example "add:i64_i64".
    UInt32 internFunction(std::string_view urn, std::string_view signature);

    /// `type_name` as declared in the extension YAML, for example "u64".
    UInt32 internType(std::string_view urn, std::string_view type_name);

    /// Gives back the `type_name` that internType registered. An unknown anchor is our bug and throws.
    const String & typeNameByAnchor(UInt32 anchor) const;

    /// Writes the header declarations that bind each URN, function, and type to its anchor.
    void declareIn(substrait::Plan & plan) const;

private:
    void registerUrn(std::string_view urn);

    /// std::map so the same query always produces identical plan.
    std::map<String, UInt32> urns;
    std::map<std::pair<String, String>, UInt32> functions;
    std::map<std::pair<String, String>, UInt32> types;
};

struct SubstraitColumn
{
    String name;
    substrait::Type type;
};

/// The columns a converted subtree produces, in order. The Substrait-side mirror of the step's output header.
/// Parents use it to resolve names into positions. ClickHouse references columns by name, Substrait by position.
/// A type here is what the conversion emitted, which is not always derivable from the ClickHouse type.
struct SubstraitSchema
{
    std::vector<SubstraitColumn> columns;

    /// On duplicate names, gives the first position.
    std::optional<size_t> positionByName(const String & name) const
    {
        if (const auto it = positions.find(name); it != positions.end())
            return it->second;
        return {};
    }

    size_t size() const { return columns.size(); }

    void append(String name, substrait::Type type)
    {
        positions.try_emplace(name, columns.size());
        columns.push_back({std::move(name), std::move(type)});
    }

    void appendAll(const SubstraitSchema & other)
    {
        for (const auto & column : other.columns)
            append(column.name, column.type);
    }

private:
    std::unordered_map<String, size_t> positions;
};

}

#endif
