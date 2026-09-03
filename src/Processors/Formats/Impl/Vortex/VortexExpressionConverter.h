#pragma once

#include "config.h"

#if USE_VORTEX

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/Vortex/VortexFFIHelpers.h>

#include <optional>
#include <string_view>
#include <unordered_map>

namespace arrow
{
class DataType;
class Field;
class Schema;
}

namespace DB
{
class RPNBuilderTreeNode;
class RPNBuilderFunctionTreeNode;
}

namespace DB::Vortex
{

/// Translates nodes of the query's filter `ActionsDAG` into Vortex filter expressions. Passing the
/// result to the scan lets it skip whole statistics zones instead of decoding them.
///
/// The converter expects the DAG to have gone through `ActionsDAGWithInversionPushDown` first: NOTs
/// are pushed into the atoms (`NOT (a < 5)` arrives as `a >= 5`), aliases are stripped, and a `not`
/// node survives only over functions that have no inverse.
///
/// Not every predicate can be translated, and a failed subtree is not an error: `tryConvert`
/// returns null and the caller leaves that part of the filter for ClickHouse, which reapplies the
/// full WHERE to the scan's result in any case. Because of that, a translation may be *widened* -
/// made to keep more rows than the predicate - but must never lose a row the query needs. Widening
/// is only sound while the subtree is in a positive position; under a `not` every node has to
/// translate exactly, which `allow_widening = false` enforces.
///
/// An atom translates only when the comparison provably means the same thing on both sides: the
/// column's header type and file type must describe the same ordered set of values, and the
/// constant must fit the file type exactly. Adding support for one more function is one entry in
/// the dispatch table; adding one more literal type is one case in the type matching and literal
/// building switches (usually backed by a `vortex_ffi_expr_literal_*` FFI builder).
class VortexExpressionConverter
{
public:
    VortexExpressionConverter(const Block & header_, const arrow::Schema & file_schema_, const FormatSettings & format_settings_);

    /// Translates the subtree rooted at `node`, exactly or - in the positions where
    /// `allow_widening` permits - keeping at least every matching row. Returns null when the
    /// subtree cannot be translated. Throws only on logical errors.
    VortexExpressionPtr tryConvert(const RPNBuilderTreeNode & node, bool allow_widening) const;

private:
    /// A column reference that passed every pushdown gate: it exists in both the header and the
    /// file, its null-ness is not substituted away, and - unless the caller opted out - the two
    /// types describe the same set of values. `cmp_type` is the header type with `Nullable` and
    /// `LowCardinality` peeled off; constants are converted into it before becoming literals.
    struct ResolvedColumn
    {
        VortexExpressionPtr expr;
        std::shared_ptr<arrow::Field> field;
        DataTypePtr cmp_type;
    };
    enum class TypeMatch
    {
        Required,
        /// For type-independent atoms: `IS NULL` does not compare values, so the value sets do not
        /// have to match.
        NotRequired,
    };
    std::optional<ResolvedColumn> resolveColumn(const RPNBuilderTreeNode & node, TypeMatch type_match) const;

    /// Whether the two types describe the same set of values. Only then does a comparison mean the
    /// same thing on both sides. `UInt64` in the header against `I64` in the file is the trap: the
    /// same bits, but a different order.
    bool typesMatchForFilterPushdown(const DataTypePtr & cmp_type, const arrow::DataType & arrow_type) const;

    /// Builds a literal in the file column's own type. `value` is converted into `cmp_type` first,
    /// exactly or not at all. Returns null when the value does not fit, and the atom is then not
    /// pushed down.
    VortexExpressionPtr
    makeLiteral(const arrow::DataType & file_type, const DataTypePtr & cmp_type, const Field & value, const DataTypePtr & value_type) const;

    /// `column >= prefix AND column < firstStringThatIsGreaterThanAllStringsWithPrefix(prefix)`:
    /// exactly the strings starting with `prefix`. When the right bound does not exist or cannot
    /// become a literal, the range degrades to its left half - a widening, so it needs
    /// `allow_widening` unless the right bound is absent because no string is greater than the
    /// prefix (then the left half alone is still exact).
    VortexExpressionPtr makePrefixRange(const ResolvedColumn & column, const String & prefix, bool allow_widening) const;

    using Handler = VortexExpressionPtr (VortexExpressionConverter::*)(const RPNBuilderFunctionTreeNode &, bool allow_widening) const;
    static const std::unordered_map<std::string_view, Handler> & handlers();

    VortexExpressionPtr convertAnd(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertOr(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertNot(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertComparison(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertIsNull(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertIsNotNull(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertIn(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertLike(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertNotLike(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;
    VortexExpressionPtr convertStartsWith(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const;

    /// A column standing on its own as the predicate, `WHERE flag`. Only a boolean file column may
    /// be handed to the scan as a filter of its own.
    VortexExpressionPtr convertBareBooleanColumn(const RPNBuilderTreeNode & node) const;

    const Block & header;
    const arrow::Schema & file_schema;
    const FormatSettings & format_settings;
};

}

#endif
