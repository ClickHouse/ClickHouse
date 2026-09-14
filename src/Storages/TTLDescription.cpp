#include <Storages/TTLDescription.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVariant.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/TypeMismatchStrictness.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTTLElement.h>
#include <Storages/extractKeyExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAssignment.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/Context.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/SipHash.h>

#include <optional>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int BAD_TTL_EXPRESSION;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


TTLAggregateDescription::TTLAggregateDescription(const TTLAggregateDescription & other)
    : column_name(other.column_name)
    , expression_result_column_name(other.expression_result_column_name)
{
    if (other.expression)
        expression = other.expression->clone();
}

TTLAggregateDescription & TTLAggregateDescription::operator=(const TTLAggregateDescription & other)
{
    if (&other == this)
        return *this;

    column_name = other.column_name;
    expression_result_column_name = other.expression_result_column_name;
    if (other.expression)
        expression = other.expression->clone();
    else
        expression.reset();
    return *this;
}

namespace
{

/// The product of alternative counts probed for one function node is bounded to keep CREATE TABLE cheap.
/// A TTL whose validation needs more joint probes than this is rejected as suspicious (fail closed) rather
/// than partially checked; `allow_suspicious_ttl_expressions` remains the escape hatch.
constexpr size_t max_probe_combinations = 256;

[[noreturn]] void throwTooManyProbeCombinations(std::string_view expression_kind)
{
    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
        "TTL {}expression uses a function over arguments with too many combinations "
        "of AggregateFunction payloads to validate ({} probes at most are allowed). "
        "Use typed subcolumns instead, or set `allow_suspicious_ttl_expressions` to allow it",
        expression_kind, max_probe_combinations);
}

/// Build the list of single-row "suspect" materializations of `type` for the DDL-time TTL probe. Each
/// returned column is one combination of payloads stored inside the suspect types found in `type` - a
/// direct `AggregateFunction` state, or a `Variant`/`Dynamic` carrier that may hold one; the consumer must
/// survive every one of them. An empty list means the type contains nothing in scope of the check, so the
/// default value is representative and no extra probes are needed.
///
/// The suspect payload does not have to be the argument's top-level type: a consumer over
/// `Array(AggregateFunction(...))`, `Array(Dynamic)` or `Tuple(UInt32, Variant(...))` builds fine (the
/// container's default value is empty/NULL, so the nested consumer never runs) yet still fails on the
/// nested payloads during TTL execution. So the materialization recurses through
/// `Array`/`Tuple`/`Map`/`Nullable` (and `Variant` alternatives), wrapping each nested payload back into a
/// single-row container column.
std::vector<ColumnPtr> collectSuspectMaterializations(const DataTypePtr & type, std::string_view expression_kind)
{
    WhichDataType which(type);

    if (which.isAggregateFunction())
    {
        /// A direct AggregateFunction payload. For a top-level argument the default-value probe already
        /// covers it, but when the state is nested inside a container whose default value is empty
        /// (`Array`, `Map`) the element-level consumer never sees it: e.g. the `equals` built inside
        /// `arrayRemove(arr, 0)` for `arr Array(AggregateFunction(max, UInt64))` only runs on the
        /// elements of a non-empty row. Materialize one default state so the container branches below
        /// wrap it into a single-row non-empty column.
        return {type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst()};
    }

    if (which.isDynamic())
    {
        /// A `Dynamic` can store any type, so probing a single representative payload is not enough:
        /// a consumer that happens to accept an AggregateFunction state can still throw on other
        /// legal payloads (e.g. `finalizeAggregation(dyn)` accepts the state but rejects `UInt64` /
        /// `String`). Probe a small representative set instead - the AggregateFunction state that
        /// brings the column into scope, plus a numeric and a string payload - so only a genuinely
        /// type-agnostic consumer survives every probe. The state goes first so that a consumer failing
        /// on it gets the aggregate-specific error message.
        static const std::vector<String> representative_type_names =
            {"AggregateFunction(max, UInt64)", "UInt64", "String"};

        std::vector<ColumnPtr> payloads;
        payloads.reserve(representative_type_names.size());
        for (const auto & type_name : representative_type_names)
        {
            auto payload_type = DataTypeFactory::instance().get(type_name);
            ColumnPtr payload = payload_type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();

            auto dynamic_column = type->createColumn();
            auto & dynamic = assert_cast<ColumnDynamic &>(*dynamic_column);
            if (dynamic.addNewVariant(payload_type))
            {
                auto discr = dynamic.getVariantInfo().variant_name_to_discriminator.at(payload_type->getName());
                dynamic.getVariantColumn().insertIntoVariantFrom(discr, *payload, 0);
            }
            else
            {
                /// The type cannot hold new variants (e.g. `Dynamic(max_types=0)`), so values are
                /// stored in the shared variant - probe through it as well.
                dynamic.insertValueIntoSharedVariant(*payload, payload_type, payload_type->getName(), 0);
            }
            payloads.push_back(std::move(dynamic_column));
        }
        return payloads;
    }

    if (which.isVariant())
    {
        const auto & variant_type = assert_cast<const DataTypeVariant &>(*type);
        const auto & variant_types = variant_type.getVariants();

        /// Only a `Variant` that can actually carry an AggregateFunction state (directly, or through a
        /// nested carrier inside an alternative) is in scope of this check. But once such a `Variant` is a
        /// consumer's argument, *every* alternative must be probed, not only the aggregate-carrying ones:
        /// a state-aware consumer can accept the AggregateFunction branch and still throw
        /// `ILLEGAL_TYPE_OF_ARGUMENT` on a sibling alternative that a later row happens to store. For
        /// example `finalizeAggregation(v)` with `v Variant(AggregateFunction(max, UInt32), UInt32)`
        /// succeeds on the state branch but throws on a row storing the `UInt32` alternative during TTL
        /// execution. Probing only the aggregate alternative would wrongly accept it.
        std::vector<std::vector<ColumnPtr>> alternative_payloads(variant_types.size());
        bool has_suspect_alternative = false;
        for (size_t discr = 0; discr < variant_types.size(); ++discr)
        {
            if (hasAggregateFunctionType(variant_types[discr]))
                has_suspect_alternative = true;
            alternative_payloads[discr] = collectSuspectMaterializations(variant_types[discr], expression_kind);
            if (!alternative_payloads[discr].empty())
                has_suspect_alternative = true;
        }

        if (!has_suspect_alternative)
            return {};

        std::vector<ColumnPtr> result;
        for (size_t discr = 0; discr < variant_types.size(); ++discr)
        {
            auto & payloads = alternative_payloads[discr];
            if (payloads.empty())
                payloads.push_back(variant_types[discr]->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst());

            for (const auto & payload : payloads)
            {
                auto variant_column = variant_type.createColumn();
                assert_cast<ColumnVariant &>(*variant_column).insertIntoVariantFrom(
                    static_cast<ColumnVariant::Discriminator>(discr), *payload, 0);
                result.push_back(std::move(variant_column));
            }
            if (result.size() > max_probe_combinations)
                throwTooManyProbeCombinations(expression_kind);
        }
        return result;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto nested = collectSuspectMaterializations(array_type->getNestedType(), expression_kind);
        std::vector<ColumnPtr> result;
        result.reserve(nested.size());
        for (const auto & payload : nested)
        {
            auto offsets = ColumnArray::ColumnOffsets::create();
            offsets->getData().push_back(1);
            result.push_back(ColumnArray::create(IColumn::mutate(payload), std::move(offsets)));
        }
        return result;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & element_types = tuple_type->getElements();
        std::vector<std::vector<ColumnPtr>> element_payloads(element_types.size());
        bool has_suspect_element = false;
        size_t total_combinations = 1;
        for (size_t i = 0; i < element_types.size(); ++i)
        {
            element_payloads[i] = collectSuspectMaterializations(element_types[i], expression_kind);
            if (element_payloads[i].empty())
                element_payloads[i].push_back(element_types[i]->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst());
            else
                has_suspect_element = true;

            total_combinations *= element_payloads[i].size();
            if (total_combinations > max_probe_combinations)
                throwTooManyProbeCombinations(expression_kind);
        }

        if (!has_suspect_element)
            return {};

        /// The cartesian product of the element materializations, by a mixed-radix counter.
        std::vector<ColumnPtr> result;
        result.reserve(total_combinations);
        std::vector<size_t> selection(element_types.size(), 0);
        while (true)
        {
            Columns elements(element_types.size());
            for (size_t i = 0; i < element_types.size(); ++i)
                elements[i] = element_payloads[i][selection[i]];
            result.push_back(ColumnTuple::create(std::move(elements)));

            size_t i = 0;
            while (i < selection.size() && ++selection[i] == element_payloads[i].size())
            {
                selection[i] = 0;
                ++i;
            }
            if (i == selection.size())
                break;
        }
        return result;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        /// A Map is stored as Array(Tuple(key, value)); reuse the Array/Tuple materializations and wrap
        /// them back into a Map column.
        auto nested = collectSuspectMaterializations(map_type->getNestedType(), expression_kind);
        std::vector<ColumnPtr> result;
        result.reserve(nested.size());
        for (const auto & payload : nested)
            result.push_back(ColumnMap::create(IColumn::mutate(payload)));
        return result;
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        /// A carrier can hide under a Nullable wrapper too (e.g. `Nullable(Tuple(UInt32, Dynamic))` with
        /// `enable_nullable_tuple_type`). The default Nullable row is NULL, so a consumer over it
        /// short-circuits and never sees the nested payload; wrap each of them into a non-NULL row instead.
        auto nested = collectSuspectMaterializations(nullable_type->getNestedType(), expression_kind);
        std::vector<ColumnPtr> result;
        result.reserve(nested.size());
        for (const auto & payload : nested)
            result.push_back(ColumnNullable::create(IColumn::mutate(payload), ColumnUInt8::create(1, UInt8(0))));
        return result;
    }

    return {};
}

/// True if casting `from_type` to `to_type` can pick the stored `Variant`/`Dynamic` alternative by parsing
/// the *row contents* instead of deriving it from the source type. `FunctionCast::createColumnToVariantWrapper`
/// routes a string source to `createStringToVariantWrapper` under `cast_string_to_variant_use_inference`
/// (enabled by default), and `createColumnToDynamicWrapper` does the same under
/// `cast_string_to_dynamic_use_inference`. Container casts recurse into their elements, so this check
/// mirrors that recursion; a `Variant` source, in contrast, is never re-parsed (it goes to
/// `createVariantToDynamicWrapper`, which preserves the alternative each row already stores).
bool castMayInferPayloadFromString(const DataTypePtr & from_type, const DataTypePtr & to_type)
{
    /// The wrappers look through `Nullable`/`LowCardinality` on both sides.
    auto from = removeNullable(removeLowCardinality(from_type));
    auto to = removeNullable(removeLowCardinality(to_type));

    const WhichDataType which_to(*to);
    if ((which_to.isVariant() || which_to.isDynamic()) && WhichDataType(*from).isStringOrFixedString())
        return true;

    if (const auto * from_array = typeid_cast<const DataTypeArray *>(from.get()))
    {
        const auto * to_array = typeid_cast<const DataTypeArray *>(to.get());
        return to_array && castMayInferPayloadFromString(from_array->getNestedType(), to_array->getNestedType());
    }

    if (const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from.get()))
    {
        const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to.get());
        if (!to_tuple || from_tuple->getElements().size() != to_tuple->getElements().size())
            return false;
        for (size_t i = 0; i < from_tuple->getElements().size(); ++i)
            if (castMayInferPayloadFromString(from_tuple->getElements()[i], to_tuple->getElements()[i]))
                return true;
        return false;
    }

    if (const auto * from_map = typeid_cast<const DataTypeMap *>(from.get()))
    {
        const auto * to_map = typeid_cast<const DataTypeMap *>(to.get());
        return to_map
            && (castMayInferPayloadFromString(from_map->getKeyType(), to_map->getKeyType())
                || castMayInferPayloadFromString(from_map->getValueType(), to_map->getValueType()));
    }

    return false;
}

/// Build the single-row representative values of a non-suspect type for executing a typed `CAST` during
/// the DDL-time probe. The plain default value is degenerate for wrappers that would hide the payload
/// structure from the consumers of the cast result: a `Nullable` default row is NULL (the
/// `Variant`/`Dynamic` adaptors short-circuit on it) and an `Array`/`Map` default is empty (element-level
/// consumers never run), so recurse through them, materializing a non-NULL row and one-element containers
/// instead.
///
/// A `Variant` source needs *several* representatives, not one: a cast of a `Variant` to a carrier
/// preserves whichever alternative each row currently stores (`createVariantToDynamicWrapper`), so the
/// payload of the result is not fixed by a single representative row. The default `Variant` row is NULL,
/// and narrowing the consumer's domain to it would accept e.g. `length(CAST(v, 'Dynamic'))` for
/// `v Variant(String, UInt32)`, which throws `ILLEGAL_TYPE_OF_ARGUMENT` during TTL execution as soon as a
/// row stores the `UInt32` alternative. So every alternative is materialized and the cast is probed with
/// each of them; the union of the outputs is the domain the consumers are validated against.
std::vector<ColumnPtr> makeRepresentativeColumns(const DataTypePtr & type, std::string_view expression_kind)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto nested = makeRepresentativeColumns(nullable_type->getNestedType(), expression_kind);
        std::vector<ColumnPtr> result;
        result.reserve(nested.size());
        for (const auto & payload : nested)
            result.push_back(ColumnNullable::create(IColumn::mutate(payload), ColumnUInt8::create(1, UInt8(0))));
        return result;
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        const auto & variant_types = variant_type->getVariants();
        std::vector<ColumnPtr> result;
        for (size_t discr = 0; discr < variant_types.size(); ++discr)
        {
            for (const auto & payload : makeRepresentativeColumns(variant_types[discr], expression_kind))
            {
                auto variant_column = variant_type->createColumn();
                assert_cast<ColumnVariant &>(*variant_column).insertIntoVariantFrom(
                    static_cast<ColumnVariant::Discriminator>(discr), *payload, 0);
                result.push_back(std::move(variant_column));
            }
            if (result.size() > max_probe_combinations)
                throwTooManyProbeCombinations(expression_kind);
        }
        return result;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto nested = makeRepresentativeColumns(array_type->getNestedType(), expression_kind);
        std::vector<ColumnPtr> result;
        result.reserve(nested.size());
        for (const auto & payload : nested)
        {
            auto offsets = ColumnArray::ColumnOffsets::create();
            offsets->getData().push_back(1);
            result.push_back(ColumnArray::create(IColumn::mutate(payload), std::move(offsets)));
        }
        return result;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()); tuple_type && !tuple_type->getElements().empty())
    {
        const auto & element_types = tuple_type->getElements();
        std::vector<std::vector<ColumnPtr>> element_payloads(element_types.size());
        size_t total_combinations = 1;
        for (size_t i = 0; i < element_types.size(); ++i)
        {
            element_payloads[i] = makeRepresentativeColumns(element_types[i], expression_kind);
            total_combinations *= element_payloads[i].size();
            if (total_combinations > max_probe_combinations)
                throwTooManyProbeCombinations(expression_kind);
        }

        /// The cartesian product of the element representatives, by a mixed-radix counter.
        std::vector<ColumnPtr> result;
        result.reserve(total_combinations);
        std::vector<size_t> selection(element_types.size(), 0);
        while (true)
        {
            Columns elements(element_types.size());
            for (size_t i = 0; i < element_types.size(); ++i)
                elements[i] = element_payloads[i][selection[i]];
            result.push_back(ColumnTuple::create(std::move(elements)));

            size_t i = 0;
            while (i < selection.size() && ++selection[i] == element_payloads[i].size())
            {
                selection[i] = 0;
                ++i;
            }
            if (i == selection.size())
                break;
        }
        return result;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        auto nested = makeRepresentativeColumns(map_type->getNestedType(), expression_kind);
        std::vector<ColumnPtr> result;
        result.reserve(nested.size());
        for (const auto & payload : nested)
            result.push_back(ColumnMap::create(IColumn::mutate(payload)));
        return result;
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        auto nested = makeRepresentativeColumns(low_cardinality_type->getDictionaryType(), expression_kind);
        std::vector<ColumnPtr> result;
        result.reserve(nested.size());
        for (const auto & payload : nested)
        {
            auto column = type->createColumn();
            column->insert((*payload)[0]);
            result.push_back(std::move(column));
        }
        return result;
    }

    return {type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst()};
}

/// A fingerprint of a single-row candidate materialization, used to deduplicate candidates when merging
/// the domains of several selector branches. `ColumnDynamic`/`ColumnVariant` hash the *type* of the
/// payload stored in the row together with its value, so equal fingerprints mean the two candidates
/// carry the same payload (and in particular the same payload type).
UInt64 candidateFingerprint(const ColumnPtr & column)
{
    SipHash hash;
    hash.update(column->getDataType());
    column->updateHashWithValue(0, hash);
    return hash.get64();
}

/// The positions of the *value* arguments of a selector function - one whose result is always one of its
/// arguments, chosen by the others. `{}` for anything else.
std::vector<size_t> getSelectorValueArguments(const String & function_name, size_t arguments_count)
{
    std::vector<size_t> value_arguments;

    if (function_name == "if" && arguments_count == 3)
    {
        /// if(cond, then, else)
        value_arguments = {1, 2};
    }
    else if (function_name == "multiIf" && arguments_count >= 3)
    {
        /// multiIf(cond_1, then_1, ..., cond_n, then_n[, else])
        for (size_t i = 1; i < arguments_count; i += 2)
            value_arguments.push_back(i);
        if (arguments_count % 2 == 1)
            value_arguments.push_back(arguments_count - 1);
    }
    else if ((function_name == "coalesce" || function_name == "ifNull") && arguments_count >= 1)
    {
        for (size_t i = 0; i < arguments_count; ++i)
            value_arguments.push_back(i);
    }

    return value_arguments;
}

/// Reject TTL expressions that feed an AggregateFunction state into a function which cannot consume it
/// (e.g. `toDateTime(state)`), while still accepting state-aware functions like `finalizeAggregation`.
///
/// We only execute the individual functions that directly receive an argument whose type contains an
/// AggregateFunction state (including states nested inside Tuple/Array/Map/etc.). Executing the whole
/// expression instead would make DDL validity depend on synthetic default values: a data-dependent
/// error from an unrelated downstream function - e.g. division by zero in `intDiv(100, finalizeAggregation(state))`
/// when the default state finalizes to 0 - would turn a perfectly valid TTL into a CREATE TABLE failure.
/// Walking nodes individually also makes the check independent of short-circuit evaluation, so an
/// unsupported consumer hidden in a not-taken `if`/`multiIf` branch is still validated.
///
/// Higher-order functions (e.g. `arrayMap`) keep their lambda body in a separate inner DAG owned by a
/// `FunctionCapture`. Executing the outer node on a synthetic empty array would reduce the lambda over
/// zero rows and never reach the body, so we recurse into the lambda DAG instead. Only the type error
/// is translated into a clear message; all other exceptions are rethrown.
///
/// A synthetic default value catches a top-level AggregateFunction argument, but not one that is only an
/// alternative of a `Variant` column: the default `Variant` row is NULL, so the `Variant` function
/// adaptor short-circuits (returns NULL) and never runs the consumer on the AggregateFunction
/// alternative. To exercise it we additionally probe with a single-row `Variant` column whose only value
/// is that alternative (e.g. `toDateTime(v)` with `v Variant(AggregateFunction(max, DateTime64(3)), String)`).
///
/// `Dynamic` erases its value types entirely: the static type never mentions AggregateFunction, yet any
/// row may carry a state (e.g. inserted via CAST to `Dynamic`), and a consumer like `toDateTime` would
/// only fail later, during TTL execution. Since the stored types cannot be enumerated at DDL time, we
/// probe every `Dynamic` argument with a *set* of representative single-row payloads - the
/// AggregateFunction state that brings the column into scope (`AggregateFunction(max, UInt64)`) plus a
/// numeric (`UInt64`) and a string (`String`) payload. Only a genuinely type-agnostic consumer
/// (`isNotNull`, `dynamicType`, `toString`, ...) survives every probe; a state-aware consumer such as
/// `finalizeAggregation(dyn)`, which accepts the state but throws `ILLEGAL_TYPE_OF_ARGUMENT` on other
/// legal payloads, is rejected here rather than at execution time. Such a TTL is one inserted row
/// away from breaking every merge of the table, so rejecting it at CREATE is the safer default; the
/// `allow_suspicious_ttl_expressions` setting and ATTACH remain available as escape hatches.
///
/// A suspect payload can also sit *inside* a container argument - a direct state in
/// `Array(AggregateFunction(...))` or `Map(String, AggregateFunction(...))`, or a carrier in
/// `Array(Dynamic)`, `Tuple(Dynamic)`, `Map(String, Dynamic)`, `Nullable(Tuple(..., Dynamic))`, or a
/// `Variant` nested in any of them. The container's default value is empty (or NULL), so a consumer that
/// processes the elements (e.g. the `equals` built inside `arrayRemove`) never sees a payload during a
/// default-value probe, yet still fails on the stored payloads during TTL execution. The suspect
/// materializations therefore recurse through the container types and wrap each nested payload back into a
/// single-row container column.
///
/// Enumerating payloads from a static type is only correct for *stored* columns, which can hold any value
/// of their type. A carrier *computed* inside the expression can have a much narrower runtime domain:
/// `CAST(state, 'Dynamic')` or `CAST(state, 'Variant(AggregateFunction(max, UInt32), UInt32)')` only ever
/// produces the aggregate-state payload, so probing its consumer with fabricated sibling payloads
/// (`UInt64`/`String`, or the `UInt32` alternative) would reject a valid TTL such as
/// `DELETE WHERE isNotNull(finalizeAggregation(CAST(state, 'Dynamic')))`. So each probe records the
/// function's *actual* output columns, and a parent consuming a computed carrier is validated against
/// those instead of the static enumeration. This propagation applies only when the probes cover the node's
/// whole runtime domain, which requires two conditions:
/// - at least one argument carries suspect payloads itself: a carrier computed purely from non-suspect
///   inputs (e.g. `JSONExtract(s, 'Dynamic')`) can produce payloads that depend on the data rather than
///   on the input types, which a single synthetic execution cannot reveal;
/// - every non-suspect argument is a constant, so its probe value is exactly its execution-time value.
///   A non-constant non-suspect argument can *select* which payload the result carries - e.g. in
///   `if(cond, CAST(state, 'Dynamic'), CAST(0, 'Dynamic'))` the probes run with the default `cond = 0`
///   and only ever record the second branch, hiding the aggregate-state payload of the first one.
/// When either condition fails, the node keeps the fail-closed static enumeration of its result type.
///
/// A *selector* function - `if`, `multiIf`, `coalesce`, `ifNull` - is the exception to the second condition:
/// its result is always one of its value arguments, so a non-constant control argument can only choose
/// *which* of their domains the result comes from, never introduce a payload none of them can hold. The union
/// of the value arguments' domains is therefore propagated after all, and valid TTLs such as
/// `toDateTime(if(cond, CAST(n, 'Dynamic'), CAST(m, 'Dynamic')))` over `n`, `m UInt32` and
/// `toDateTime(if(cond, CAST(1, 'Dynamic'), CAST(2, 'Dynamic')))` are accepted. A selector converts every
/// value argument to its result type, so a branch whose own type differs from it - including a branch that is
/// no carrier at all, like `m` in `if(cond, CAST(n, 'Dynamic'), m)` with `m UInt32` - contributes the payloads
/// that conversion produces from its values, which is what the branch domains are converted to below.
///
/// Higher-order functions cannot be executed here at all, so their result normally falls back to the static
/// enumeration too. `arrayMap` is the exception: its result is exactly the array of the values its lambda
/// body produces, and that body is walked as an inner DAG by the recursive call below - with every rule of
/// this check applied to it - so its candidate domain is wrapped into one-element arrays and propagated.
/// This accepts e.g. `toDateTime(arrayElement(arrayMap(x -> CAST(x, 'Dynamic'), arr), 1))` over
/// `arr Array(UInt32)`, whose elements can only ever hold the `UInt32` payload.
///
/// A typed `CAST` is the exception to the first condition: its output payloads are fixed by the *source
/// type* alone (the cast wrapper fills the discriminators derived from it), independent of the values. So
/// `CAST(n, 'Dynamic')` with `n UInt32` can only ever store the `UInt32` payload, and probing its consumer
/// with the static enumeration would reject a valid TTL such as
/// `DELETE WHERE toDateTime(CAST(n, 'Dynamic')) < now()` over synthetic `AggregateFunction` payloads
/// the cast can never produce. An untainted `CAST` is instead executed on the representative values of its
/// source type (non-NULL, one-element containers - the plain default would hide nested payload structure -
/// and one value per `Variant` alternative, which the cast preserves row by row) and the union of its
/// actual outputs is propagated to the consumers.
///
/// A cast of a *string* to a carrier is in turn the exception to that exception - it parses the stored
/// alternative out of the row contents, so it is value-dependent after all and stays fail-closed; see the
/// `source_payload_may_be_inferred` check below.
///
/// `result_name`, when set, names an output of `actions_dag` whose candidate materializations are returned to
/// the caller. It is used for the inner DAG of a lambda: the domain of the lambda body is what a higher-order
/// function like `arrayMap` produces, so returning it lets the outer node propagate it too.
std::vector<ColumnPtr> checkActionsDAGForAggregateFunctions(
    const ActionsDAG & actions_dag, std::string_view expression_kind, const String * result_name = nullptr)
{
    /// Per-node "candidate" materializations: the single-row columns whose payloads the node can produce
    /// at TTL execution time and that are in scope of this check. An empty list means the node's default
    /// (or constant) value is representative and its consumers need no extra probes.
    std::unordered_map<const ActionsDAG::Node *, std::vector<ColumnPtr>> candidates_map;

    /// The candidate materializations of the *body* of each lambda argument, keyed by its capture node.
    std::unordered_map<const ActionsDAG::Node *, std::vector<ColumnPtr>> lambda_body_candidates;

    std::function<const std::vector<ColumnPtr> & (const ActionsDAG::Node *)> candidates_of
        = [&](const ActionsDAG::Node * node) -> const std::vector<ColumnPtr> &
    {
        if (auto it = candidates_map.find(node); it != candidates_map.end())
            return it->second;

        std::vector<ColumnPtr> candidates;

        if (node->column)
        {
            /// The node's value is a known constant, so it is exact: probe consumers with the actual
            /// value instead of over-approximating it from the static type. Keep it a single-row clone
            /// (possibly const) so functions requiring constant arguments still see one.
            if (hasAggregateFunctionType(node->result_type) || hasDynamicType(node->result_type))
                candidates.push_back(node->column->cloneResized(1));
        }
        else if (node->type == ActionsDAG::ActionType::ALIAS)
        {
            candidates = candidates_of(node->children.front());
        }
        else if (node->type == ActionsDAG::ActionType::FUNCTION)
        {
            /// Descend into lambda bodies of higher-order functions to validate consumers hidden inside
            /// them. The capture node itself produces a function value, nothing to materialize.
            if (const auto * function_capture = dynamic_cast<const FunctionCapture *>(node->function_base.get()))
            {
                const auto & lambda_result_name = function_capture->getCapture().return_name;
                lambda_body_candidates[node] = checkActionsDAGForAggregateFunctions(
                    function_capture->getAcionsDAG(), expression_kind, &lambda_result_name);
                return candidates_map.emplace(node, std::move(candidates)).first->second;
            }

            const ActionsDAG::Node * lambda_argument = nullptr;
            bool has_lambda_argument = false;
            ColumnsWithTypeAndName arguments;
            std::vector<size_t> suspect_indexes;
            std::vector<const std::vector<ColumnPtr> *> suspect_columns;
            bool has_dynamic_suspect = false;
            bool non_suspect_args_are_constant = true;
            arguments.reserve(node->children.size());
            for (size_t i = 0; i < node->children.size(); ++i)
            {
                const auto * child = node->children[i];

                /// A lambda argument cannot be materialized into a column; the higher-order function
                /// that receives it is validated through the captured lambda DAG above, so skip
                /// executing it here.
                if (WhichDataType(child->result_type).isFunction())
                {
                    has_lambda_argument = true;
                    lambda_argument = child->type == ActionsDAG::ActionType::ALIAS ? child->children.front() : child;
                    /// Make sure the lambda body has been walked (and its candidates recorded) before the
                    /// higher-order node below looks them up - the outer loop over the DAG nodes visits the
                    /// nodes in no particular order.
                    candidates_of(lambda_argument);
                    break;
                }

                /// Preserve constant arguments as constants - some functions (e.g. `CAST`) require a
                /// constant argument and otherwise throw an unrelated error during this synthetic execution.
                ColumnPtr column = child->column
                    ? child->column->cloneResized(1)
                    : child->result_type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();
                arguments.emplace_back(std::move(column), child->result_type, child->result_name);

                const auto & child_candidates = candidates_of(child);
                if (child_candidates.empty())
                {
                    if (!child->column)
                        non_suspect_args_are_constant = false;
                    continue;
                }
                suspect_indexes.push_back(i);
                suspect_columns.push_back(&child_candidates);
                if (hasDynamicType(child->result_type))
                    has_dynamic_suspect = true;
            }

            const bool result_in_scope = hasAggregateFunctionType(node->result_type) || hasDynamicType(node->result_type);

            if (has_lambda_argument)
            {
                /// The node was not executed (its output cannot be derived synthetically here), so if its
                /// result can carry a suspect payload, fail closed with the static enumeration - except for
                /// `arrayMap`, whose result is exactly an array of the lambda body's values, so the body's
                /// candidate domain (computed in the inner DAG above, with every narrowing rule of this
                /// check applied to it) describes the elements: wrap each of them into a one-element array.
                if (result_in_scope)
                {
                    const auto * result_array_type = typeid_cast<const DataTypeArray *>(node->result_type.get());
                    const auto * lambda_capture = lambda_argument && lambda_argument->type == ActionsDAG::ActionType::FUNCTION
                        ? dynamic_cast<const FunctionCapture *>(lambda_argument->function_base.get())
                        : nullptr;
                    const auto * lambda_candidates = lambda_capture && lambda_body_candidates.contains(lambda_argument)
                        ? &lambda_body_candidates.at(lambda_argument)
                        : nullptr;

                    if (node->function_base->getName() == "arrayMap" && result_array_type && lambda_candidates
                        && !lambda_candidates->empty()
                        && result_array_type->getNestedType()->equals(*lambda_capture->getCapture().return_type))
                    {
                        for (const auto & element : *lambda_candidates)
                        {
                            auto offsets = ColumnArray::ColumnOffsets::create();
                            offsets->getData().push_back(1);
                            candidates.push_back(ColumnArray::create(element->cloneResized(1), std::move(offsets)));
                        }
                    }
                    else
                        candidates = collectSuspectMaterializations(node->result_type, expression_kind);
                }
            }
            else if (suspect_indexes.empty())
            {
                /// No argument carries a suspect payload, so there is nothing to probe this node with.
                /// If its *result* is a carrier computed from non-suspect inputs, its runtime payloads
                /// may be data-dependent (see above), so consumers get the fail-closed static enumeration.
                /// The exception is a typed CAST, whose output payload type is fixed by the source type
                /// alone: execute it once on a representative source value and propagate the actual
                /// output domain instead (see above).
                if (result_in_scope)
                {
                    const auto & function_name = node->function_base->getName();

                    /// A cast of a *string* to a carrier is not source-type-determined: with
                    /// `cast_string_to_variant_use_inference` (on by default) and
                    /// `cast_string_to_dynamic_use_inference`, `createColumnToVariantWrapper` /
                    /// `createColumnToDynamicWrapper` parse the alternative out of the row contents, so
                    /// `CAST(s, 'Variant(String, UInt32, AggregateFunction(max, UInt32))')` stores the
                    /// `String` alternative for the representative `''` but the `UInt32` one for a row
                    /// `s = '42'`. A single representative value therefore says nothing about the runtime
                    /// domain. The settings are also per-query while the stored TTL expression is rebuilt
                    /// and executed under other contexts, so, like the strict probe above, the DDL-time
                    /// verdict must hold for either of them: keep such casts on the fail-closed path.
                    bool source_payload_may_be_inferred = false;
                    for (const auto & child : node->children)
                        if (!child->column && castMayInferPayloadFromString(child->result_type, node->result_type))
                            source_payload_may_be_inferred = true;

                    if ((function_name == "CAST" || function_name == "_CAST") && !source_payload_may_be_inferred)
                    {
                        /// A source type can need more than one representative value (a `Variant` needs
                        /// one per alternative), so run the cast over the cartesian product of them and
                        /// propagate the union of the outputs.
                        std::vector<size_t> representative_indexes;
                        std::vector<std::vector<ColumnPtr>> representative_columns;
                        size_t total_combinations = 1;
                        for (size_t i = 0; i < node->children.size(); ++i)
                        {
                            if (node->children[i]->column)
                                continue;
                            representative_indexes.push_back(i);
                            representative_columns.push_back(
                                makeRepresentativeColumns(node->children[i]->result_type, expression_kind));
                            total_combinations *= representative_columns.back().size();
                        }

                        /// Too many source combinations to enumerate (or none at all): the node itself is
                        /// valid, only the narrowing is given up, so fall back to the static enumeration
                        /// instead of failing the whole expression.
                        if (total_combinations == 0 || total_combinations > max_probe_combinations)
                        {
                            candidates = collectSuspectMaterializations(node->result_type, expression_kind);
                        }
                        else
                        {
                            std::vector<size_t> selection(representative_indexes.size(), 0);
                            while (true)
                            {
                                ColumnsWithTypeAndName representative_arguments = arguments;
                                for (size_t r = 0; r < representative_indexes.size(); ++r)
                                    representative_arguments[representative_indexes[r]].column
                                        = representative_columns[r][selection[r]];

                                try
                                {
                                    ColumnPtr cast_result = node->function_base->execute(
                                        representative_arguments, node->result_type, /*input_rows_count=*/ 1, /*dry_run=*/ true);
                                    candidates.push_back(cast_result->convertToFullColumnIfConst());
                                }
                                catch (...) /// Ok: any failure here only means we cannot narrow the domain.
                                {
                                    /// The cast failed on a synthetic representative value (a data-dependent
                                    /// error, e.g. an unparseable default string). The node itself needs no
                                    /// validation - fail closed to the static enumeration for its consumers.
                                    candidates = collectSuspectMaterializations(node->result_type, expression_kind);
                                    break;
                                }

                                size_t r = 0;
                                while (r < selection.size() && ++selection[r] == representative_columns[r].size())
                                {
                                    selection[r] = 0;
                                    ++r;
                                }
                                if (r == selection.size())
                                    break;
                            }
                        }
                    }
                    else
                        candidates = collectSuspectMaterializations(node->result_type, expression_kind);
                }
            }
            else
            {
                /// Translate the "cannot consume an AggregateFunction state" type error into a clear TTL
                /// message; rethrow anything else (e.g. a data-dependent error raised by a perfectly
                /// valid consumer).
                auto probe = [&](const ColumnsWithTypeAndName & probe_arguments, std::string_view hint) -> ColumnPtr
                {
                    try
                    {
                        return node->function_base->execute(probe_arguments, node->result_type, /*input_rows_count=*/ 1, /*dry_run=*/ true);
                    }
                    catch (Exception & e)
                    {
                        if (e.code() == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT)
                            throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                                "TTL {}expression uses {}: {}", expression_kind, hint, e.message());
                        throw;
                    }
                };

                constexpr std::string_view aggregate_state_hint =
                    "AggregateFunction column in a function that cannot handle it. "
                    "Use `finalizeAggregation` to extract the value first";

                constexpr std::string_view dynamic_hint =
                    "a Dynamic column in a function that cannot handle all types a Dynamic column can store "
                    "(e.g. an AggregateFunction state), so TTL execution could fail depending on the inserted values. "
                    "Use a typed subcolumn instead, or set `allow_suspicious_ttl_expressions` to allow it";

                /// All suspect arguments must be materialized in the same probe: substituting them one at
                /// a time would leave the other carriers at their all-NULL defaults, letting the adaptor
                /// short-circuit to NULL and hide a consumer that only fails when several carriers hold
                /// states simultaneously (e.g. `d1 + d2` or `v1 = v2`). So the probes below run the
                /// cartesian product of the candidate materializations across all suspect arguments.
                size_t total_combinations = 1;
                for (const auto * columns : suspect_columns)
                {
                    total_combinations *= columns->size();
                    if (total_combinations > max_probe_combinations)
                        throwTooManyProbeCombinations(expression_kind);
                }

                std::vector<size_t> selection(suspect_indexes.size(), 0);
                while (true)
                {
                    ColumnsWithTypeAndName probe_arguments = arguments;
                    for (size_t s = 0; s < suspect_indexes.size(); ++s)
                        probe_arguments[suspect_indexes[s]].column = (*suspect_columns[s])[selection[s]];
                    ColumnPtr probe_result = probe(probe_arguments, has_dynamic_suspect ? dynamic_hint : aggregate_state_hint);

                    /// When every non-suspect argument is a constant, the probe outputs are exactly the
                    /// payloads this node can produce from its suspect inputs - propagate them so a parent
                    /// consuming this computed carrier is validated against the real domain, not the
                    /// static enumeration of its result type. A non-constant non-suspect argument breaks
                    /// this: it is probed with a synthetic default value, but at execution time it can
                    /// select a payload the probes never produced (e.g. the condition of `if`), so such
                    /// nodes fall back to the static enumeration below instead.
                    if (result_in_scope && non_suspect_args_are_constant)
                        candidates.push_back(probe_result->convertToFullColumnIfConst());

                    /// Advance the mixed-radix counter over the candidates of each suspect argument.
                    size_t s = 0;
                    while (s < selection.size() && ++selection[s] == suspect_columns[s]->size())
                    {
                        selection[s] = 0;
                        ++s;
                    }
                    if (s == selection.size())
                        break;
                }

                /// The probes above validated this node, but their outputs under-approximate its runtime
                /// domain when a non-constant non-suspect argument can select the payload - fail closed
                /// with the static enumeration for the parents in that case.
                ///
                /// A *selector* function is the exception: its result is always one of its value arguments,
                /// converted to the result type, so whatever its non-constant control arguments choose at
                /// execution time, the result stays inside the union of the converted value domains. That
                /// union - deduplicated by fingerprint - is propagated instead of the static enumeration:
                /// `if(cond, CAST(n, 'Dynamic'), CAST(m, 'Dynamic'))` with `n`, `m UInt32` can only ever
                /// hold the `UInt32` payload, whichever branch `cond` takes,
                /// `if(cond, CAST(1, 'Dynamic'), CAST(2, 'Dynamic'))` only the `UInt8` one, and
                /// `if(cond, CAST(n, 'Dynamic'), m)` with `m UInt32` only numeric ones. A branch whose
                /// domain does contain a state (e.g. `CAST(state, 'Dynamic')`) keeps its state candidate in
                /// the union, so an unsupported parent consumer is still rejected by its probes.
                if (result_in_scope && !non_suspect_args_are_constant)
                {
                    bool selector_domain_is_proven = false;
                    const auto value_arguments = getSelectorValueArguments(node->function_base->getName(), node->children.size());
                    if (!value_arguments.empty())
                    {
                        /// The domain of one value branch, expressed in the result type of the selector.
                        /// The branch's own domain is its candidate list, or the representative values of its
                        /// static type when it carries no suspect payload itself; each value is then converted
                        /// to the result type exactly like the selector does at execution time. `{}` means the
                        /// domain could not be proven - e.g. a conversion that infers the payload out of a
                        /// string is value-dependent, so it says nothing about the runtime payloads.
                        auto branch_domain = [&](const ActionsDAG::Node * value_argument) -> std::vector<ColumnPtr>
                        {
                            const auto & branch_candidates = candidates_of(value_argument);
                            if (value_argument->result_type->equals(*node->result_type))
                                return branch_candidates;

                            if (castMayInferPayloadFromString(value_argument->result_type, node->result_type))
                                return {};

                            std::vector<ColumnPtr> converted;
                            try
                            {
                                const auto & branch_values = branch_candidates.empty()
                                    ? makeRepresentativeColumns(value_argument->result_type, expression_kind)
                                    : branch_candidates;
                                for (const auto & value : branch_values)
                                    converted.push_back(
                                        castColumn({value, value_argument->result_type, value_argument->result_name},
                                                   node->result_type)->convertToFullColumnIfConst());
                            }
                            catch (...) /// Ok: any failure here only means we cannot narrow the domain.
                            {
                                return {};
                            }
                            return converted;
                        };

                        selector_domain_is_proven = true;
                        std::vector<ColumnPtr> union_candidates;
                        std::unordered_set<UInt64> union_fingerprints;
                        for (size_t index : value_arguments)
                        {
                            const auto value_candidates = branch_domain(node->children[index]);
                            if (value_candidates.empty())
                            {
                                selector_domain_is_proven = false;
                                break;
                            }
                            for (const auto & candidate : value_candidates)
                                if (union_fingerprints.insert(candidateFingerprint(candidate)).second)
                                    union_candidates.push_back(candidate);
                        }

                        if (selector_domain_is_proven)
                            candidates.insert(candidates.end(), union_candidates.begin(), union_candidates.end());
                    }

                    if (!selector_domain_is_proven)
                        candidates = collectSuspectMaterializations(node->result_type, expression_kind);
                }
            }
        }
        else
        {
            /// An INPUT column (or any other node kind) can hold any value of its type - enumerate the
            /// suspect payloads from the static type.
            candidates = collectSuspectMaterializations(node->result_type, expression_kind);
        }

        return candidates_map.emplace(node, std::move(candidates)).first->second;
    };

    for (const auto & node : actions_dag.getNodes())
        candidates_of(&node);

    if (!result_name)
        return {};

    /// The lambda body: its result is the single output of the inner DAG. If it cannot be found, give the
    /// caller nothing and let it fall back to the static enumeration.
    if (const auto * result_node = actions_dag.tryFindInOutputs(*result_name))
        return candidates_of(result_node);
    return {};
}

void checkTTLExpressionForAggregateFunctions(const ExpressionActionsPtr & expression, std::string_view expression_kind)
{
    /// The synthetic probe in `checkActionsDAGForAggregateFunctions` exercises consumers over `Variant`/`Dynamic`
    /// columns carrying an AggregateFunction state. For consumers wrapped in the `Variant`/`Dynamic` function
    /// adaptors, whether a type mismatch throws or is silently turned into NULL at *execution* is decided by
    /// `variant_throw_on_type_mismatch` / `dynamic_throw_on_type_mismatch`, which the adaptors read from the
    /// query context of the current thread. But a stored TTL expression is later rebuilt and executed under
    /// several unrelated contexts: the *inserting* session in `MergeTreeDataWriter::updateTTL` (strict by
    /// default) and the background context during TTL merges (settings from the `background_profile` server
    /// config, strict by default). The DDL-time verdict must therefore not depend on any one of them:
    /// the probe always runs strict, which is the superset - an expression that survives the strict probe
    /// only ever gets *more* lenient at execution (a mismatch turns into NULL instead of an exception), so it
    /// is safe under every context, while anything rejected here would throw on the first
    /// AggregateFunction-carrying row in at least the strict paths (e.g. a default-settings INSERT).
    /// A server that deliberately runs everything lenient still has `allow_suspicious_ttl_expressions`.
    /// (Conversion functions such as `toDateTime` handle `Variant`/`Dynamic` natively, ignore both settings
    /// and always throw on a stored type they cannot convert, so for them the probe's verdict is the same
    /// under any settings.)
    TypeMismatchStrictnessOverride probe_strictness(/*variant_throw_on_type_mismatch=*/ true, /*dynamic_throw_on_type_mismatch=*/ true);

    checkActionsDAGForAggregateFunctions(expression->getActionsDAG(), expression_kind);
}

void checkTTLExpression(const ExpressionActionsPtr & ttl_expression, const String & result_column_name, bool allow_suspicious)
{
    /// Do not apply this check in ATTACH queries for compatibility reasons and if explicitly allowed.
    if (!allow_suspicious)
    {
        if (ttl_expression->getRequiredColumns().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "TTL expression {} does not depend on any of the columns of the table", result_column_name);

        for (const auto & action : ttl_expression->getActions())
        {
            if (action.node->type == ActionsDAG::ActionType::FUNCTION)
            {
                const IFunctionBase & func = *action.node->function_base;
                if (!func.isDeterministic())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "TTL expression cannot contain non-deterministic functions, but contains function {}",
                                    func.getName());
            }
        }

        checkTTLExpressionForAggregateFunctions(ttl_expression, /*expression_kind=*/ "");
    }

    const auto & result_column = ttl_expression->getSampleBlock().getByName(result_column_name);
    if (!typeid_cast<const DataTypeDateTime *>(result_column.type.get())
        && !typeid_cast<const DataTypeDate *>(result_column.type.get())
        && !typeid_cast<const DataTypeDateTime64 *>(result_column.type.get())
        && !typeid_cast<const DataTypeDate32 *>(result_column.type.get()))
    {
        throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                        "TTL expression result column should have Date, Date32, DateTime or DateTime64 type, but has {}",
                        result_column.type->getName());
    }
}

class FindAggregateFunctionData
{
public:
    using TypeToVisit = ASTFunction;
    bool has_aggregate_function = false;

    void visit(const ASTFunction & func, ASTPtr &)
    {
        /// Do not throw if found aggregate function inside another aggregate function,
        /// because it will be checked, while creating expressions.
        if (AggregateUtils::isAggregateFunction(func))
            has_aggregate_function = true;
    }
};

using FindAggregateFunctionFinderMatcher = OneTypeMatcher<FindAggregateFunctionData>;
using FindAggregateFunctionVisitor = InDepthNodeVisitor<FindAggregateFunctionFinderMatcher, true>;

/// Widens `Date` / `DateTime` to `Date32` / `DateTime64(0, tz)`, recursively inside
/// `Tuple`, `Array`, and `Map` carriers. A TTL expression can refer to a nested temporal
/// value while its syntax-level source column is the enclosing carrier, so widening only
/// top-level source types would leave that value in the 16/32-bit domain.
DataTypePtr widenTemporalType(const DataTypePtr & type)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto widened_nested = widenTemporalType(nullable_type->getNestedType());
        if (!nullable_type->getNestedType()->equals(*widened_nested))
            return std::make_shared<DataTypeNullable>(std::move(widened_nested));

        return type;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes widened_elements;
        widened_elements.reserve(tuple_type->getElements().size());
        bool widened_any = false;

        for (const auto & element : tuple_type->getElements())
        {
            auto widened_element = widenTemporalType(element);
            widened_any |= !element->equals(*widened_element);
            widened_elements.push_back(std::move(widened_element));
        }

        if (widened_any)
            return std::make_shared<DataTypeTuple>(std::move(widened_elements), tuple_type->getElementNames());

        return type;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto widened_nested = widenTemporalType(array_type->getNestedType());
        if (!array_type->getNestedType()->equals(*widened_nested))
            return std::make_shared<DataTypeArray>(std::move(widened_nested));

        return type;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        auto widened_key = widenTemporalType(map_type->getKeyType());
        auto widened_value = widenTemporalType(map_type->getValueType());
        if (!map_type->getKeyType()->equals(*widened_key) || !map_type->getValueType()->equals(*widened_value))
            return std::make_shared<DataTypeMap>(std::move(widened_key), std::move(widened_value));

        return type;
    }

    const auto inner = removeLowCardinalityAndNullable(type);
    DataTypePtr widened;
    if (isDate(inner))
    {
        widened = std::make_shared<DataTypeDate32>();
    }
    else if (isDateTime(inner))
    {
        const auto & dt = typeid_cast<const DataTypeDateTime &>(*inner);
        const String & tz = dt.getTimeZone().getTimeZone();
        widened = std::make_shared<DataTypeDateTime64>(0, tz);
    }
    else
    {
        return type;
    }

    if (isNullableOrLowCardinalityNullable(type))
        widened = std::make_shared<DataTypeNullable>(widened);

    return widened;
}

/// Returns the column list with every `Date` / `DateTime` source column widened to
/// `Date32` / `DateTime64(0, tz)` (looking through `Nullable` / `LowCardinality` and
/// through `Tuple`, `Array`, and `Map` carriers).
/// The TTL expression is analyzed against this widened view so arithmetic in
/// `column + INTERVAL ...` is performed in the 64-bit domain and cannot silently
/// wrap on overflow. The original timezone is preserved so calendar transforms
/// (`addMonths` / `addYears`) and DST boundaries produce the user-expected results.
///
/// `Nullable` is preserved: dropping it would let the analyzer treat the column as
/// non-null, which constant-folds `isNull` / `ifNull` and silently changes TTL
/// decisions for rows that are actually `NULL` (for both rows-TTL and `DELETE WHERE`).
/// `LowCardinality` is dropped because `LowCardinality(DateTime64)` is not allowed
/// in the type system; the runtime cast in `ITTLAlgorithm::executeExpressionAndGetColumn`
/// converts the original `LC` column to the widened type.
NamesAndTypesList widenTemporalColumns(const NamesAndTypesList & columns)
{
    NamesAndTypesList result;
    for (const auto & col : columns)
    {
        result.emplace_back(col.name, widenTemporalType(col.type));
    }
    return result;
}

}

TTLDescription::TTLDescription(const TTLDescription & other)
    : mode(other.mode)
    , expression_ast(other.expression_ast ? other.expression_ast->clone() : nullptr)
    , expression_columns(other.expression_columns)
    , expression_source_columns(other.expression_source_columns)
    , result_column(other.result_column)
    , where_expression_ast(other.where_expression_ast ? other.where_expression_ast->clone() : nullptr)
    , where_expression_columns(other.where_expression_columns)
    , where_expression_source_columns(other.where_expression_source_columns)
    , where_result_column(other.where_result_column)
    , group_by_keys(other.group_by_keys)
    , set_parts(other.set_parts)
    , aggregate_descriptions(other.aggregate_descriptions)
    , destination_type(other.destination_type)
    , destination_name(other.destination_name)
    , if_exists(other.if_exists)
    , recompression_codec(other.recompression_codec)
{
}

TTLDescription & TTLDescription::operator=(const TTLDescription & other)
{
    if (&other == this)
        return *this;

    mode = other.mode;
    if (other.expression_ast)
        expression_ast = other.expression_ast->clone();
    else
        expression_ast.reset();

    expression_columns = other.expression_columns;
    expression_source_columns = other.expression_source_columns;
    result_column = other.result_column;

    if (other.where_expression_ast)
        where_expression_ast = other.where_expression_ast->clone();
    else
        where_expression_ast.reset();

    where_expression_columns = other.where_expression_columns;
    where_expression_source_columns = other.where_expression_source_columns;
    where_result_column = other.where_result_column;
    group_by_keys = other.group_by_keys;
    set_parts = other.set_parts;
    aggregate_descriptions = other.aggregate_descriptions;
    destination_type = other.destination_type;
    destination_name = other.destination_name;
    if_exists = other.if_exists;

    if (other.recompression_codec)
        recompression_codec = other.recompression_codec->clone();
    else
        recompression_codec.reset();

    return * this;
}

/// `required_source_columns`, when given, receives the columns of `columns` that the AST refers to. Note
/// this is deliberately taken from the syntax analysis and not from the built expression: constant folding
/// can prune a column out of the expression (`WHERE isNull(x)` over a non-`Nullable` `x` folds to `0`),
/// while the stored AST still refers to it and every later rebuild of that AST needs it to be available.
/// The built expression's own required columns (the runtime read set the read planners consume) are taken
/// separately, from `getRequiredColumnsWithTypes` of the returned expression.
static ExpressionAndSets analyzeExpressionAndSets(
    const ASTPtr & ast_template,
    const NamesAndTypesList & columns,
    const ContextPtr & context,
    NamesAndTypesList * required_source_columns = nullptr)
{
    ExpressionAndSets result;
    /// `TreeRewriter::analyze` mutates the AST in place; clone so a failed attempt does
    /// not leave a half-rewritten AST behind for the fallback analysis to choke on.
    auto ast = ast_template->clone();
    auto ttl_string = ast->formatWithSecretsOneLine();
    auto syntax_analyzer_result = TreeRewriter(context).analyze(ast, columns);
    if (required_source_columns)
        *required_source_columns = syntax_analyzer_result->required_source_columns;
    ExpressionAnalyzer analyzer(ast, syntax_analyzer_result, context);
    auto dag = analyzer.getActionsDAG(false);

    const auto * col = &dag.findInOutputs(ast->getColumnName());
    if (col->result_name != ttl_string)
        col = &dag.addAlias(*col, ttl_string);

    dag.getOutputs() = {col};
    dag.removeUnusedActions();

    result.expression = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings(context));
    result.sets = analyzer.getPreparedSets();

    return result;
}

static ExpressionAndSets buildExpressionAndSets(
    ASTPtr & ast,
    const NamesAndTypesList & columns,
    const ContextPtr & context,
    NamesAndTypesList * required_source_columns = nullptr,
    bool widen_temporal_columns = true)
{
    /// Analyze the TTL expression against `Date` / `DateTime` source columns widened to
    /// `Date32` / `DateTime64(0, tz)`, so `column + INTERVAL ...` arithmetic runs in the
    /// 64-bit domain and cannot silently 16/32-bit wrap on overflow (issue #101763).
    ///
    /// Some valid TTL expressions use functions that accept only the narrow temporal
    /// types and reject the widened ones (e.g. `tumbleStart` / `tumbleEnd` require
    /// `DateTime`, not `DateTime64`). The widened analysis would reject those and break
    /// `ATTACH` of legacy tables after an upgrade, so we fall back to analyzing against
    /// the original column types. Such expressions explicitly operate in the narrow
    /// `Date` / `DateTime` domain and are out of scope for the overflow fix.
    if (!widen_temporal_columns)
        return analyzeExpressionAndSets(ast, columns, context, required_source_columns);

    auto widened_columns = widenTemporalColumns(columns);
    bool widened_any = !std::equal(
        columns.begin(), columns.end(), widened_columns.begin(), widened_columns.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.type->equals(*rhs.type); });

    if (widened_any)
    {
        try
        {
            auto result = analyzeExpressionAndSets(ast, widened_columns, context, required_source_columns);

            /// The widening is an internal detail of the analysis, so report the required source columns
            /// with their original (narrow) types. This keeps the reported list a subset of `columns` as
            /// the caller passed them - it is stored in the TTL description and used as the column set of
            /// every later rebuild of this AST, which widens them again from the real table types.
            if (required_source_columns)
            {
                NamesAndTypesList narrow_source_columns;
                for (const auto & required_column : *required_source_columns)
                {
                    /// The analysis can also report subcolumns (e.g. `j.ts` of a `JSON` column) that are
                    /// not in `columns`. Keep those as reported: widening only alters the types of
                    /// top-level temporal columns, and no subcolumn of a widened column changes its type
                    /// (`Nullable` is preserved, so `.null` stays `UInt8`), so the reported types match
                    /// what the narrow analysis would report.
                    if (auto original = columns.tryGetByName(required_column.name))
                        narrow_source_columns.push_back(*original);
                    else
                        narrow_source_columns.push_back(required_column);
                }
                *required_source_columns = std::move(narrow_source_columns);
            }

            return result;
        }
        catch (const Exception &) // NOLINT(bugprone-empty-catch): intentional fallback to the narrow analysis below
        {
            /// A function in the expression rejected the widened temporal type
            /// (e.g. `tumbleStart` requires `DateTime`, not `DateTime64`).
            /// Retry the analysis against the original (narrow) column types.
        }
    }

    return analyzeExpressionAndSets(ast, columns, context, required_source_columns);
}

/// Collect the argument expressions of every aggregate function found in the AST.
static void collectAggregateFunctionArguments(const ASTPtr & ast, ASTs & arguments)
{
    if (const auto * function = ast->as<ASTFunction>(); function && AggregateUtils::isAggregateFunction(*function))
    {
        if (function->arguments)
            for (const auto & argument : function->arguments->children)
                arguments.push_back(argument);
    }

    for (const auto & child : ast->children)
        collectAggregateFunctionArguments(child, arguments);
}

/// Validate the aggregate-function arguments of a `GROUP BY ... SET` assignment. These argument
/// expressions (e.g. `toDateTime(ts)` in `SET out = max(toDateTime(ts))`) are evaluated later by
/// TTLAggregationAlgorithm and are not part of the main TTL expression, so an unsupported
/// AggregateFunction-state consumer there would otherwise pass CREATE TABLE and fail at merge time.
static void checkTTLGroupBySetForAggregateFunctions(
    const ASTPtr & assignment_expression, const NamesAndTypesList & columns, const ContextPtr & context)
{
    ASTs aggregate_arguments;
    collectAggregateFunctionArguments(assignment_expression, aggregate_arguments);

    for (const auto & argument : aggregate_arguments)
    {
        auto argument_ast = argument->clone();
        auto argument_expression = buildExpressionAndSets(
            argument_ast, columns, context, nullptr, /*widen_temporal_columns=*/ false).expression;
        checkTTLExpressionForAggregateFunctions(argument_expression, /*expression_kind=*/ "GROUP BY SET ");
    }
}

ExpressionAndSets TTLDescription::buildExpression(const ContextPtr & context) const
{
    auto ast = expression_ast->clone();
    return buildExpressionAndSets(ast, expression_source_columns, context);
}

ExpressionAndSets TTLDescription::buildWhereExpression(const ContextPtr & context) const
{
    if (where_expression_ast)
    {
        auto ast = where_expression_ast->clone();
        /// Only the TTL timestamp expression needs widening. The `DELETE WHERE`
        /// predicate must keep the table's original static column types.
        return buildExpressionAndSets(ast, where_expression_source_columns, context, nullptr, false);
    }

    return {};
}

TTLDescription TTLDescription::getTTLFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    ContextPtr context,
    const KeyDescription & primary_key,
    TTLValidationMode validation_mode)
{
    TTLDescription result;
    const auto * ttl_element = definition_ast->as<ASTTTLElement>();

    /// First child is expression: `TTL expr TO DISK`
    if (ttl_element != nullptr)
        result.expression_ast = ttl_element->children.front()->clone();
    else /// It's columns TTL without any additions, just copy it
        result.expression_ast = definition_ast->clone();

    checkExpressionDoesntContainSubqueries(*result.expression_ast);

    const bool skip_validation = validation_mode != TTLValidationMode::Validate;

    /// Pin the `Variant`/`Dynamic` build strictness per the validation mode (see `TTLValidationMode` for
    /// the reasoning): strict for a validated user DDL, lenient when loading existing metadata. With
    /// `allow_suspicious_ttl_expressions` nothing is pinned - the escape hatch skips the TTL validator but
    /// does not override the session's mismatch policy, so the build reads the ambient settings exactly as
    /// any other expression build does.
    std::optional<TypeMismatchStrictnessOverride> build_strictness;
    if (validation_mode == TTLValidationMode::Validate)
        build_strictness.emplace(/*variant_throw_on_type_mismatch=*/ true, /*dynamic_throw_on_type_mismatch=*/ true);
    else if (validation_mode == TTLValidationMode::Attach)
        build_strictness.emplace(/*variant_throw_on_type_mismatch=*/ false, /*dynamic_throw_on_type_mismatch=*/ false);

    auto ttl_ast = result.expression_ast->clone();
    auto expression = buildExpressionAndSets(ttl_ast, columns.getAllPhysical(), context, &result.expression_source_columns).expression;
    result.expression_columns = expression->getRequiredColumnsWithTypes();

    result.result_column = expression->getSampleBlock().safeGetByPosition(0).name;

    ExpressionActionsPtr where_expression;

    if (ttl_element == nullptr) /// columns TTL
    {
        result.destination_type = DataDestinationType::DELETE;
        result.mode = TTLMode::DELETE;
    }
    else /// rows TTL
    {
        result.mode = ttl_element->mode;
        result.destination_type = ttl_element->destination_type;
        result.destination_name = ttl_element->destination_name;
        result.if_exists = ttl_element->if_exists;

        if (ttl_element->mode == TTLMode::DELETE)
        {
            if (ASTPtr where_expr_ast = ttl_element->where())
            {
                result.where_expression_ast = where_expr_ast->clone();

                ASTPtr ast = where_expr_ast->clone();
                where_expression
                = buildExpressionAndSets(
                    ast, columns.getAllPhysical(), context, &result.where_expression_source_columns, /*widen_temporal_columns=*/ false).expression;
                result.where_expression_columns = where_expression->getRequiredColumnsWithTypes();
                result.where_result_column = where_expression->getSampleBlock().safeGetByPosition(0).name;
            }
        }
        else if (ttl_element->mode == TTLMode::GROUP_BY)
        {
            const auto & pk_columns = primary_key.column_names;

            if (ttl_element->group_by_key.size() > pk_columns.size())
                throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "TTL Expression GROUP BY key should be a prefix of primary key");

            NameSet aggregation_columns_set;

            for (size_t i = 0; i < ttl_element->group_by_key.size(); ++i)
            {
                if (ttl_element->group_by_key[i]->getColumnName() != pk_columns[i])
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "TTL Expression GROUP BY key should be a prefix of primary key {} {}", ttl_element->group_by_key[i]->getColumnName(), pk_columns[i]);
            }

            std::vector<std::pair<String, ASTPtr>> aggregations;
            for (const auto & ast : ttl_element->group_by_assignments)
            {
                const auto assignment = ast->as<const ASTAssignment &>();
                auto ass_expression = assignment.expression();

                FindAggregateFunctionVisitor::Data data{false};
                FindAggregateFunctionVisitor(data).visit(ass_expression);

                if (!data.has_aggregate_function)
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                    "Invalid expression for assignment of column {}. Should contain an aggregate function", assignment.column_name);

                if (!skip_validation)
                    checkTTLGroupBySetForAggregateFunctions(ass_expression, columns.getAllPhysical(), context);

                ass_expression = addTypeConversionToAST(std::move(ass_expression), columns.getPhysical(assignment.column_name).type->getName());
                aggregations.emplace_back(assignment.column_name, std::move(ass_expression));
                aggregation_columns_set.insert(assignment.column_name);
            }

            if (aggregation_columns_set.size() != ttl_element->group_by_assignments.size())
                throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "Multiple aggregations set for one column in TTL Expression");

            result.group_by_keys = Names(pk_columns.begin(), pk_columns.begin() + ttl_element->group_by_key.size());

            for (auto [name, value] : aggregations)
            {
                auto syntax_result = TreeRewriter(context).analyze(value, columns.getAllPhysical(), {}, {}, true);
                auto expr_analyzer = ExpressionAnalyzer(value, syntax_result, context);

                TTLAggregateDescription set_part;
                set_part.column_name = name;
                set_part.expression_result_column_name = value->getColumnName();
                set_part.expression = expr_analyzer.getActions(false);

                /// The post-aggregation expression (including the implicit cast to the target column type)
                /// is executed later by TTLAggregationAlgorithm. When an aggregate returns an AggregateFunction
                /// state itself (e.g. `any(ts)`), casting it to an incompatible target type (e.g. `DateTime`)
                /// must be rejected here instead of failing during the TTL merge.
                if (!skip_validation)
                    checkTTLExpressionForAggregateFunctions(set_part.expression, /*expression_kind=*/ "GROUP BY SET ");

                result.set_parts.emplace_back(set_part);

                for (const auto & descr : expr_analyzer.getAnalyzedData().aggregate_descriptions)
                    result.aggregate_descriptions.push_back(descr);
            }
        }
        else if (ttl_element->mode == TTLMode::RECOMPRESS)
        {
            /// On `ATTACH` (loading stored metadata) the codec checks are relaxed the same way column codecs are:
            /// a table created on an earlier version must still load even if its recompression codec would now be
            /// rejected at `CREATE`, otherwise the server could fail to start after an upgrade. A create with
            /// `allow_suspicious_ttl_expressions` also skips them, matching `checkTTLExpression` below.
            result.recompression_codec =
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                    ttl_element->recompression_codec, {},
                    skip_validation ? CodecValidationSettings::trusted() : CodecValidationSettings(context->getSettingsRef()));
        }
    }

    checkTTLExpression(expression, result.result_column, skip_validation);

    if (where_expression && !skip_validation)
        checkTTLExpressionForAggregateFunctions(where_expression, /*expression_kind=*/ "WHERE ");

    return result;
}


TTLTableDescription::TTLTableDescription(const TTLTableDescription & other)
 : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
 , rows_ttl(other.rows_ttl)
 , rows_where_ttl(other.rows_where_ttl)
 , move_ttl(other.move_ttl)
 , recompression_ttl(other.recompression_ttl)
 , group_by_ttl(other.group_by_ttl)
{
}

TTLTableDescription & TTLTableDescription::operator=(const TTLTableDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    rows_ttl = other.rows_ttl;
    rows_where_ttl = other.rows_where_ttl;
    move_ttl = other.move_ttl;
    recompression_ttl = other.recompression_ttl;
    group_by_ttl = other.group_by_ttl;

    return *this;
}

TTLTableDescription TTLTableDescription::getTTLForTableFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    ContextPtr context,
    const KeyDescription & primary_key,
    TTLValidationMode validation_mode)
{
    TTLTableDescription result;
    if (!definition_ast)
        return result;

    result.definition_ast = definition_ast->clone();

    bool have_unconditional_delete_ttl = false;
    for (const auto & ttl_element_ptr : definition_ast->children)
    {
        auto ttl = TTLDescription::getTTLFromAST(ttl_element_ptr, columns, context, primary_key, validation_mode);
        if (ttl.mode == TTLMode::DELETE)
        {
            if (!ttl.where_expression_ast)
            {
                if (have_unconditional_delete_ttl)
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "More than one DELETE TTL expression without WHERE expression is not allowed");

                have_unconditional_delete_ttl = true;
                result.rows_ttl = ttl;
            }
            else
            {
                result.rows_where_ttl.emplace_back(std::move(ttl));
            }
        }
        else if (ttl.mode == TTLMode::RECOMPRESS)
        {
            result.recompression_ttl.emplace_back(std::move(ttl));
        }
        else if (ttl.mode == TTLMode::GROUP_BY)
        {
            result.group_by_ttl.emplace_back(std::move(ttl));
        }
        else
        {
            result.move_ttl.emplace_back(std::move(ttl));
        }
    }
    return result;
}

TTLTableDescription TTLTableDescription::parse(
    const String & str, const ColumnsDescription & columns, ContextPtr context, const KeyDescription & primary_key, TTLValidationMode validation_mode)
{
    TTLTableDescription result;
    if (str.empty())
        return result;

    ParserTTLExpressionList parser;
    ASTPtr ast = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    FunctionNameNormalizer::visit(ast.get());

    return getTTLForTableFromAST(ast, columns, context, primary_key, validation_mode);
}

}
