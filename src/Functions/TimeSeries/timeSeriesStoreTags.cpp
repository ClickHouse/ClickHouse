#include <Functions/FunctionFactory.h>

#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesStoreTags(id, [('tag_name_1', 'tag_value_1'), ...], 'tag_name_2', 'tag_value_2', ...) returns `id`
/// and stores the mapping between the identifier of a time series and its tags in the query context so that
/// they can later be extracted by function timeSeriesIdToTags().
class FunctionTimeSeriesStoreTags final : public IFunction, public WithContext
{
public:
    static constexpr auto name = "timeSeriesStoreTags";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTimeSeriesStoreTags>(context_); }
    /// Defer query-context access to `executeImpl` so the function can be instantiated
    /// without a query context (e.g. by `system.functions` which only needs to read
    /// `getSignatureString`).
    explicit FunctionTimeSeriesStoreTags(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    /// There should be 2 or more arguments.
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Function timeSeriesStoreTags(id, ...) always returns `id`, so it's deterministic.
    bool isDeterministic() const override { return true; }

    /// Stateful: writes to the per-query tags collector read by timeSeriesIdToGroup() etc.
    bool isStateful() const override { return true; }

    /// Disable constant folding: the side effect (storing tags in the per-query `ContextTimeSeriesTagsCollector`)
    /// must run at execution time, not analysis time.
    bool isSuitableForConstantFolding() const override { return false; }

    /// This function allows NULLs as a way to specify that some tags don't have values.
    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Declarative signature — returns the `id` argument as-is, so the
    /// result type follows the first argument's type. The mandatory prefix is
    /// `(id, tags_array)`; the loose `(tag_name, tag_value)` pairs are an
    /// optional group repeated by the ellipsis. Writing the pair as a bracketed
    /// `[T1, V1]` group keeps the preceding `tags_array` argument out of the
    /// repeated unit (the ellipsis-walk-back would otherwise fold the adjacent
    /// `Array(...) | Nothing` argument into it), so the accepted arities are
    /// `2 + 2 * N` and calls like `timeSeriesStoreTags(id, [])` are valid.
    String getSignatureString() const override
    {
        return "(I : Any,"
               " Array(Tuple(String, String)) | Nothing,"
               " [T1 : MaybeNullable(StringOrFixedString | IsNothing),"
               " V1 : MaybeNullable(StringOrFixedString | IsNothing)], ...) -> I";
    }

    /// The declarative signature above is documentation-only — the exact accepted shapes
    /// aren't expressible in the DSL yet (the `id` capture `I : Any` is looser than the
    /// helper-accepted set — any comparable non-`Nothing` type except `Variant` and types with
    /// dynamic subcolumns, plus their `Nullable` / `LowCardinality` wrappers — and the
    /// `tags_array` matcher is narrower than the helper's `Map` / `FixedString`-element /
    /// `Nullable` / `LowCardinality` shapes). The
    /// `ColumnsWithTypeAndName` override below is authoritative (it runs the helpers). Route
    /// the types-only path to it too, so the base `IFunction::getReturnTypeImpl(DataTypes)`
    /// fallback never applies the approximate signature string as a validator.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        ColumnsWithTypeAndName columns;
        columns.reserve(arguments.size());
        for (const auto & type : arguments)
            columns.emplace_back(nullptr, type, String{});
        return getReturnTypeImpl(columns);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return arguments[0].type;
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with at least 2 arguments: {}(id, [('tag_name_1', 'tag_value_1), ...], 'tag_name_2', 'tag_value_2', ...)",
                            name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForID(name, arguments, 0, /* allow_nullable = */ true);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypesForTagNamesAndValues(name, arguments, 1);
    }

    /// A dry run is used to compute headers / partial results during query planning
    /// (e.g. constant folding in `tryMergeExpressions`). It must not perform the side effect
    /// of storing tags, nor access the query context via `getContext`: the context the function
    /// was created with may already have expired by plan-optimization time, which would otherwise
    /// raise a `Context has expired` logical error. The function returns its `id` argument
    /// unchanged, so the dry-run result is just that column.
    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        return arguments[0].column;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagNamesAndValuesFromArguments(name, arguments, 1);
        auto tags_collector = getContext()->getQueryContext()->getTimeSeriesTagsCollector();
        tags_collector->storeTags(arguments[0].column, tags_vector);
        return arguments[0].column;
    }
};


REGISTER_FUNCTION(TimeSeriesStoreTags)
{
    FunctionDocumentation::Description description = R"(
Stores in the query context a mapping between a specified identifier of a time series and a set of tags.
Functions [timeSeriesIdToTags()](/reference/functions/regular-functions/time-series-functions#timeSeriesIdToTags)
and [timeSeriesIdToGroup()](/reference/functions/regular-functions/time-series-functions#timeSeriesIdToGroup)
can be used to access this mapping later during the query execution.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesStoreTags(id, tags_array, separate_tag_name_1, separate_tag_value_1, ...)";
    FunctionDocumentation::Arguments arguments = {
        {"id", "Identifier of a time series. Can be of any comparable type. Rows with NULL identifiers are skipped.", {"Any"}},
        {"tags_array", "Array of pairs (tag_name, tag_value).", {"Array(Tuple(String, String))", "NULL"}},
        {"separate_tag_name_i", "The name of a tag.", {"String", "FixedString"}},
        {"separate_tag_value_i", "The value of a tag.", {"String", "FixedString", "Nullable(String)"}}\
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the identifier of a time series (i.e. just the first argument)."
    };
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SELECT 8374283493092 AS id,
       timeSeriesStoreTags(id, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS same_id,
       throwIf(same_id != id),
       timeSeriesIdToTags(same_id),
       timeSeriesGroupToTags(timeSeriesIdToGroup(same_id))
        )",
        R"(
┌────────────id─┬───────same_id─┬─throwIf(notEquals(same_id, id))─┬─timeSeriesIdToTags(same_id)────────────────────────────────────────┬─timeSeriesGroupToTags(timeSeriesIdToGroup(same_id))────────────────┐
│ 8374283493092 │ 8374283493092 │                               0 │ [('__name__','http_requests_count'),('env','dev'),('region','eu')] │ [('__name__','http_requests_count'),('env','dev'),('region','eu')] │
└───────────────┴───────────────┴─────────────────────────────────┴────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesStoreTags>(documentation);
}

}
