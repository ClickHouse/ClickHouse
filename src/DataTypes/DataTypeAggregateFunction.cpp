#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnAggregateFunction.h>

#include <Common/SipHash.h>
#include <Common/AlignedBuffer.h>
#include <Common/FieldVisitorToString.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/Serializations/SerializationAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/FieldVisitorToCastedLiteral.h>
#include <Parsers/parseFieldFromCastedLiteral.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}


DataTypeAggregateFunction::DataTypeAggregateFunction(AggregateFunctionPtr function_, const DataTypes & argument_types_,
                            const Array & parameters_, std::optional<size_t> version_)
    : function(std::move(function_))
    , argument_types(argument_types_)
    , parameters(parameters_)
    , version(version_)
{
}

String DataTypeAggregateFunction::getFunctionName() const
{
    return function->getName();
}


String DataTypeAggregateFunction::doGetName() const
{
    return getNameImpl(true);
}


String DataTypeAggregateFunction::getNameWithoutVersion() const
{
    return getNameImpl(false);
}


size_t DataTypeAggregateFunction::getVersion() const
{
    if (version)
        return *version;
    return function->getDefaultVersion();
}

DataTypePtr DataTypeAggregateFunction::getReturnType() const
{
    return function->getResultType();
}

DataTypePtr DataTypeAggregateFunction::getReturnTypeToPredict() const
{
    return function->getReturnTypeToPredict();
}

bool DataTypeAggregateFunction::isVersioned() const
{
    return function->isVersioned();
}

DataTypePtr DataTypeAggregateFunction::cloneWithVersion(size_t version_) const
{
    auto result = std::make_shared<DataTypeAggregateFunction>(function, argument_types, parameters, version_);

    /// The customization has to be carried over, not recreated: `SimpleAggregateFunction(f, T)` is
    /// stored as `T` plus a `DataTypeCustomSimpleAggregateFunction` name, and code as central as the
    /// AggregatingMergeTree and SummingMergeTree merge algorithms recognises such a column by
    /// dynamic_cast on that very name object. Dropping it would silently turn the column into a
    /// plain `AggregateFunction` one.
    if (auto customization = cloneCustomization())
        result->setCustomization(std::move(customization));

    return result;
}

String DataTypeAggregateFunction::getNameImpl(bool with_version) const
{
    WriteBufferFromOwnString stream;
    stream << "AggregateFunction(";

    /// If aggregate function does not support versioning its version is 0 and is not printed.
    auto data_type_version = getVersion();
    if (with_version && data_type_version)
        stream << data_type_version << ", ";
    stream << function->getName();

    if (!parameters.empty())
    {
        stream << '(';
        if (function->shouldPrintParametersWithTypes())
        {
            FieldVisitorToCastedLiteral visitor;
            for (size_t i = 0, size = parameters.size(); i < size; ++i)
            {
                if (i)
                    stream << ", ";
                stream << applyVisitor(visitor, parameters[i]);
            }
        }
        else
        {
            FieldVisitorToString visitor;
            for (size_t i = 0, size = parameters.size(); i < size; ++i)
            {
                if (i)
                    stream << ", ";
                stream << applyVisitor(visitor, parameters[i]);
            }
        }
        stream << ')';
    }

    for (const auto & argument_type : argument_types)
        stream << ", " << argument_type->getName();

    stream << ')';
    return stream.str();
}


MutableColumnPtr DataTypeAggregateFunction::createColumn() const
{
    return ColumnAggregateFunction::create(function, getVersion());
}


/// Create empty state
Field DataTypeAggregateFunction::getDefault() const
{
    Field field = AggregateFunctionStateData();
    field.safeGet<AggregateFunctionStateData>().name = getName();

    AlignedBuffer place_buffer(function->sizeOfData(), function->alignOfData());
    AggregateDataPtr place = place_buffer.data();

    function->create(place);

    try
    {
        WriteBufferFromString buffer_from_field(field.safeGet<AggregateFunctionStateData>().data);
        function->serialize(place, buffer_from_field, version);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    function->destroy(place);

    return field;
}

bool DataTypeAggregateFunction::strictEquals(const DataTypePtr & lhs_state_type, const DataTypePtr & rhs_state_type, bool ignore_variant)
{
    const auto * lhs_state = typeid_cast<const DataTypeAggregateFunction *>(lhs_state_type.get());
    const auto * rhs_state = typeid_cast<const DataTypeAggregateFunction *>(rhs_state_type.get());

    if (!lhs_state || !rhs_state)
        return false;

    if (!ignore_variant && lhs_state->function->getStateVariant() != rhs_state->function->getStateVariant())
        return false;

    if (lhs_state->function->getName() != rhs_state->function->getName())
        return false;

    if (lhs_state->parameters.size() != rhs_state->parameters.size())
        return false;

    for (size_t i = 0; i < lhs_state->parameters.size(); ++i)
        if (lhs_state->parameters[i] != rhs_state->parameters[i])
            return false;

    if (lhs_state->argument_types.size() != rhs_state->argument_types.size())
        return false;

    for (size_t i = 0; i < lhs_state->argument_types.size(); ++i)
        if (!lhs_state->argument_types[i]->equals(*rhs_state->argument_types[i]))
            return false;

    return true;
}

void DataTypeAggregateFunction::updateHashImpl(SipHash & hash) const
{
    hash.update(getFunctionName());
    hash.update(parameters.size());
    for (const auto & param : parameters)
        hash.update(param.getType());
    hash.update(argument_types.size());
    for (const auto & arg_type : argument_types)
        arg_type->updateHash(hash);
    if (version)
        hash.update(*version);
    hash.update(static_cast<UInt8>(function->getStateVariant()));
}

bool DataTypeAggregateFunction::equalsIgnoringVariant(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    auto lhs_state_type = function->getNormalizedStateType();
    auto rhs_state_type = typeid_cast<const DataTypeAggregateFunction &>(rhs).function->getNormalizedStateType();

    return strictEquals(lhs_state_type, rhs_state_type, /*ignore_variant=*/ true);
}

bool DataTypeAggregateFunction::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    auto lhs_state_type = function->getNormalizedStateType();
    auto rhs_state_type = typeid_cast<const DataTypeAggregateFunction &>(rhs).function->getNormalizedStateType();

    return strictEquals(lhs_state_type, rhs_state_type);
}


SerializationPtr DataTypeAggregateFunction::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationAggregateFunction::create(function, getName(), getVersion());
}


namespace
{

/// Extract a single AggregateFunction parameter value from its AST node.
Field parseAggregateFunctionParameter(const ASTPtr & param_ast, const String & function_name)
{
    try
    {
        return parseFieldFromCastedLiteral(param_ast);
    }
    catch (Exception & e)
    {
        e.addMessage("while parsing aggregate function '{}'", function_name);
        throw;
    }
}

}

static DataTypePtr create(const ASTPtr & arguments)
{
    String function_name;
    DataTypes argument_types;
    Array params_row;
    std::optional<size_t> version;

    if (!arguments || arguments->children.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Data type AggregateFunction requires parameters: "
                        "version(optionally), name of aggregate function and list of data types for arguments");

    ASTPtr data_type_ast = arguments->children[0];
    size_t argument_types_start_idx = 1;

    /* If aggregate function definition doesn't have version, it will have in AST children args [ASTFunction, types...] - in case
     * it is parametric, or [ASTIdentifier, types...] - otherwise. If aggregate function has version in AST, then it will be:
     * [ASTLiteral, ASTFunction (or ASTIdentifier), types...].
     */
    if (auto * version_ast = arguments->children[0]->as<ASTLiteral>())
    {
        if (arguments->children.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Data type AggregateFunction has version, but it requires at least one more parameter - name of aggregate function");
        version = version_ast->value.safeGet<UInt64>();
        data_type_ast = arguments->children[1];
        argument_types_start_idx = 2;
    }

    auto action = NullsAction::EMPTY;
    if (const auto * parametric = data_type_ast->as<ASTFunction>())
    {
        if (parametric->parameters)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected level of parameters to aggregate function");

        function_name = parametric->name;
        action = parametric->getNullsAction();

        if (parametric->arguments)
        {
            const ASTs & parameters = parametric->arguments->children;
            params_row.resize(parameters.size());

            for (size_t i = 0; i < parameters.size(); ++i)
                params_row[i] = parseAggregateFunctionParameter(parameters[i], function_name);
        }
    }
    else if (auto opt_name = tryGetIdentifierName(data_type_ast))
    {
        function_name = *opt_name;
    }
    else if (data_type_ast->as<ASTLiteral>())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Aggregate function name for data type AggregateFunction must "
                        "be passed as identifier (without quotes) or function");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected AST element {} passed as aggregate function name for data type AggregateFunction. "
                        "Must be identifier or function", data_type_ast->getID());

    for (size_t i = argument_types_start_idx; i < arguments->children.size(); ++i)
        argument_types.push_back(DataTypeFactory::instance().get(arguments->children[i]));

    if (function_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty name of aggregate function passed");

    AggregateFunctionProperties properties;
    AggregateFunctionPtr function = AggregateFunctionFactory::instance().get(function_name, action, argument_types, params_row, properties);
    return std::make_shared<DataTypeAggregateFunction>(function, argument_types, params_row, version);
}

namespace
{

/// Returns `type` with the requested version assigned to every nested versioned aggregate function,
/// or `type` itself when nothing has to change.
///
/// This deliberately does not go through `transformTypesRecursively`: that helper rebuilds every
/// Array/Tuple/Map wrapper through make_shared and so drops custom type names. Since here the leaf
/// is replaced rather than mutated, the rebuilt tree is what the caller ends up with, and losing the
/// names would rewrite `Nested(...)` into `Array(Tuple(...))` - a type that is not only sent to the
/// client but also stored in the table metadata on ATTACH.
///
/// Only the wrappers that `transformTypesRecursively` used to descend into are handled, so nothing
/// that was reached before is skipped now. Nullable is among them: a state cannot be directly inside
/// Nullable, but a Tuple can, and `Nullable(Tuple(AggregateFunction(...)))` is reachable with
/// enable_nullable_tuple_type.
DataTypePtr withVersionedAggregateFunctions(const DataTypePtr & type, bool if_empty, std::optional<size_t> revision)
{
    /// A rebuilt wrapper keeps the customization of the original: a custom name can sit on the
    /// wrapper rather than on the leaf, as in `SimpleAggregateFunction(anyLast, Array(AggregateFunction(...)))`,
    /// which is stored as the `Array` plus a `DataTypeCustomSimpleAggregateFunction` name.
    auto keep_customization = [&](const DataTypePtr & rebuilt)
    {
        if (auto customization = type->cloneCustomization())
            rebuilt->setCustomization(std::move(customization));
        return rebuilt;
    };

    /// Rewrites `elements` in place and reports whether any of them changed.
    auto update_elements = [&](DataTypes & elements)
    {
        bool changed = false;
        for (auto & element : elements)
        {
            auto updated = withVersionedAggregateFunctions(element, if_empty, revision);
            changed |= updated.get() != element.get();
            element = std::move(updated);
        }
        return changed;
    };

    if (const auto * aggregate_function_type = typeid_cast<const DataTypeAggregateFunction *>(type.get()))
    {
        if (!aggregate_function_type->isVersioned())
            return type;

        /// Keep an already-explicit version when if_empty is requested.
        if (if_empty && aggregate_function_type->getVersionIfExplicit())
            return type;

        const size_t version = revision ? aggregate_function_type->getFunction()->getVersionFromRevision(*revision) : 0;
        if (aggregate_function_type->getVersionIfExplicit() == version)
            return type;

        return aggregate_function_type->cloneWithVersion(version);
    }

    /// Nested has to be matched before Array: it *is* an Array(Tuple(...)), and its custom name
    /// embeds the element types, so the name has to be rebuilt along with them to stay in sync.
    if (const auto * nested_name = typeid_cast<const DataTypeNestedCustomName *>(type->getCustomName()))
    {
        DataTypes elements = nested_name->getElements();
        if (!update_elements(elements))
            return type;

        /// Built directly rather than through createNested, which derives the type from the printed
        /// name: version 0 is deliberately not printed, so a name round trip would turn a leaf
        /// explicitly pinned to version 0 back into an unversioned one using the latest version.
        const auto & names = nested_name->getNames();
        auto rebuilt = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(elements, names));
        rebuilt->setCustomization(std::make_unique<DataTypeCustomDesc>(
            std::make_shared<DataTypeNestedCustomName>(elements, names)));
        return rebuilt;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto nested_type = withVersionedAggregateFunctions(array_type->getNestedType(), if_empty, revision);
        if (nested_type.get() == array_type->getNestedType().get())
            return type;
        return keep_customization(std::make_shared<DataTypeArray>(std::move(nested_type)));
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements = tuple_type->getElements();
        if (!update_elements(elements))
            return type;
        return keep_customization(tuple_type->hasExplicitNames()
            ? std::make_shared<DataTypeTuple>(elements, tuple_type->getElementNames())
            : std::make_shared<DataTypeTuple>(elements));
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        DataTypes elements = {map_type->getKeyType(), map_type->getValueType()};
        if (!update_elements(elements))
            return type;
        return keep_customization(std::make_shared<DataTypeMap>(elements[0], elements[1]));
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto nested_type = withVersionedAggregateFunctions(nullable_type->getNestedType(), if_empty, revision);
        if (nested_type.get() == nullable_type->getNestedType().get())
            return type;
        return keep_customization(std::make_shared<DataTypeNullable>(std::move(nested_type)));
    }

    return type;
}

}

void setVersionToAggregateFunctions(DataTypePtr & type, bool if_empty, std::optional<size_t> revision)
{
    type = withVersionedAggregateFunctions(type, if_empty, revision);
}


void registerDataTypeAggregateFunction(DataTypeFactory & factory)
{
    factory.registerDataType("AggregateFunction", create, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
## Description {#description}

All [Aggregate functions](/sql-reference/aggregate-functions) in ClickHouse have
an implementation-specific intermediate state that can be serialized to an
`AggregateFunction` data type and stored in a table. This is usually done by
means of a [materialized view](/reference/statements/create/view).

There are two aggregate function [combinators](/sql-reference/aggregate-functions/combinators)
commonly used with the `AggregateFunction` type:

- The [`-State`](/sql-reference/aggregate-functions/combinators#-state) aggregate function combinator, which when appended to an aggregate
function name, produces `AggregateFunction` intermediate states.
- The [`-Merge`](/sql-reference/aggregate-functions/combinators#-merge) aggregate
function combinator, which is used to get the final result of an aggregation
from the intermediate states.

## Syntax {#syntax}

```sql
AggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Parameters**

- `aggregate_function_name` - The name of an aggregate function. If the function
is parametric, then its parameters should be specified too.
- `types_of_arguments` - The types of the aggregate function arguments.

for example:

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

## Usage {#usage}

### Data Insertion {#data-insertion}

To insert data into a table with columns of type `AggregateFunction`, you can
use `INSERT SELECT` with aggregate functions and the
[`-State`](/sql-reference/aggregate-functions/combinators#-state) aggregate
function combinator.

For example, to insert into columns of type `AggregateFunction(uniq, UInt64)` and
`AggregateFunction(quantiles(0.5, 0.9), UInt64)` you would use the following
aggregate functions with combinators.

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

In contrast to functions `uniq` and `quantiles`, `uniqState` and `quantilesState`
(with `-State` combinator appended) return the state, rather than the final value.
In other words, they return a value of `AggregateFunction` type.

In the results of the `SELECT` query, values of type `AggregateFunction` have
implementation-specific binary representations for all of the ClickHouse output
formats.

There is a special Session level setting `aggregate_function_input_format` that allows to build state from the input values.
It supports the following formats:

- `state` - binary string with the serialized state (the default).
If you dump data into, for example, the `TabSeparated` format with a `SELECT`
query, then this dump can be loaded back using the `INSERT` query.
- `value` - the format will expect a single value of the argument of the aggregate function, or in the case of multiple arguments, a tuple of them; that will be deserialized to form the relevant state
- `array` - the format will expect an Array of values, as described in the values option above; all the elements of the array will be aggregated to form the state

### Data Selection {#data-selection}

When selecting data from `AggregatingMergeTree` table, use the `GROUP BY` clause
and the same aggregate functions as for when you inserted the data, but use the
[`-Merge`](/sql-reference/aggregate-functions/combinators#-merge) combinator.

An aggregate function with the `-Merge` combinator appended to it takes a set of
states, combines them, and returns the result of the complete data aggregation.

For example, the following two queries return the same result:

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Usage Example {#usage-example}

See [AggregatingMergeTree](/reference/engines/table-engines/mergetree-family/aggregatingmergetree) engine description.

## Related Content {#related-content}

- Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
- [MergeState](/sql-reference/aggregate-functions/combinators#-mergestate)
combinator.
- [State](/sql-reference/aggregate-functions/combinators#-state) combinator.
)DOCS_MD",
            .syntax = "AggregateFunction(name, types...)",
            .examples = {},
            .related = {"SimpleAggregateFunction"},
        });
}

bool hasAggregateFunctionType(const DataTypePtr & type)
{
    auto result = false;
    auto check = [&](const IDataType & t)
    {
        result |= WhichDataType(t).isAggregateFunction();
    };

    check(*type);
    type->forEachChild(check);
    return result;
}

}
