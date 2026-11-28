#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>


namespace DB
{

/** Type - the state of the aggregate function.
  * Type parameters is an aggregate function, the types of its arguments, and its parameters (for parametric aggregate functions).
  *
  * Data type can support versioning for serialization of aggregate function state.
  * Version 0 also means no versioning. When a table with versioned data type is attached, its version is parsed from AST. If
  * there is no version in AST, then it is either attach with no version in metadata (then version is 0) or it
  * is a new data type (then version is default - latest).
  */
class DataTypeAggregateFunction final : public IDataType
{
private:
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array parameters;
    std::optional<size_t> version;

    String getNameImpl(bool with_version) const;

public:
    static constexpr bool is_parametric = true;

    DataTypeAggregateFunction(AggregateFunctionPtr function_, const DataTypes & argument_types_,
                              const Array & parameters_, std::optional<size_t> version_ = std::nullopt);

    size_t getVersion() const;

    /// The version exactly as it was set (parsed from AST or assigned), without collapsing
    /// std::nullopt to the function's default. Empty means "no explicit version".
    std::optional<size_t> getVersionIfExplicit() const { return version; }

    String getFunctionName() const;
    AggregateFunctionPtr getFunction() const { return function; }

    String doGetName() const override;
    String getNameWithoutVersion() const;
    const char * getFamilyName() const override { return "AggregateFunction"; }
    TypeIndex getTypeId() const override { return TypeIndex::AggregateFunction; }

    Array getParameters() const { return parameters; }

    bool canBeInsideNullable() const override { return false; }

    DataTypePtr getReturnType() const;
    DataTypePtr getReturnTypeToPredict() const;
    DataTypes getArgumentsDataTypes() const { return argument_types; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    /// Compares name, parameters, and argument types.
    /// When ignore_variant is false (default), also compares the state variant (Aggregation vs Window).
    static bool strictEquals(const DataTypePtr & lhs_state_type, const DataTypePtr & rhs_state_type, bool ignore_variant = false);

    /// True if `state_type_name` denotes a state with the same binary representation as (function, version).
    /// The names can differ: the aggregate function factory strips LowCardinality from the argument types,
    /// while the declared type keeps it, so `AggregateFunction(argMax, LowCardinality(String), DateTime)`
    /// and `AggregateFunction(argMax, String, DateTime)` describe the very same state.
    static bool nameMatchesState(const String & state_type_name, const AggregateFunctionPtr & function, size_t version);

    /// Same as equals() but ignores the state variant (Aggregation vs Window).
    bool equalsIgnoringVariant(const IDataType & rhs) const;

    bool equals(const IDataType & rhs) const override;
    void updateHashImpl(SipHash & hash) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }

    SerializationPtr doGetSerialization(const SerializationInfoSettings &) const override;
    bool supportsSparseSerialization() const override { return false; }

    bool isVersioned() const;

    /// The version is set once, at construction: parsed from AST, decoded from the binary type
    /// encoding, or chosen by `setVersionToAggregateFunctions`, which replaces the type rather than
    /// modifying it. There is deliberately no setter: a type object is typically shared - notably
    /// with the table metadata a block was read from - so pinning a version in place would be
    /// visible to everyone else holding the same type, and racy against them reading it.
    ///
    /// Whether the version was pinned explicitly, as opposed to falling back to the default one.
    bool hasExplicitVersion() const { return version.has_value(); }
};

/// Pins the state version of every versioned aggregate function nested in `type` to the one that
/// corresponds to `revision`, or to 0 if no revision is given. With `if_empty`, a version that is
/// already pinned explicitly is kept. The nested types are replaced rather than modified in place,
/// because a type object is typically shared - notably with the table metadata a block was read from.
void setVersionToAggregateFunctions(DataTypePtr & type, bool if_empty, std::optional<size_t> revision = std::nullopt);

/// For a freshly declared column type (`CREATE TABLE`): pins the state version the current server
/// revision maps to, but only where that version is newer than the default the function would fall
/// back to anyway, and never over an explicitly spelled version. The pin makes the version part of
/// the persisted type name, so the column keeps its layout when a newer server changes the default.
void pinCurrentStateVersionToAggregateFunctions(DataTypePtr & type);

/// Checks type of any nested type is DataTypeAggregateFunction.
bool hasAggregateFunctionType(const DataTypePtr & type);

}
