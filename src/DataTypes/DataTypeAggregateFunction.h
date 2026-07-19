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

    /// The explicitly assigned version, if any. Empty means "not set": getVersion() then
    /// falls back to the function's default (latest) version. Version is set only at
    /// construction (parsed from AST, decoded from binary, or via setVersionToAggregateFunctions).
    std::optional<size_t> getVersionIfExplicit() const { return version; }

    /// Returns a copy of this type with the given version baked in via the constructor.
    /// Used to carry a serialization version without mutating the (shared) type object.
    std::shared_ptr<const DataTypeAggregateFunction> cloneWithVersion(size_t version_) const
    {
        return std::make_shared<DataTypeAggregateFunction>(function, argument_types, parameters, version_);
    }
};

/// Assigns a serialization version to every nested DataTypeAggregateFunction in `type`,
/// replacing (never mutating) each versioned leaf with a copy carrying the version, so
/// concurrent serializations of a shared type object never race on it. `type` may be
/// rebuilt in place. `if_empty` keeps an already-explicit version; `revision` picks the
/// version from the server/client revision (nullopt forces version 0).
void setVersionToAggregateFunctions(DataTypePtr & type, bool if_empty, std::optional<size_t> revision = std::nullopt);

/// Checks type of any nested type is DataTypeAggregateFunction.
bool hasAggregateFunctionType(const DataTypePtr & type);

}
