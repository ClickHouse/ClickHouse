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
    mutable std::optional<size_t> version;

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

    static bool strictEquals(const DataTypePtr & lhs_state_type, const DataTypePtr & rhs_state_type);
    bool equals(const IDataType & rhs) const override;
    void updateHashImpl(SipHash & hash) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }

    SerializationPtr doGetDefaultSerialization() const override;
    bool supportsSparseSerialization() const override { return false; }

    bool isVersioned() const;

    /// Version is not empty only if it was parsed from AST or implicitly cast to 0 or version according
    /// to server revision.
    /// It is ok to have an empty version value here - then for serialization a default (latest)
    /// version is used. This method is used to force some zero version to be used instead of
    /// default, or to set version for serialization in distributed queries.
    void setVersion(size_t version_, bool if_empty) const
    {
        if (version && if_empty)
            return;

        version = version_;
    }

    void updateVersionFromRevision(size_t revision, bool if_empty) const;
};

void setVersionToAggregateFunctions(DataTypePtr & type, bool if_empty, std::optional<size_t> revision = std::nullopt);

/// Checks type of any nested type is DataTypeAggregateFunction.
bool hasAggregateFunctionType(const DataTypePtr & type);

}
