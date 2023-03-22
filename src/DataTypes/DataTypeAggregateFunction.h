#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

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
    size_t getVersion() const;

public:
    static constexpr bool is_parametric = true;

    DataTypeAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_,
                              const Array & parameters_, std::optional<size_t> version_ = std::nullopt)
        : function(function_)
        , argument_types(argument_types_)
        , parameters(parameters_)
        , version(version_)
    {
    }

    String getFunctionName() const { return function->getName(); }
    AggregateFunctionPtr getFunction() const { return function; }

    String doGetName() const override;
    String getNameWithoutVersion() const;
    const char * getFamilyName() const override { return "AggregateFunction"; }
    TypeIndex getTypeId() const override { return TypeIndex::AggregateFunction; }

    Array getParameters() const { return parameters; }

    bool canBeInsideNullable() const override { return false; }

    DataTypePtr getReturnType() const { return function->getReturnType(); }
    DataTypePtr getReturnTypeToPredict() const { return function->getReturnTypeToPredict(); }
    DataTypes getArgumentsDataTypes() const { return argument_types; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }

    SerializationPtr doGetDefaultSerialization() const override;
    bool supportsSparseSerialization() const override { return false; }

    bool isVersioned() const { return function->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const { return function->getVersionFromRevision(revision); }

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
};

}
