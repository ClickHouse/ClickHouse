#pragma once

#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

class UserDefinedDataType;
using UserDefinedDataTypePtr = std::shared_ptr<UserDefinedDataType>;

class UserDefinedDataType final : public IDataType
{
private:
    DataTypePtr nested;
    ASTPtr nested_ast;
    String type_name;
    FunctionOverloadResolverPtr input_function;
    FunctionOverloadResolverPtr output_function;
    ContextPtr context;

public:
    UserDefinedDataType();

    DataTypePtr getNested() const;
    ASTPtr getNestedAST() const;
    String getTypeName() const;
    FunctionOverloadResolverPtr getInputFunction() const;
    FunctionOverloadResolverPtr getOutputFunction() const;
    ContextPtr getContext() const;

    void setNested(const DataTypePtr & nested_);
    void setNestedAST(const ASTPtr & nested_ast_);
    void setTypeName(const String & type_name_);
    void setInputFunction(const FunctionOverloadResolverPtr & function_);
    void setOutputFunction(const FunctionOverloadResolverPtr & function_);
    void setContext(const ContextPtr & context_);

    TypeIndex getTypeId() const override { return nested->getTypeId(); }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return type_name.c_str(); }

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool isComparable() const override { return nested->isComparable(); }
    bool canBeInsideNullable() const override { return nested->canBeInsideNullable(); }

    DataTypePtr tryGetSubcolumnType(const String & subcolumn_name) const override;
    ColumnPtr getSubcolumn(const String & subcolumn_name, const IColumn & column) const override;
    SerializationPtr getSubcolumnSerialization(const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter) const override;
    SerializationPtr doGetDefaultSerialization() const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;
};
}
