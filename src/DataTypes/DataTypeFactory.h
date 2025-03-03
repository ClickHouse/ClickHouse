#pragma once

#include <Parsers/IAST_fwd.h>
#include <Common/IFactoryWithAliases.h>
#include <DataTypes/DataTypeCustom.h>


#include <functional>
#include <memory>
#include <unordered_map>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;


/** Creates a data type by name of data type family and parameters.
  */
class DataTypeFactory final : private boost::noncopyable, public IFactoryWithAliases<std::function<DataTypePtr(const ASTPtr & parameters)>>
{
private:
    using SimpleCreator = std::function<DataTypePtr()>;
    using DataTypesDictionary = std::unordered_map<String, Value>;
    using CreatorWithCustom = std::function<std::pair<DataTypePtr, DataTypeCustomDescPtr>(const ASTPtr & parameters)>;
    using SimpleCreatorWithCustom = std::function<std::pair<DataTypePtr,DataTypeCustomDescPtr>()>;

public:
    static DataTypeFactory & instance();

    DataTypePtr get(const String & full_name) const;
    DataTypePtr get(const String & family_name, const ASTPtr & parameters) const;
    DataTypePtr get(const ASTPtr & ast) const;
    DataTypePtr getCustom(DataTypeCustomDescPtr customization) const;
    DataTypePtr getCustom(const String & base_name, DataTypeCustomDescPtr customization) const;

    /// Return nullptr in case of error.
    DataTypePtr tryGet(const String & full_name) const;
    DataTypePtr tryGet(const String & family_name, const ASTPtr & parameters) const;
    DataTypePtr tryGet(const ASTPtr & ast) const;

    /// Register a type family by its name.
    void registerDataType(const String & family_name, Value creator, Case case_sensitiveness = Case::Sensitive);

    /// Register a simple data type, that have no parameters.
    void registerSimpleDataType(const String & name, SimpleCreator creator, Case case_sensitiveness = Case::Sensitive);

    /// Register a customized type family
    void registerDataTypeCustom(const String & family_name, CreatorWithCustom creator, Case case_sensitiveness = Case::Sensitive);

    /// Register a simple customized data type
    void registerSimpleDataTypeCustom(const String & name, SimpleCreatorWithCustom creator, Case case_sensitiveness = Case::Sensitive);

private:
    template <bool nullptr_on_error>
    DataTypePtr getImpl(const String & full_name) const;
    template <bool nullptr_on_error>
    DataTypePtr getImpl(const String & family_name, const ASTPtr & parameters) const;
    template <bool nullptr_on_error>
    DataTypePtr getImpl(const ASTPtr & ast) const;
    template <bool nullptr_on_error>
    const Value * findCreatorByName(const String & family_name) const;

    DataTypesDictionary data_types;

    /// Case insensitive data types will be additionally added here with lowercased name.
    DataTypesDictionary case_insensitive_data_types;

    DataTypeFactory();

    const DataTypesDictionary & getMap() const override { return data_types; }

    const DataTypesDictionary & getCaseInsensitiveMap() const override { return case_insensitive_data_types; }

    String getFactoryName() const override { return "DataTypeFactory"; }
};

void registerDataTypeNumbers(DataTypeFactory & factory);
void registerDataTypeDecimal(DataTypeFactory & factory);
void registerDataTypeDate(DataTypeFactory & factory);
void registerDataTypeDate32(DataTypeFactory & factory);
void registerDataTypeDateTime(DataTypeFactory & factory);
void registerDataTypeString(DataTypeFactory & factory);
void registerDataTypeFixedString(DataTypeFactory & factory);
void registerDataTypeEnum(DataTypeFactory & factory);
void registerDataTypeArray(DataTypeFactory & factory);
void registerDataTypeTuple(DataTypeFactory & factory);
void registerDataTypeMap(DataTypeFactory & factory);
void registerDataTypeNullable(DataTypeFactory & factory);
void registerDataTypeNothing(DataTypeFactory & factory);
void registerDataTypeUUID(DataTypeFactory & factory);
void registerDataTypeIPv4andIPv6(DataTypeFactory & factory);
void registerDataTypeAggregateFunction(DataTypeFactory & factory);
void registerDataTypeNested(DataTypeFactory & factory);
void registerDataTypeInterval(DataTypeFactory & factory);
void registerDataTypeLowCardinality(DataTypeFactory & factory);
void registerDataTypeDomainBool(DataTypeFactory & factory);
void registerDataTypeDomainSimpleAggregateFunction(DataTypeFactory & factory);
void registerDataTypeDomainGeo(DataTypeFactory & factory);
void registerDataTypeObjectDeprecated(DataTypeFactory & factory);
void registerDataTypeVariant(DataTypeFactory & factory);
void registerDataTypeDynamic(DataTypeFactory & factory);
void registerDataTypeJSON(DataTypeFactory & factory);

}
