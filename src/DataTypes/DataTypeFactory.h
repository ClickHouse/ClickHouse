#pragma once

#include <Parsers/IAST_fwd.h>
#include <Common/IFactoryWithAliases.h>
#include <DataTypes/DataTypeCustom.h>

#include <functional>
#include <memory>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;


/** Creates a data type by name of data type family and parameters.
  */
class DataTypeFactory final : private boost::noncopyable, public IFactoryWithAliases<std::function<DataTypePtr(const ASTPtr & parameters)>>
{
    using Base = IFactoryWithAliases<std::function<DataTypePtr(const ASTPtr & parameters)>>;
private:
    using SimpleCreator = std::function<DataTypePtr()>;
    using CreatorWithCustom = std::function<std::pair<DataTypePtr, DataTypeCustomDescPtr>(const ASTPtr & parameters)>;
    using SimpleCreatorWithCustom = std::function<std::pair<DataTypePtr,DataTypeCustomDescPtr>()>;

public:
    static DataTypeFactory & instance();

    DataTypePtr get(const String & full_name) const;
    DataTypePtr get(const String & family_name, const ASTPtr & parameters) const;
    DataTypePtr get(const ASTPtr & ast) const;
    DataTypePtr getCustom(DataTypeCustomDescPtr customization) const;

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

    using DataTypesDictionary = std::unordered_map<String, Value>;

    DataTypesDictionary data_types;
    DataTypesDictionary case_insensitive_data_types;

    DataTypeFactory();

    const Base::OriginalNameMap & getOriginalNameMap() const override { return data_types; }
    const Base::OriginalNameMap & getOriginalCaseInsensitiveNameMap() const override { return case_insensitive_data_types; }

    String getFactoryName() const override { return "DataTypeFactory"; }
};

}
