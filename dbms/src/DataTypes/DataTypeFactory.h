#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Common/IFactoryWithAliases.h>


#include <functional>
#include <memory>
#include <unordered_map>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;


/** Creates a data type by name of data type family and parameters.
  */
using FullTypesStack = std::vector<String>;
using DataTypeCreatorFunc = std::function<DataTypePtr(const ASTPtr &, FullTypesStack &)>;
class DataTypeFactory final : private boost::noncopyable, public IFactoryWithAliases<DataTypeCreatorFunc>
{
private:
    using SimpleCreator = std::function<DataTypePtr()>;
    using DataTypesDictionary = std::unordered_map<String, Creator>;
    using CreatorWithCustom = std::function<std::pair<DataTypePtr,DataTypeCustomDescPtr>(const ASTPtr & parameters)>;
    using SimpleCreatorWithCustom = std::function<std::pair<DataTypePtr,DataTypeCustomDescPtr>()>;

public:
    static DataTypeFactory & instance();

    DataTypePtr get(const String & full_name) const;
    DataTypePtr get(const String & family_name, const ASTPtr & parameters, std::vector<String> & full_types) const;
    DataTypePtr get(const ASTPtr & ast) const;
    DataTypePtr get(const ASTPtr & ast, std::vector<String> & full_types) const;

    /// Register a type family by its name.
    void registerDataType(const String & family_name, Creator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple data type, that have no parameters.
    void registerSimpleDataType(const String & name, SimpleCreator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a customized type family
    void registerDataTypeCustom(const String & family_name, CreatorWithCustom creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple customized data type
    void registerSimpleDataTypeCustom(const String & name, SimpleCreatorWithCustom creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

private:
    const Creator& findCreatorByName(const String & family_name) const;

private:
    DataTypesDictionary data_types;

    /// Case insensitive data types will be additionally added here with lowercased name.
    DataTypesDictionary case_insensitive_data_types;

    DataTypeFactory();
    ~DataTypeFactory() override;

    const DataTypesDictionary & getCreatorMap() const override { return data_types; }

    const DataTypesDictionary & getCaseInsensitiveCreatorMap() const override { return case_insensitive_data_types; }

    String getFactoryName() const override { return "DataTypeFactory"; }
};

}
