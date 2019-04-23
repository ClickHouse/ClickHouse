#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Common/IFactoryWithAliases.h>

#include <ext/singleton.h>

#include <functional>
#include <memory>
#include <unordered_map>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class IDataTypeDomain;
using DataTypeDomainPtr = std::unique_ptr<const IDataTypeDomain>;


/** Creates a data type by name of data type family and parameters.
  */
class DataTypeFactory final : public ext::singleton<DataTypeFactory>, public IFactoryWithAliases<std::function<DataTypePtr(const ASTPtr & parameters)>>
{
private:
    using SimpleCreator = std::function<DataTypePtr()>;
    using DataTypesDictionary = std::unordered_map<String, Creator>;

public:
    DataTypePtr get(const String & full_name) const;
    DataTypePtr get(const String & family_name, const ASTPtr & parameters) const;
    DataTypePtr get(const ASTPtr & ast) const;

    /// Register a type family by its name.
    void registerDataType(const String & family_name, Creator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple data type, that have no parameters.
    void registerSimpleDataType(const String & name, SimpleCreator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    // Register a domain - a refinement of existing type.
    void registerDataTypeDomain(const String & type_name, DataTypeDomainPtr domain, CaseSensitiveness case_sensitiveness = CaseSensitive);

private:
    static void setDataTypeDomain(const IDataType & data_type, const IDataTypeDomain & domain);
    const Creator& findCreatorByName(const String & family_name) const;

private:
    DataTypesDictionary data_types;

    /// Case insensitive data types will be additionally added here with lowercased name.
    DataTypesDictionary case_insensitive_data_types;

    // All domains are owned by factory and shared amongst DataType instances.
    std::vector<DataTypeDomainPtr> all_domains;

    DataTypeFactory();
    ~DataTypeFactory() override;

    const DataTypesDictionary & getCreatorMap() const override { return data_types; }

    const DataTypesDictionary & getCaseInsensitiveCreatorMap() const override { return case_insensitive_data_types; }

    String getFactoryName() const override { return "DataTypeFactory"; }

    friend class ext::singleton<DataTypeFactory>;
};

}
