#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include <DataTypes/IDataType.h>
#include <ext/singleton.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/** Creates a data type by name of data type family and parameters.
  */
class DataTypeFactory final : public ext::singleton<DataTypeFactory>
{
private:
    using Creator = std::function<DataTypePtr(const ASTPtr & parameters)>;
    using SimpleCreator = std::function<DataTypePtr()>;
    using DataTypesDictionary = std::unordered_map<String, Creator>;
    using AliasMap = std::unordered_map<String, String>; // alias -> original type

public:
    DataTypePtr get(const String & full_name) const;
    DataTypePtr get(const String & family_name, const ASTPtr & parameters) const;
    DataTypePtr get(const ASTPtr & ast) const;

    /// For compatibility with SQL, it's possible to specify that certain data type name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /// Register a type family by its name.
    void registerDataType(const String & family_name, Creator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple data type, that have no parameters.
    void registerSimpleDataType(const String & name, SimpleCreator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /** Register additional name for datatype.
     * real_name datatype have to be already registered.
     */
    void registerAlias(const String & alias_name, const String & real_name, CaseSensitiveness case_sensitiveness = CaseSensitive);

    std::vector<String> getAllDataTypeNames() const;

    bool isCaseInsensitive(const String & name) const;

    const String & aliasTo(const String & name) const;

    bool isAlias(const String & name) const;

private:
    DataTypesDictionary data_types;

    /// Case insensitive data types will be additionally added here with lowercased name.
    DataTypesDictionary case_insensitive_data_types;

    /// Alias map to data_types from previous two maps
    AliasMap aliases;

    /// Case insensitive aliases
    AliasMap case_insensitive_aliases;

    DataTypeFactory();
    friend class ext::singleton<DataTypeFactory>;
};

}
