#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <DataTypes/IDataType.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Common/SettingsChanges.h>

namespace DB
{

class ASTCreateWasmFunctionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    struct Definition
    {
        String function_name;
        Strings argument_names;
        DataTypes argument_types;
        DataTypePtr result_type;
        String module_name;
        String module_hash;
        String source_function_name;
        WasmAbiVersion abi_version = WasmAbiVersion::RowDirect;

        WebAssemblyFunctionSettings settings;
    };

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override;

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateWasmFunctionQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

    Definition validateAndGetDefinition() const;
    String getFunctionName() const;

    void setName(ASTPtr ast) { function_name_ast = children.emplace_back(std::move(ast)); }
    void setArguments(ASTPtr ast) { arguments_ast = children.emplace_back(std::move(ast)); }
    void setReturnType(ASTPtr ast) { result_type_ast = children.emplace_back(std::move(ast)); }
    void setModuleName(ASTPtr ast) { module_name_ast = children.emplace_back(std::move(ast)); }
    void setModuleHash(ASTPtr ast) { module_hash_ast = children.emplace_back(std::move(ast)); }
    void setModuleHash(String hash_str);
    void setSourceFunctionName(ASTPtr ast) { source_function_name_ast = children.emplace_back(std::move(ast)); }
    void setAbi(ASTPtr ast) { abi_ast = children.emplace_back(std::move(ast)); }
    void setSettings(SettingsChanges settings_) { function_settings = std::move(settings_); }

    /// Rebuild children in the canonical order that formatImpl uses.
    /// Must be called after parsing to ensure consistent tree hashing.
    void normalizeChildrenOrder()
    {
        children.clear();
        if (function_name_ast) children.push_back(function_name_ast);
        if (arguments_ast) children.push_back(arguments_ast);
        if (result_type_ast) children.push_back(result_type_ast);
        if (module_name_ast) children.push_back(module_name_ast);
        if (source_function_name_ast) children.push_back(source_function_name_ast);
        if (module_hash_ast) children.push_back(module_hash_ast);
        if (abi_ast) children.push_back(abi_ast);
    }

private:
    ASTPtr function_name_ast;
    ASTPtr arguments_ast;
    ASTPtr result_type_ast;
    ASTPtr module_name_ast;
    ASTPtr module_hash_ast = nullptr;
    ASTPtr source_function_name_ast = nullptr;
    ASTPtr abi_ast = nullptr;
    SettingsChanges function_settings;
};

}
