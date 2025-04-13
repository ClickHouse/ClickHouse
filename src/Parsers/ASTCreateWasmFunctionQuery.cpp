#include <Parsers/ASTCreateWasmFunctionQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>

#include <Common/quoteString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <IO/Operators.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

String ASTCreateWasmFunctionQuery::getID(char delim) const
{
    return fmt::format("CreateWasmFunctionQuery{}{}", delim, getIdentifierName(function_name_ast));
}

ASTPtr ASTCreateWasmFunctionQuery::clone() const
{
    auto res = std::make_shared<ASTCreateWasmFunctionQuery>(*this);
    res->children.clear();

    res->setName(function_name_ast->clone());
    res->setArguments(arguments_ast->clone());
    res->setReturnType(result_type_ast->clone());
    res->setModuleName(module_name_ast->clone());
    if (abi_ast)
        res->setAbi(abi_ast->clone());

    return res;
}

void ASTCreateWasmFunctionQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    const auto *current_hilite = settings.hilite ? hilite_keyword : "";
    const auto *reset_hilite = settings.hilite ? hilite_none : "";

    ostr << current_hilite << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "FUNCTION ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << reset_hilite;

    if (function_name_ast)
        function_name_ast->format(ostr, settings, state, frame);

    formatOnCluster(ostr, settings);

    ostr << current_hilite << " LANGUAGE WASM" << reset_hilite;

    if (arguments_ast)
    {
        ostr << current_hilite << " ARGUMENTS " << reset_hilite << "(";
        arguments_ast->format(ostr, settings, state, frame);
        ostr << current_hilite << ")" << reset_hilite;
    }

    if (result_type_ast)
    {
        ostr << current_hilite << " RETURNS " << reset_hilite;
        result_type_ast->format(ostr, settings, state, frame);
    }

    if (module_name_ast)
    {
        ostr << current_hilite << " FROM " << reset_hilite;
        module_name_ast->format(ostr, settings, state, frame);

        if (source_function_name_ast)
        {
            ostr << current_hilite << " :: " << reset_hilite;
            source_function_name_ast->format(ostr, settings, state, frame);
        }
    }

    if (module_hash_ast)
    {
        ostr << current_hilite << " SHA256_HASH " << reset_hilite;
        module_hash_ast->format(ostr, settings, state, frame);
    }

    if (abi_ast)
    {
        ostr << current_hilite << " ABI " << reset_hilite;
        abi_ast->format(ostr, settings, state, frame);
    }

    if (!function_settings.empty())
    {
        ostr << current_hilite << " SETTINGS " << reset_hilite;

        auto settings_changes_ast = std::make_shared<ASTSetQuery>();
        settings_changes_ast->changes = function_settings;
        settings_changes_ast->is_standalone = false;
        settings_changes_ast->format(ostr, settings, state, frame);
    }
}

String ASTCreateWasmFunctionQuery::getFunctionName() const
{
    return getIdentifierName(function_name_ast);
}

static String getAstAsStringLiteral(const ASTPtr & ast)
{
    if (const auto * literal = typeid_cast<const ASTLiteral *>(ast.get()))
        return literal->value.safeGet<String>();
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ASTLiteral, got '{}'", ast->formatForErrorMessage());
}

void ASTCreateWasmFunctionQuery::setModuleHash(String hash_str)
{
    module_hash_ast = std::make_shared<ASTLiteral>(hash_str);
}

ASTCreateWasmFunctionQuery::Definition ASTCreateWasmFunctionQuery::validateAndGetDefinition() const
{
    ASTCreateWasmFunctionQuery::Definition info;

    info.function_name = getFunctionName();

    const auto & data_type_factory = DataTypeFactory::instance();
    for (const auto & argument_type : arguments_ast->children)
    {
        const auto * name_type_pair = argument_type->as<ASTNameTypePair>();
        if (name_type_pair)
        {
            info.argument_names.push_back(name_type_pair->name);
            info.argument_types.push_back(data_type_factory.get(name_type_pair->type));
        }
        else
        {
            info.argument_names.push_back("");
            info.argument_types.push_back(data_type_factory.get(argument_type));
        }
    }

    info.result_type = data_type_factory.get(result_type_ast);

    info.module_name = getAstAsStringLiteral(module_name_ast);
    if (module_hash_ast)
        info.module_hash = getAstAsStringLiteral(module_hash_ast);

    if (source_function_name_ast)
        info.source_function_name = getAstAsStringLiteral(source_function_name_ast);

    if (abi_ast)
    {
        String abi_name = getIdentifierName(abi_ast);
        info.abi_version = getWasmAbiFromString(abi_name);
    }

    for (const auto & setting : function_settings)
        info.settings.trySet(setting.name, setting.value);

    return info;
}

}
