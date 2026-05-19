#include <Parsers/ASTCreateWasmFunctionQuery.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
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
    extern const int BAD_ARGUMENTS;
}

String ASTCreateWasmFunctionQuery::getID(char delim) const
{
    return fmt::format("CreateWasmFunctionQuery{}{}", delim, getIdentifierName(function_name_ast));
}

ASTPtr ASTCreateWasmFunctionQuery::clone() const
{
    auto res = make_intrusive<ASTCreateWasmFunctionQuery>(*this);
    res->children.clear();

    res->setName(function_name_ast->clone());
    res->setArguments(arguments_ast->clone());
    res->setReturnType(result_type_ast->clone());
    res->setModuleName(module_name_ast->clone());
    if (source_function_name_ast)
        res->setSourceFunctionName(source_function_name_ast->clone());
    if (module_hash_ast)
        res->setModuleHash(module_hash_ast->clone());
    if (abi_ast)
        res->setAbi(abi_ast->clone());

    return res;
}

void ASTCreateWasmFunctionQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{

    ostr << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "FUNCTION ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";


    if (function_name_ast)
        function_name_ast->format(ostr, settings, state, frame);

    formatOnCluster(ostr, settings);

    ostr << " LANGUAGE WASM";

    if (arguments_ast)
    {
        ostr << " ARGUMENTS " << "(";
        arguments_ast->format(ostr, settings, state, frame);
        ostr << ")";
    }

    if (result_type_ast)
    {
        ostr << " RETURNS ";
        result_type_ast->format(ostr, settings, state, frame);
    }

    if (module_name_ast)
    {
        ostr << " FROM ";
        module_name_ast->format(ostr, settings, state, frame);

        if (source_function_name_ast)
        {
            ostr << " :: ";
            source_function_name_ast->format(ostr, settings, state, frame);
        }
    }

    if (abi_ast)
    {
        ostr << " ABI ";
        abi_ast->format(ostr, settings, state, frame);
    }

    if (is_deterministic)
        ostr << " DETERMINISTIC";

    if (module_hash_ast)
    {
        ostr << " SHA256_HASH ";
        module_hash_ast->format(ostr, settings, state, frame);
    }

    if (!function_settings.empty())
    {
        ostr << " SETTINGS ";

        auto settings_changes_ast = make_intrusive<ASTSetQuery>();
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
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected ASTLiteral, got '{}'", ast->formatForErrorMessage());
}

void ASTCreateWasmFunctionQuery::setModuleHash(String hash_str)
{
    module_hash_ast = make_intrusive<ASTLiteral>(hash_str);
}

void ASTCreateWasmFunctionQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "CreateWasmFunctionQuery");

    w.writeBool("or_replace", or_replace);
    w.writeBool("if_not_exists", if_not_exists);
    w.writeBool("is_deterministic", is_deterministic);

    if (!cluster.empty())
        w.writeString("cluster", cluster);

    w.writeChild("function_name", function_name_ast);
    w.writeChild("arguments", arguments_ast);
    w.writeChild("result_type", result_type_ast);
    w.writeChild("module_name", module_name_ast);
    w.writeChild("source_function_name", source_function_name_ast);
    w.writeChild("module_hash", module_hash_ast);
    w.writeChild("abi", abi_ast);

    if (!function_settings.empty())
    {
        w.writeKey("function_settings");
        auto & o = w.getOut();
        const auto & fs = w.getFormatSettings();
        o << '[';
        for (size_t i = 0; i < function_settings.size(); ++i)
        {
            if (i > 0) o << ',';
            o << "{\"name\":";
            writeJSONString(function_settings[i].name, o, fs);
            w.writeFieldValue("value", function_settings[i].value);
            o << '}';
        }
        o << ']';
    }
}

void ASTCreateWasmFunctionQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    or_replace = r.getBool("or_replace");
    if_not_exists = r.getBool("if_not_exists");
    is_deterministic = r.getBool("is_deterministic");
    cluster = r.getString("cluster");

    children.clear();
    function_name_ast = nullptr;
    arguments_ast = nullptr;
    result_type_ast = nullptr;
    module_name_ast = nullptr;
    module_hash_ast = nullptr;
    source_function_name_ast = nullptr;
    abi_ast = nullptr;
    function_settings.clear();

    if (auto ast = r.readChild("function_name"))
        setName(std::move(ast));
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'function_name' for `CreateWasmFunctionQuery` during AST JSON deserialization");
    if (auto ast = r.readChild("arguments"))
        setArguments(std::move(ast));
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'arguments' for `CreateWasmFunctionQuery` during AST JSON deserialization");
    if (auto ast = r.readChild("result_type"))
        setReturnType(std::move(ast));
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'result_type' for `CreateWasmFunctionQuery` during AST JSON deserialization");
    if (auto ast = r.readChild("module_name"))
        setModuleName(std::move(ast));
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'module_name' for `CreateWasmFunctionQuery` during AST JSON deserialization");
    if (auto ast = r.readChild("source_function_name"))
        setSourceFunctionName(std::move(ast));
    if (auto ast = r.readChild("module_hash"))
        setModuleHash(std::move(ast));
    if (auto ast = r.readChild("abi"))
        setAbi(std::move(ast));

    if (r.has("function_settings"))
    {
        auto arr = r.getArray("function_settings");
        if (!arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'function_settings' is not an array during AST JSON deserialization");
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto change_obj = arr->getObject(i);
            if (!change_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'function_settings' array during AST JSON deserialization", i);
            SettingChange change;
            change.name = change_obj->getValue<String>("name");
            auto value_obj = change_obj->getObject("value");
            if (!value_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'value' at index {} in 'function_settings' array during AST JSON deserialization", i);
            change.value = JSONObjectReader::readFieldFromObject(*value_obj);
            function_settings.push_back(std::move(change));
        }
    }
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

    info.is_deterministic = is_deterministic;

    for (const auto & setting : function_settings)
        info.settings.trySet(setting.name, setting.value);

    return info;
}

}
