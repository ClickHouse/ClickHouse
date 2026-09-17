#include <Interpreters/UDT/StoredObjectTypeStringSlots.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <Common/StringUtils.h>

#include <algorithm>
#include <array>

namespace DB::UDT
{
namespace
{

using Contract = TableFunctionTypeStringSlotContract;
using ExpressionContract = StoredExpressionTypeStringSlotContract;
using ExpressionPosition = StoredExpressionTypeStringArgumentPosition;
using Mode = StoredObjectTypeStringSlotMode;
using Site = StoredObjectOccurrenceSite;
using Status = StoredObjectTypeStringSlotStatus;

constexpr std::array table_function_contracts{
    Contract{"input", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"null", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"format", Site::FormatSchemaString, Mode::ExactASTLayout, true},
    Contract{"values", Site::TableFunctionSchemaString, Mode::ContextDependentLayout, true},
    Contract{"SQLStandardValues", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"generateRandom", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"file", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"url", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"fileCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"urlCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"executable", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"redis", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"hive", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"ytsaurus", Site::TableFunctionSchemaString, Mode::ExactASTLayout},
    Contract{"mongodb", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"s3", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"gcs", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"cosn", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"oss", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"azureBlobStorage", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"hdfs", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"iceberg", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergS3", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergAzure", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergLocal", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergHDFS", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"deltaLake", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"deltaLakeS3", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"deltaLakeAzure", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"deltaLakeLocal", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"hudi", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimon", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimonS3", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimonAzure", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimonHDFS", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimonLocal", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"s3Cluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"azureBlobStorageCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"hdfsCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergS3Cluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergAzureCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergHDFSCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"icebergLocalCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"deltaLakeCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"deltaLakeS3Cluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"deltaLakeAzureCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"hudiCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimonCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimonS3Cluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimonAzureCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
    Contract{"paimonHDFSCluster", Site::TableFunctionSchemaString, Mode::AdapterOwnedLayout},
};

constexpr std::array stored_expression_contracts{
    ExpressionContract{"CAST", Site::UnclassifiedTypeString, ExpressionPosition::Second, true},
    ExpressionContract{"_CAST", Site::UnclassifiedTypeString, ExpressionPosition::Second, true},
    ExpressionContract{"accurateCast", Site::UnclassifiedTypeString, ExpressionPosition::Second, true},
    ExpressionContract{"accurateCastOrNull", Site::UnclassifiedTypeString, ExpressionPosition::Second, true},
    ExpressionContract{"accurateCastOrDefault", Site::UnclassifiedTypeString, ExpressionPosition::Second, true},
    ExpressionContract{"reinterpret", Site::UnclassifiedTypeString, ExpressionPosition::Second},
    ExpressionContract{"dynamicElement", Site::UnclassifiedTypeString, ExpressionPosition::Second},
    ExpressionContract{"variantElement", Site::UnclassifiedTypeString, ExpressionPosition::Second},
    ExpressionContract{"defaultValueOfTypeName", Site::UnclassifiedTypeString, ExpressionPosition::First},
    ExpressionContract{"getTypeSerializationStreams", Site::UnclassifiedTypeString, ExpressionPosition::First},
    ExpressionContract{"structureToProtobufSchema", Site::FormatSchemaString, ExpressionPosition::First},
    ExpressionContract{"structureToCapnProtoSchema", Site::FormatSchemaString, ExpressionPosition::First},
    ExpressionContract{"JSONExtract", Site::UnclassifiedTypeString, ExpressionPosition::Last},
    ExpressionContract{"JSONExtractCaseInsensitive", Site::UnclassifiedTypeString, ExpressionPosition::Last},
    ExpressionContract{"JSONExtractKeysAndValues", Site::UnclassifiedTypeString, ExpressionPosition::Last},
    ExpressionContract{"JSONExtractKeysAndValuesCaseInsensitive", Site::UnclassifiedTypeString, ExpressionPosition::Last},
};

constexpr std::array stored_setting_contracts{
    StoredSettingTypeStringSlotContract{"schema_inference_hints", Site::FormatSchemaString},
};

constexpr size_t format_contract_index = 2;
static_assert(table_function_contracts[format_contract_index].occurrence_site == Site::FormatSchemaString);

constexpr std::array<StorageEngineTypeStringSlotContract, 0> storage_engine_contracts{};

constexpr char asciiLower(char value) noexcept
{
    return value >= 'A' && value <= 'Z' ? static_cast<char>(value - 'A' + 'a') : value;
}

constexpr bool equalCaseInsensitive(std::string_view lhs, std::string_view rhs) noexcept
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t index = 0; index < lhs.size(); ++index)
    {
        if (asciiLower(lhs[index]) != asciiLower(rhs[index]))
            return false;
    }
    return true;
}

constexpr bool tableFunctionContractsAreClosed() noexcept
{
    for (size_t index = 0; index < table_function_contracts.size(); ++index)
    {
        const auto & contract = table_function_contracts[index];
        if (contract.function_name.empty()
            || (contract.occurrence_site != Site::TableFunctionSchemaString && contract.occurrence_site != Site::FormatSchemaString))
            return false;
        for (size_t previous = 0; previous < index; ++previous)
        {
            if (equalCaseInsensitive(table_function_contracts[previous].function_name, contract.function_name))
                return false;
        }
    }
    return true;
}

static_assert(tableFunctionContractsAreClosed());

constexpr bool storedExpressionContractsAreClosed() noexcept
{
    for (size_t index = 0; index < stored_expression_contracts.size(); ++index)
    {
        const auto & contract = stored_expression_contracts[index];
        if (contract.function_name.empty()
            || (contract.occurrence_site != Site::UnclassifiedTypeString && contract.occurrence_site != Site::FormatSchemaString))
            return false;
        for (size_t previous = 0; previous < index; ++previous)
        {
            if (equalCaseInsensitive(stored_expression_contracts[previous].function_name, contract.function_name))
                return false;
        }
    }
    return true;
}

static_assert(storedExpressionContractsAreClosed());

constexpr bool storedSettingContractsAreClosed() noexcept
{
    for (size_t index = 0; index < stored_setting_contracts.size(); ++index)
    {
        const auto & contract = stored_setting_contracts[index];
        if (contract.setting_name.empty() || contract.occurrence_site != Site::FormatSchemaString)
            return false;
        for (size_t previous = 0; previous < index; ++previous)
        {
            if (equalCaseInsensitive(stored_setting_contracts[previous].setting_name, contract.setting_name))
                return false;
        }
    }
    return true;
}

static_assert(storedSettingContractsAreClosed());

const Contract * findContract(std::string_view name) noexcept
{
    const auto found = std::find_if(
        table_function_contracts.begin(),
        table_function_contracts.end(),
        [name](const Contract & contract)
        { return contract.case_insensitive ? equalsCaseInsensitive(name, contract.function_name) : name == contract.function_name; });
    return found == table_function_contracts.end() ? nullptr : std::addressof(*found);
}

const ExpressionContract * findExpressionContract(std::string_view name) noexcept
{
    const auto found = std::find_if(
        stored_expression_contracts.begin(),
        stored_expression_contracts.end(),
        [name](const ExpressionContract & contract)
        { return contract.case_insensitive ? equalsCaseInsensitive(name, contract.function_name) : name == contract.function_name; });
    return found == stored_expression_contracts.end() ? nullptr : std::addressof(*found);
}

StoredObjectTypeStringSlotClassification
result(const Contract & contract, Status status, const IAST * expression = nullptr, UInt64 argument_ordinal = 0) noexcept
{
    return {
        .status = status,
        .occurrence_site = contract.occurrence_site,
        .expression = expression,
        .argument_ordinal = argument_ordinal,
    };
}

StoredObjectTypeStringSlotClassification
result(const ExpressionContract & contract, Status status, const IAST * expression = nullptr, UInt64 argument_ordinal = 0) noexcept
{
    return {
        .status = status,
        .occurrence_site = contract.occurrence_site,
        .expression = expression,
        .argument_ordinal = argument_ordinal,
    };
}

StoredObjectTypeStringSlotClassification exactAt(const Contract & contract, const ASTs & arguments, size_t argument_ordinal) noexcept
{
    if (argument_ordinal >= arguments.size() || !arguments[argument_ordinal])
        return result(contract, Status::UnclassifiedLayout);
    return result(contract, Status::ExactExpression, arguments[argument_ordinal].get(), argument_ordinal);
}

StoredObjectTypeStringSlotClassification
exactAt(const ExpressionContract & contract, const ASTs & arguments, size_t argument_ordinal) noexcept
{
    if (argument_ordinal >= arguments.size() || !arguments[argument_ordinal])
        return result(contract, Status::UnclassifiedLayout);
    return result(contract, Status::ExactExpression, arguments[argument_ordinal].get(), argument_ordinal);
}

StoredObjectTypeStringSlotClassification
exactOrPhysicalAuto(const Contract & contract, const ASTs & arguments, size_t argument_ordinal) noexcept
{
    const auto exact = exactAt(contract, arguments, argument_ordinal);
    const auto * literal = exact.expression ? exact.expression->as<ASTLiteral>() : nullptr;
    if (literal && literal->value.getType() == Field::Types::String && literal->value.safeGet<String>() == "auto")
        return result(contract, Status::NoExplicitSchemaString);
    return exact;
}

StoredObjectTypeStringSlotClassification classifyYTsaurus(const Contract & contract, const ASTs & arguments) noexcept
{
    size_t settings_count = 0;
    size_t non_settings_count = 0;
    size_t last_non_settings_ordinal = 0;
    for (size_t index = 0; index < arguments.size(); ++index)
    {
        if (!arguments[index])
            return result(contract, Status::UnclassifiedLayout);
        if (arguments[index]->as<ASTSetQuery>())
        {
            ++settings_count;
            continue;
        }
        ++non_settings_count;
        last_non_settings_ordinal = index;
    }
    if (settings_count > 1 || (non_settings_count != 2 && non_settings_count != 4))
        return result(contract, Status::UnclassifiedLayout);
    return exactAt(contract, arguments, last_non_settings_ordinal);
}

StoredObjectTypeStringSlotClassification classifyExactLayout(const Contract & contract, const ASTs & arguments) noexcept
{
    if (equalsCaseInsensitive(contract.function_name, "input") || equalsCaseInsensitive(contract.function_name, "null"))
    {
        if (arguments.empty())
            return result(contract, Status::NoExplicitSchemaString);
        return arguments.size() == 1 ? exactAt(contract, arguments, 0) : result(contract, Status::UnclassifiedLayout);
    }
    if (equalsCaseInsensitive(contract.function_name, "format"))
    {
        if (arguments.empty() || arguments.size() > 3)
            return result(contract, Status::UnclassifiedLayout);
        return arguments.size() == 3 ? exactOrPhysicalAuto(contract, arguments, 1) : result(contract, Status::NoExplicitSchemaString);
    }
    if (equalsCaseInsensitive(contract.function_name, "SQLStandardValues"))
        return arguments.empty() ? result(contract, Status::UnclassifiedLayout) : result(contract, Status::NoExplicitSchemaString);
    if (equalsCaseInsensitive(contract.function_name, "generateRandom"))
    {
        if (arguments.empty())
            return result(contract, Status::NoExplicitSchemaString);
        if (arguments.size() > 4 || !arguments.front())
            return result(contract, Status::UnclassifiedLayout);
        if (const auto * literal = arguments.front()->as<ASTLiteral>(); literal && literal->value.getType() != Field::Types::String)
            return result(contract, Status::NoExplicitSchemaString);
        return exactAt(contract, arguments, 0);
    }
    if (equalsCaseInsensitive(contract.function_name, "file"))
    {
        if (arguments.empty() || arguments.size() > 4)
            return result(contract, Status::UnclassifiedLayout);
        return arguments.size() >= 3 ? exactOrPhysicalAuto(contract, arguments, 2) : result(contract, Status::NoExplicitSchemaString);
    }
    if (equalsCaseInsensitive(contract.function_name, "executable"))
        return arguments.size() >= 3 ? exactAt(contract, arguments, 2) : result(contract, Status::UnclassifiedLayout);
    if (equalsCaseInsensitive(contract.function_name, "redis"))
    {
        if (arguments.size() < 3 || arguments.size() > 6)
            return result(contract, Status::UnclassifiedLayout);
        return exactAt(contract, arguments, 2);
    }
    if (equalsCaseInsensitive(contract.function_name, "hive"))
        return arguments.size() == 5 ? exactAt(contract, arguments, 3) : result(contract, Status::UnclassifiedLayout);
    if (equalsCaseInsensitive(contract.function_name, "ytsaurus"))
        return classifyYTsaurus(contract, arguments);
    return result(contract, Status::UnclassifiedLayout);
}

StoredObjectTypeStringSlotClassification classifyContextDependentLayout(const Contract & contract, const ASTs & arguments) noexcept
{
    if (!equalsCaseInsensitive(contract.function_name, "values") || arguments.empty())
        return result(contract, Status::UnclassifiedLayout);
    if (arguments.size() == 1 || !arguments.front())
        return result(contract, arguments.front() ? Status::NoExplicitSchemaString : Status::UnclassifiedLayout);
    const auto * literal = arguments.front()->as<ASTLiteral>();
    /// TableFunctionValues attempts schema parsing only for an ASTLiteral
    /// String. A dynamic/non-literal first argument is row data, not an
    /// unresolved schema owner.
    if (!literal || literal->value.getType() != Field::Types::String)
        return result(contract, Status::NoExplicitSchemaString);
    return result(contract, Status::ContextRequired, arguments.front().get(), 0);
}

}

std::span<const TableFunctionTypeStringSlotContract> getTableFunctionTypeStringSlotContracts() noexcept
{
    return table_function_contracts;
}

std::span<const TableFunctionTypeStringSlotContract> getFormatSchemaStringSlotContracts() noexcept
{
    return std::span<const TableFunctionTypeStringSlotContract>(table_function_contracts).subspan(format_contract_index, 1);
}

StoredObjectTypeStringSlotClassification classifyTableFunctionTypeStringSlot(const ASTFunction & function) noexcept
{
    const auto * contract = findContract(function.name);
    if (!contract)
        return {};
    if (function.getKind() != ASTFunction::Kind::ORDINARY_FUNCTION)
        return result(*contract, Status::UnclassifiedLayout);
    if (!function.arguments)
        return result(*contract, Status::UnclassifiedLayout);

    const auto & arguments = function.arguments->children;
    switch (contract->mode)
    {
        case Mode::ExactASTLayout: return classifyExactLayout(*contract, arguments);
        case Mode::ContextDependentLayout: return classifyContextDependentLayout(*contract, arguments);
        case Mode::AdapterOwnedLayout: return result(*contract, Status::ContextRequired);
    }
    return result(*contract, Status::UnclassifiedLayout);
}

StoredObjectNestedTableFunctionSlotClassification classifyStoredObjectNestedTableFunctionSlot(const ASTFunction & owner) noexcept
{
    using NestedStatus = StoredObjectNestedTableFunctionSlotStatus;

    const bool registered_owner = owner.name == "loop" || owner.name == "viewIfPermitted";
    if (!registered_owner)
        return {};
    if (owner.getKind() != ASTFunction::Kind::ORDINARY_FUNCTION || owner.parameters || !owner.arguments
        || std::count_if(
               owner.children.begin(), owner.children.end(), [&](const ASTPtr & child) { return child.get() == owner.arguments.get(); })
            != 1)
    {
        return {.status = NestedStatus::UnclassifiedLayout};
    }

    const auto & arguments = owner.arguments->children;
    if (owner.name == "loop")
    {
        /// The two-argument form is loop(database, table); both positions are
        /// scalar name expressions even when represented as ASTFunction.
        if (arguments.size() == 2 && arguments[0] && arguments[1])
            return {.status = NestedStatus::NoNestedTableFunction};
        if (arguments.size() != 1 || !arguments.front())
            return {.status = NestedStatus::UnclassifiedLayout};
        if (const auto * nested = arguments.front()->as<ASTFunction>())
            return {.status = NestedStatus::ExactTableFunction, .function = nested, .argument_ordinal = 0};
        if (arguments.front()->as<ASTIdentifier>())
            return {.status = NestedStatus::NoNestedTableFunction};
        return {.status = NestedStatus::UnclassifiedLayout};
    }

    if (arguments.size() != 2 || !arguments[0] || !arguments[1] || !arguments[0]->as<ASTSelectWithUnionQuery>())
    {
        return {.status = NestedStatus::UnclassifiedLayout};
    }
    if (const auto * nested = arguments[1]->as<ASTFunction>())
        return {.status = NestedStatus::ExactTableFunction, .function = nested, .argument_ordinal = 1};
    return {.status = NestedStatus::UnclassifiedLayout};
}

StoredObjectTableFunctionTypeStringTreeClassification classifyStoredObjectTableFunctionTypeStringTree(const ASTFunction & root) noexcept
{
    constexpr size_t maximum_nested_owner_depth = 256;
    std::array<const ASTFunction *, maximum_nested_owner_depth + 1> owners{};
    size_t owner_count = 0;
    const ASTFunction * function = &root;

    while (true)
    {
        if (owner_count == owners.size()
            || std::find(owners.begin(), owners.begin() + owner_count, function) != owners.begin() + owner_count)
        {
            return {
                .status = StoredObjectTableFunctionTypeStringTreeStatus::UnclassifiedLayout,
                .schema_owner = nullptr,
                .schema_slot = {},
                .nested_depth = 0,
            };
        }
        owners[owner_count++] = function;

        const auto nested = classifyStoredObjectNestedTableFunctionSlot(*function);
        switch (nested.status)
        {
            case StoredObjectNestedTableFunctionSlotStatus::ExactTableFunction:
                if (!nested.function)
                {
                    return {
                        .status = StoredObjectTableFunctionTypeStringTreeStatus::UnclassifiedLayout,
                        .schema_owner = nullptr,
                        .schema_slot = {},
                        .nested_depth = 0,
                    };
                }
                function = nested.function;
                break;
            case StoredObjectNestedTableFunctionSlotStatus::UnclassifiedLayout:
                return {
                    .status = StoredObjectTableFunctionTypeStringTreeStatus::UnclassifiedLayout,
                    .schema_owner = nullptr,
                    .schema_slot = {},
                    .nested_depth = 0,
                };
            case StoredObjectNestedTableFunctionSlotStatus::UnregisteredOwner:
            case StoredObjectNestedTableFunctionSlotStatus::NoNestedTableFunction:
                return {
                    .status = StoredObjectTableFunctionTypeStringTreeStatus::Complete,
                    .schema_owner = function,
                    .schema_slot = classifyTableFunctionTypeStringSlot(*function),
                    .nested_depth = static_cast<UInt64>(owner_count - 1),
                };
        }
    }
}

std::span<const StoredExpressionTypeStringSlotContract> getStoredExpressionTypeStringSlotContracts() noexcept
{
    return stored_expression_contracts;
}

StoredObjectTypeStringSlotClassification classifyStoredExpressionTypeStringSlot(const ASTFunction & function) noexcept
{
    const auto * contract = findExpressionContract(function.name);
    if (!contract || function.getKind() != ASTFunction::Kind::ORDINARY_FUNCTION)
        return {};
    if (function.parameters || !function.arguments)
        return result(*contract, Status::UnclassifiedLayout);

    const auto & arguments = function.arguments->children;
    switch (contract->argument_position)
    {
        case ExpressionPosition::First: return exactAt(*contract, arguments, 0);
        case ExpressionPosition::Second: return exactAt(*contract, arguments, 1);
        case ExpressionPosition::Last:
            if (arguments.empty())
                return result(*contract, Status::UnclassifiedLayout);
            return exactAt(*contract, arguments, arguments.size() - 1);
    }
    return result(*contract, Status::UnclassifiedLayout);
}

std::span<const StoredSettingTypeStringSlotContract> getStoredSettingTypeStringSlotContracts() noexcept
{
    return stored_setting_contracts;
}

const StoredSettingTypeStringSlotContract * tryGetStoredSettingTypeStringSlotContract(std::string_view setting_name) noexcept
{
    const auto found = std::find_if(
        stored_setting_contracts.begin(),
        stored_setting_contracts.end(),
        [setting_name](const auto & contract) { return setting_name == contract.setting_name; });
    return found == stored_setting_contracts.end() ? nullptr : std::addressof(*found);
}

std::span<const StorageEngineTypeStringSlotContract> getStorageEngineTypeStringSlotContracts() noexcept
{
    return storage_engine_contracts;
}

StoredObjectTypeStringSlotClassification classifyStorageEngineTypeStringSlot(const ASTFunction &) noexcept
{
    return {};
}

}
