#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

namespace
{
    /// Returns true if the AST tree contains an `ASTIdentifier` with the given name.
    bool hasIdentifier(const IAST & ast, std::string_view name)
    {
        if (const auto * id = ast.as<ASTIdentifier>())
            return id->name() == name;
        for (const auto & child : ast.children)
            if (child && hasIdentifier(*child, name))
                return true;
        return false;
    }
}


ASTPtr TimeSeriesIDGenerator::getDefault(
    const DataTypePtr & id_type, const TimeSeriesSettings & settings, const StorageID & table_id)
{
    /// Build a list of arguments for a hash function.
    /// All hash functions below allow multiple arguments, so we use two arguments: metric_name, all_tags.
    ASTs arguments_for_hash_function;
    arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

    if (settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
    {
        arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::AllTags));
    }
    else
    {
        const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(column_name));
        }
        arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
    }

    auto make_hash_function = [&](const String & function_name) -> boost::intrusive_ptr<ASTFunction>
    {
        auto function = make_intrusive<ASTFunction>();
        function->name = function_name;
        function->arguments = make_intrusive<ASTExpressionList>();
        function->children.push_back(function->arguments);
        function->arguments->children = std::move(arguments_for_hash_function);
        return function;
    };

    WhichDataType id_which(*id_type);

    if (id_which.isUInt64())
        return make_hash_function("sipHash64");

    if (id_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
        return make_hash_function("sipHash128");

    if (id_which.isUUID())
        return makeASTFunction("reinterpretAsUUID", make_hash_function("sipHash128"));

    if (id_which.isUInt128())
        return makeASTFunction("reinterpretAsUInt128", make_hash_function("sipHash128"));

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
        table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
}


bool TimeSeriesIDGenerator::usesAllTags(const ASTPtr & id_generator)
{
    if (!id_generator)
        return false;
    return hasIdentifier(*id_generator, TimeSeriesColumnNames::AllTags);
}

}
