#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
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
    extern const int INCORRECT_QUERY;
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

    auto make_hash_function = [](const String & function_name, ASTs arguments) -> boost::intrusive_ptr<ASTFunction>
    {
        auto function = make_intrusive<ASTFunction>();
        function->name = function_name;
        function->arguments = make_intrusive<ASTExpressionList>();
        function->children.push_back(function->arguments);
        function->arguments->children = std::move(arguments);
        return function;
    };

    /// Makes an expression calculating a hash of `arguments` represented as a value of type `type`,
    /// returns null if `type` is not one of the plain identifier types supported by the generator.
    auto make_hash_expression = [&](const IDataType & type, ASTs arguments) -> ASTPtr
    {
        WhichDataType which(type);

        if (which.isUInt64())
            return make_hash_function("sipHash64", std::move(arguments));

        if (which.isFixedString() && typeid_cast<const DataTypeFixedString &>(type).getN() == 16)
            return make_hash_function("sipHash128", std::move(arguments));

        if (which.isUUID())
            return makeASTFunction("reinterpretAsUUID", make_hash_function("sipHash128", std::move(arguments)));

        if (which.isUInt128())
            return makeASTFunction("reinterpretAsUInt128", make_hash_function("sipHash128", std::move(arguments)));

        return nullptr;
    };

    /// An identifier of a plain type is a hash of the metric name and the tags.
    if (auto expression = make_hash_expression(*id_type, arguments_for_hash_function))
        return expression;

    /// For a two-component identifier Tuple(F, S) the first component is a hash of the metric name only,
    /// and the second component is a hash of the metric name and the tags.
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(id_type.get()))
    {
        const auto & element_types = tuple_type->getElements();
        if (element_types.size() == 2)
        {
            ASTPtr first = make_hash_expression(*element_types[0], ASTs{make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName)});
            ASTPtr second = first ? make_hash_expression(*element_types[1], arguments_for_hash_function) : nullptr;
            if (first && second)
                return makeASTFunction("tuple", std::move(first), std::move(second));
        }
    }

    throw Exception(ErrorCodes::INCORRECT_QUERY,
        "{}: An expression generating identifiers must be specified explicitly for the {} column of type {} - "
        "either as a DEFAULT expression of that column or in the 'id_generator' setting. "
        "An expression can be chosen automatically only for types UInt64, UInt128, UUID, FixedString(16), "
        "and tuples of two of those types",
        table_id.getNameForLogs(), TimeSeriesColumnNames::ID, id_type->getName());
}


bool TimeSeriesIDGenerator::usesAllTags(const ASTPtr & id_generator)
{
    if (!id_generator)
        return false;
    return hasIdentifier(*id_generator, TimeSeriesColumnNames::AllTags);
}

}
