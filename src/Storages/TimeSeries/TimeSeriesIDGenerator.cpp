#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

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

    /// Makes an AST for calling a hash function with the specified arguments.
    boost::intrusive_ptr<ASTFunction> makeHashFunctionCall(const String & function_name, ASTs arguments)
    {
        auto function = make_intrusive<ASTFunction>();
        function->name = function_name;
        function->arguments = make_intrusive<ASTExpressionList>();
        function->children.push_back(function->arguments);
        function->arguments->children = std::move(arguments);
        return function;
    }

    /// Makes an expression calculating a hash of `arguments` represented as a value of type `type`,
    /// returns null if `type` is not one of the plain identifier types supported by the generator.
    ASTPtr makeHashExpressionForType(const IDataType & type, ASTs arguments)
    {
        /// For a LowCardinality type the hash is calculated for the dictionary type, and the result
        /// is dictionary-encoded right in the generator expression, so that the expression's type
        /// matches the column's type and identifiers stay dictionary-encoded on every path.
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(&type))
        {
            if (auto nested_expression = makeHashExpressionForType(*low_cardinality_type->getDictionaryType(), std::move(arguments)))
                return makeASTFunction("toLowCardinality", std::move(nested_expression));
            return nullptr;
        }

        WhichDataType which(type);

        if (which.isUInt64())
            return makeHashFunctionCall("sipHash64", std::move(arguments));

        if (which.isFixedString() && typeid_cast<const DataTypeFixedString &>(type).getN() == 16)
            return makeHashFunctionCall("sipHash128", std::move(arguments));

        if (which.isUUID())
            return makeASTFunction("reinterpretAsUUID", makeHashFunctionCall("sipHash128", std::move(arguments)));

        if (which.isUInt128())
            return makeASTFunction("reinterpretAsUInt128", makeHashFunctionCall("sipHash128", std::move(arguments)));

        return nullptr;
    }

}


ASTPtr TimeSeriesIDGenerator::getDefault(const DataTypePtr & id_type, const StorageID & table_id)
{
    /// The `tags` column contains all the tags, including the `__name__` tag with the metric name
    /// and the tags stored in the columns specified in the `tags_to_columns` setting,
    /// so hashing just `tags` is enough to identify a time series.
    ASTs arguments_for_hash_function{make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags)};

    /// An identifier of a plain type is a hash of the tags.
    if (auto expression = makeHashExpressionForType(*id_type, arguments_for_hash_function))
        return expression;

    /// For a two-component identifier Tuple(F, S) the first component is a hash of the metric name only,
    /// and the second component is a hash of the tags.
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(id_type.get()))
    {
        const auto & element_types = tuple_type->getElements();
        if (element_types.size() == 2)
        {
            ASTPtr first = makeHashExpressionForType(*element_types[0], ASTs{make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName)});
            ASTPtr second = first ? makeHashExpressionForType(*element_types[1], arguments_for_hash_function) : nullptr;
            if (first && second)
                return makeASTFunction("tuple", std::move(first), std::move(second));
        }
    }

    throw Exception(ErrorCodes::INCORRECT_QUERY,
        "{}: An expression generating identifiers must be specified explicitly for the {} column of type {} - "
        "either as a DEFAULT expression of that column or in the 'id_generator' setting. "
        "An expression can be chosen automatically only for types UInt64, UInt128, UUID, FixedString(16), "
        "the same types wrapped in LowCardinality, and tuples of two of those types",
        table_id.getNameForLogs(), TimeSeriesColumnNames::ID, id_type->getName());
}


bool TimeSeriesIDGenerator::usesAllTags(const ASTPtr & id_generator)
{
    if (!id_generator)
        return false;
    return hasIdentifier(*id_generator, TimeSeriesColumnNames::AllTags);
}

}
