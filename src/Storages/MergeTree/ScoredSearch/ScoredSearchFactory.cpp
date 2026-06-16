#include <Storages/MergeTree/ScoredSearch/ScoredSearchFactory.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IndicesDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FUNCTION;
}

namespace
{

template <typename Type>
Type castAs(const Field & field, std::string_view argument_name)
{
    auto expected_type = Field::TypeToEnum<Type>::value;

    if (expected_type != field.getType())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Scoring function argument '{}' expected to be of type {}, but got type: {}",
            argument_name, fieldTypeToString(expected_type), field.getTypeName());
    }

    return field.safeGet<Type>();
}

void assertParamsCount(size_t params_count, size_t max_count, std::string_view scoring)
{
    if (params_count > max_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "'{}' scoring function accepts at most {} parameters, but got {}",
            scoring, max_count, params_count);
    }
}

}

std::unordered_map<String, ScoredSearchFactory::Family> ScoredSearchFactory::getAllScorings() const
{
    std::unordered_map<String, Family> result;
    for (const auto & scoring : scorings)
        result[scoring.first] = scoring.second.family;
    return result;
}

static void registerScorings(ScoredSearchFactory & factory);

ScoredSearchFactory & ScoredSearchFactory::instance()
{
    static ScoredSearchFactory factory;
    return factory;
}

ScoredSearchFactory::ScoredSearchFactory()
{
    registerScorings(*this);
}

void ScoredSearchFactory::registerScoring(const String & name, Family family, Creator creator)
{
    if (!scorings.emplace(name, Entry{family, creator}).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ScoredSearchFactory: scoring function '{}' is already registered", name);
}

ScoredSearchDescriptor ScoredSearchFactory::get(const ASTPtr & ast) const
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        return get(identifier->name(), FieldVector{});
    }

    if (const auto * function = ast->as<ASTFunction>())
    {
        FieldVector args = getFieldsFromIndexArgumentsAST(function->arguments);
        return get(function->name, args);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create scoring function from AST: '{}'", ast->formatForErrorMessage());
}

ScoredSearchDescriptor ScoredSearchFactory::get(std::string_view name, const FieldVector & args) const
{
    auto it = scorings.find(String(name));
    if (it == scorings.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown scoring function: '{}'", name);

    return it->second.creator(args);
}

static void registerScorings(ScoredSearchFactory & factory)
{
    using Family = ScoredSearchDescriptor::Family;

    auto bm25_creator = [](const FieldVector & args) -> ScoredSearchDescriptor
    {
        assertParamsCount(args.size(), 2, "bm25");

        Float64 k1 = args.empty() ? 1.2  : castAs<Float64>(args[0], "k1");
        Float64 b  = args.size() < 2 ? 0.75 : castAs<Float64>(args[1], "b");

        return ScoredSearchDescriptor
        {
            .name   = "bm25",
            .params = {k1, b},
            .family = Family::Text,
        };
    };

    factory.registerScoring("bm25", Family::Text, bm25_creator);
}

}
