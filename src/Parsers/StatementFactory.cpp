#include <Parsers/StatementFactory.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StatementFactory & StatementFactory::instance()
{
    static StatementFactory factory;
    return factory;
}

void StatementFactory::registerStatement(const String & name, Documentation documentation)
{
    if (!statements.emplace(name, std::move(documentation)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The statement {} is registered twice", name);
}

std::vector<String> StatementFactory::getAllRegisteredNames() const // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::vector<String> result; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    result.reserve(statements.size());
    for (const auto & [name, _] : statements)
        result.push_back(name);
    return result;
}

Documentation StatementFactory::getDocumentation(const String & name) const
{
    if (auto it = statements.find(name); it != statements.end())
        return it->second;
    return {};
}

StatementRegisterMap & StatementRegisterMap::instance()
{
    static StatementRegisterMap map;
    return map;
}

void registerStatements()
{
    auto & factory = StatementFactory::instance();

    for (const auto & [_, reg] : StatementRegisterMap::instance())
        reg(factory);
}

}
