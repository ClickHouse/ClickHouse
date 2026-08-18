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
