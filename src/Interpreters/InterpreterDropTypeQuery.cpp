#include <Interpreters/InterpreterDropTypeQuery.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Access/Common/AccessType.h>     // Для проверки прав
#include <Access/ContextAccess.h>       // Для getContext()->checkAccess
#include <DataTypes/UserDefinedTypeFactory.h> // Для удаления типа
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Interpreters/InterpreterFactory.h> // Для регистрации
#include <Interpreters/Context.h> // Для getContext()
#include <Interpreters/executeQuery.h> // Для выполнения DELETE
#include <Interpreters/QueryFlags.h>   // Для QueryFlags
#include <Core/QueryProcessingStage.h> // Для QueryProcessingStage
#include <Common/quoteString.h>       // Для quoteString

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int CANNOT_REMOVE_TYPE; // Потенциально новая ошибка, если удаление из БД не удалось
    // ACCESS_DENIED уже должен быть
}

BlockIO InterpreterDropTypeQuery::execute()
{
    const auto & drop_query = query_ptr->as<const ASTDropTypeQuery &>();
    auto * log = &Poco::Logger::get("InterpreterDropTypeQuery");
    auto current_context = getContext();

    current_context->checkAccess(AccessType::DROP_TYPE);

    auto & udt_factory = UserDefinedTypeFactory::instance();

    const String & type_name = drop_query.type_name;

    try
    {
        udt_factory.removeType(current_context, type_name, drop_query.if_exists);
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(log, "Failed to drop type '{}'. Error: {}. Code: {}.", type_name, e.what(), e.code());
        throw;
    }

    return {};
}

// Оставим функцию регистрации здесь же для простоты, либо можно вынести как в InterpreterCreateTypeQuery
void registerInterpreterDropTypeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropTypeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropTypeQuery", create_fn);
}

}

