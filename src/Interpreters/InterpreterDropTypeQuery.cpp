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
    auto current_context = getContext(); // Получаем текущий контекст

    current_context->checkAccess(AccessType::DROP_TYPE);
    LOG_DEBUG(log, "Checked access for DROP TYPE");

    const String & type_name = drop_query.type_name;
    LOG_DEBUG(log, "Attempting to drop type: {}", type_name);

    bool type_existed_in_memory = UserDefinedTypeFactory::instance().isTypeRegistered(type_name, current_context);

    if (!type_existed_in_memory)
    {
        if (drop_query.if_exists)
        {
            LOG_DEBUG(log, "Type '{}' does not exist in memory, and IF EXISTS specified. Verifying in DB.", type_name);
            // Проверим, есть ли он в БД, чтобы не выдавать ошибку, если его там тоже нет
            // Хотя DELETE FROM ... WHERE name = ... IF EXISTS не поддерживается,
            // поэтому просто попробуем удалить и проигнорируем ошибку "не найден", если if_exists.
            // Но лучше сначала проверить, есть ли что удалять, чтобы избежать лишних логов об ошибках.
        }
        else
        {
            LOG_WARNING(log, "Type '{}' does not exist in memory.", type_name);
            // Если его нет в памяти и нет IF EXISTS, стоит проверить БД перед тем, как кидать UNKNOWN_TYPE.
            // Но пока оставим так для простоты, предполагая, что память и БД должны быть консистентны.
            // В идеале, loadTypesFromSystemTable должен был бы его загрузить, если он есть в БД.
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "User-defined type '{}' does not exist.", type_name);
        }
    }

    // Сначала удаляем из БД
    try
    {
        String delete_query_str = fmt::format("DELETE FROM system.user_defined_types WHERE name = {}", quoteString(type_name));
        ContextMutablePtr delete_query_context = Context::createCopy(current_context);
        
        LOG_DEBUG(log, "Attempting to delete type '{}' from system.user_defined_types. Query: {}", type_name, delete_query_str);
        
        // Выполняем запрос. Если он ничего не удалит (т.к. типа нет), ошибки не будет.
        // Ошибка будет, если таблица system.user_defined_types не существует, или другие проблемы с БД.
        executeQuery(delete_query_str, delete_query_context, QueryFlags{ .internal = true }, QueryProcessingStage::Complete);
        LOG_INFO(log, "Successfully executed DELETE query for type '{}' from system.user_defined_types (or type was not found there).", type_name);
    }
    catch (const Exception & e)
    {
        // Если тип должен был существовать (не if_exists), и удаление из БД не удалось, это проблема.
        if (!drop_query.if_exists && type_existed_in_memory)
        {
            LOG_ERROR(log, "Failed to delete type '{}' from system.user_defined_types. Error: {}. Type might remain in memory if subsequent removeType fails.", type_name, e.what());
            throw Exception(ErrorCodes::CANNOT_REMOVE_TYPE, "Failed to remove user-defined type '{}' from persistent storage: {}", type_name, e.what());
        }
        else
        {
            // Если if_exists, или его не было в памяти, то ошибка удаления из БД (например, таблицы нет) не критична, если она не связана с тем, что тип там был.
            LOG_WARNING(log, "Failed to delete type '{}' from system.user_defined_types (or it wasn't there). Error: {}. Proceeding with in-memory removal if applicable.", type_name, e.what());
        }
    }

    // Затем удаляем из памяти, если он там был
    if (type_existed_in_memory)
    {
        try
        {
            UserDefinedTypeFactory::instance().removeType(type_name);
            LOG_INFO(log, "Successfully removed type '{}' from in-memory factory.", type_name);
        }
        catch (const Exception & e)
        {
            // Эта ситуация не должна происходить, если type_existed_in_memory == true и isTypeRegistered работала корректно.
            // Но на всякий случай.
            LOG_ERROR(log, "Failed to remove type '{}' from in-memory factory, though it was reported as existing. Error: {}", type_name, e.what());
            if (!drop_query.if_exists) // Если не IF EXISTS, то это неожиданная ошибка
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to remove user-defined type '{}' from memory, though it was registered: {}", type_name, e.what());
            }
        }
    }
    else if (!drop_query.if_exists)
    {
        // Если его не было в памяти, и не было флага IF EXISTS,
        // и мы дошли до сюда (т.е. не вышли по UNKNOWN_TYPE раньше),
        // значит, мы должны были бы кинуть ошибку ранее. Это для полноты картины, но эта ветка маловероятна.
        LOG_WARNING(log, "Type '{}' was not found in memory, and IF EXISTS was not specified. This state is unexpected if initial check passed.", type_name);
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "User-defined type '{}' does not exist (final check).", type_name);
    }

    LOG_INFO(log, "Drop type operation for '{}' completed.", type_name);
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
