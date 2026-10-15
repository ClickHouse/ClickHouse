#include <Interpreters/Access/InterpreterCheckGrantQuery.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{

BlockIO InterpreterCheckGrantQuery::execute()
{
    auto & query = query_ptr->as<ASTCheckGrantQuery &>();

    /// Collect access rights elements which will be checked.
    AccessRightsElements & elements_to_check_grant = query.access_rights_elements;
    String current_database = getContext()->getCurrentDatabase();
    elements_to_check_grant.replaceEmptyDatabase(current_database);

    auto current_user_access = getContext()->getAccess();
    bool is_granted = current_user_access->isGranted(elements_to_check_grant);

    BlockIO res;
    res.pipeline = QueryPipeline(
        std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{ColumnUInt8::create(1, is_granted), std::make_shared<DataTypeUInt8>(), "result"}})));

    return res;
}

void registerInterpreterCheckGrantQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCheckGrantQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCheckGrantQuery", create_fn);
}

}
