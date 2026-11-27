#include <Interpreters/InterpreterCreateTypeQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateTypeQuery.h>

namespace DB
{

BlockIO InterpreterCreateTypeQuery::execute()
{
    const auto & create = query_ptr->as<const ASTCreateTypeQuery &>();

    auto current_context = getContext();
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_TYPE);
    /// Replacing a type drops its previous definition.
    if (create.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_TYPE);
    current_context->checkAccess(access_rights_elements);

    bool throw_if_exists = !create.if_not_exists && !create.or_replace;
    bool replace_if_exists = create.or_replace;

    UserDefinedTypeFactory::instance().registerType(current_context, create.name, query_ptr, throw_if_exists, replace_if_exists);

    return {};
}

void registerInterpreterCreateTypeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateTypeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateTypeQuery", create_fn);
}

}
