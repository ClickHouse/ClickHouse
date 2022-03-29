#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Access/Common/AccessRightsElement.h>

namespace DB
{


BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    if (!ast.cluster.empty())
    {
        /// set on cluster need permission SET GLOBAL
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    if (ast.getKind() == ASTSetQuery::Global)
    {
        /// set global need permission SET GLOBAL
        getContext()->checkAccess(getRequiredAccess());
        getContext()->getGlobalContext()->global_set = true;
        getContext()->getGlobalContext()->setSettingsChanges(ast.changes);
    }
    if (ast.getKind() == ASTSetQuery::Session)
    {
        /* global level settings change no need check constraints
         * for instance, set allow_ddl = 0; set global can set allow_ddl =1,
         * but set session can not change this allow_ddl from 0 to 1;
         */
        getContext()->checkSettingsConstraints(ast.changes);
        auto session_context = getContext()->getSessionContext();
        session_context->applySettingsChanges(ast.changes);
        session_context->addQueryParameters(ast.query_parameters);
    }

    return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes);
    getContext()->applySettingsChanges(ast.changes);
}


AccessRightsElements InterpreterSetQuery::getRequiredAccess() const
{
    AccessRightsElements required_access;

    const auto & ast = query_ptr->as<ASTSetQuery &>();

    if (ast.getKind() == ASTSetQuery::Global)
    {
        required_access.emplace_back(AccessType::SET_GLOBAL);
    }

    return required_access;
}

}
