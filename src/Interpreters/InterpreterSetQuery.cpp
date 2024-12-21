#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/MemorySettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsDefaultTableEngine default_table_engine;
}

namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}

BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes, SettingSource::QUERY);
    auto session_context = getContext()->getSessionContext();
    session_context->applySettingsChanges(ast.changes);
    session_context->addQueryParameters(NameToNameMap{ast.query_parameters.begin(), ast.query_parameters.end()});
    session_context->resetSettingsToDefaultValue(ast.default_settings);
    return {};
}


void InterpreterSetQuery::executeForCurrentContext(bool ignore_setting_constraints)
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    if (!ignore_setting_constraints)
        getContext()->checkSettingsConstraints(ast.changes, SettingSource::QUERY);
    getContext()->applySettingsChanges(ast.changes);
    getContext()->resetSettingsToDefaultValue(ast.default_settings);
}

static void applySettingsFromSelectWithUnion(const ASTSelectWithUnionQuery & select_with_union, ContextMutablePtr context)
{
    const ASTs & children = select_with_union.list_of_selects->children;
    if (children.empty())
        return;

    // We might have an arbitrarily complex UNION tree, so just give
    // up if the last first-order child is not a plain SELECT.
    // It is flattened later, when we process UNION ALL/DISTINCT.
    const auto * last_select = children.back()->as<ASTSelectQuery>();
    if (last_select && last_select->settings())
        InterpreterSetQuery(last_select->settings(), context).executeForCurrentContext(/* ignore_setting_constraints= */ false);
}

namespace
{
std::optional<String> getTableStorageName(const ASTCreateQuery & create, ContextMutablePtr context)
{
    if ((create.database && !create.table) || create.isView())
        return {};
    if (create.storage->engine)
        return create.storage->engine->name;
    switch (context->getSettingsRef()[Setting::default_table_engine])
    {
        case DefaultTableEngine::None:
            return {};
        case DefaultTableEngine::Log:
        case DefaultTableEngine::StripeLog:
            return "Log";
        case DefaultTableEngine::MergeTree:
        case DefaultTableEngine::ReplacingMergeTree:
        case DefaultTableEngine::ReplicatedMergeTree:
        case DefaultTableEngine::ReplicatedReplacingMergeTree:
        case DefaultTableEngine::SharedMergeTree:
        case DefaultTableEngine::SharedReplacingMergeTree:
            return "MergeTree";
        case DefaultTableEngine::Memory:
            return "Memory";
    }
}
}


void InterpreterSetQuery::applySettingsFromQuery(const ASTPtr & ast, ContextMutablePtr context_)
{
    if (!ast)
        return;

    /// First apply the outermost settings. Then they could be overridden by deeper settings.
    if (const auto * query_with_output = dynamic_cast<ASTQueryWithOutput *>(ast.get()))
    {
        if (query_with_output->settings_ast)
            InterpreterSetQuery(query_with_output->settings_ast, context_).executeForCurrentContext(/* ignore_setting_constraints= */ false);

        if (const auto * create_query = ast->as<ASTCreateQuery>(); create_query)
        {
            if (create_query->select)
                applySettingsFromSelectWithUnion(create_query->select->as<ASTSelectWithUnionQuery &>(), context_);
            else if (
                !create_query->settings_ast && create_query->storage && create_query->storage->settings
                && context_->getApplicationType() != Context::ApplicationType::CLIENT)
            {
                /// If we parsed one set of settings we don't know if it was the engine settings or the query settings
                /// We also want to allow users to mix them (so they don't need to declare SETTINGS engine_setting=0 SETTINGS query_setting=0
                /// So what we are going to do now is to check if each setting belongs to the engine or not, and if it doesn't
                /// then move it to the context

                ASTSetQuery * engine_settings = create_query->storage->settings;
                auto storage_name = getTableStorageName(*create_query, context_);
                if (storage_name->contains("Memory"))
                {
                    /// TODO: Support MemorySettings.has()
                    // MemorySettings memory_settings;
                    // for (auto it = engine_settings->changes.begin(); it != engine_settings->changes.end();)
                    // {
                    //     String & name = it->name;
                    //     if (!memory_settings.has(name) && context_->getSettingsRef().has(name))
                    //     {
                    //         context_->setSetting(name, it->value);
                    //         it = engine_settings->changes.erase(it);
                    //     }
                    //     else
                    //     {
                    //         it++;
                    //     }
                    // }
                }
                else if (storage_name->contains(("MergeTree")))
                {
                    MergeTreeSettings mergetree_settings;
                    for (auto it = engine_settings->changes.begin(); it != engine_settings->changes.end();)
                    {
                        String & name = it->name;
                        if (!mergetree_settings.has(name) && context_->getSettingsRef().has(name))
                        {
                            context_->setSetting(name, it->value);
                            it = engine_settings->changes.erase(it);
                        }
                        else
                        {
                            it++;
                        }
                    }
                }
                else if (storage_name->contains("..."))
                {
                    /// TODO: Pending for other storages. Maybe uniformize it somehow with a template or similar
                }
            }
        }
    }

    if (const auto * select_query = ast->as<ASTSelectQuery>())
    {
        if (auto new_settings = select_query->settings())
            InterpreterSetQuery(new_settings, context_).executeForCurrentContext(/* ignore_setting_constraints= */ false);
    }
    else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        applySettingsFromSelectWithUnion(*select_with_union_query, context_);
    }
    else if (const auto * explain_query = ast->as<ASTExplainQuery>())
    {
        if (explain_query->settings_ast)
            InterpreterSetQuery(explain_query->settings_ast, context_).executeForCurrentContext(/* ignore_setting_constraints= */ false);

        applySettingsFromQuery(explain_query->getExplainedQuery(), context_);
    }
    else if (auto * insert_query = ast->as<ASTInsertQuery>())
    {
        context_->setInsertFormat(insert_query->format);
        if (insert_query->settings_ast)
            InterpreterSetQuery(insert_query->settings_ast, context_).executeForCurrentContext(/* ignore_setting_constraints= */ false);
    }
}

void registerInterpreterSetQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSetQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterSetQuery", create_fn);
}
}
