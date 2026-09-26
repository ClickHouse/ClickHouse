#include <Databases/RenderedCreateQuery.h>

#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/StringUtils.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool print_pretty_type_names;
    extern const SettingsBool show_table_uuid_in_table_create_query_if_not_nil;
    extern const SettingsIdentifierQuotingRule show_create_query_identifier_quoting_rule;
    extern const SettingsIdentifierQuotingStyle show_create_query_identifier_quoting_style;
}

namespace
{

String formatWithOptions(const IAST & ast, const RenderOptions & options)
{
    return ast.formatWithPossiblyHidingSensitiveData(
        /*max_length=*/0,
        options.one_line,
        options.show_secrets,
        options.print_pretty_type_names,
        options.quoting_rule,
        options.quoting_style);
}

}

RenderOptions resolveRenderOptions(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    RenderOptions options;
    options.show_secrets = canDisplaySecrets(context);
    options.print_pretty_type_names = settings[Setting::print_pretty_type_names];
    options.quoting_rule = settings[Setting::show_create_query_identifier_quoting_rule];
    options.quoting_style = settings[Setting::show_create_query_identifier_quoting_style];
    options.show_uuid = settings[Setting::show_table_uuid_in_table_create_query_if_not_nil];
    options.masker = SensitiveDataMasker::getInstance();
    return options;
}

RenderedCreateQueryPtr renderCreateQuery(const ASTPtr & ast, const RenderOptions & options, RenderedCreateQueryFields fields)
{
    auto rendered = std::make_shared<RenderedCreateQuery>();
    if (!ast)
        return rendered;

    auto * ast_create = ast->as<ASTCreateQuery>();

    if (ast_create && !options.show_uuid)
    {
        ast_create->uuid = UUIDHelpers::Nil;
        if (ast_create->targets)
            ast_create->targets->resetInnerUUIDs();
    }

    if (fields.create_table_query)
        rendered->create_table_query = formatWithOptions(*ast, options);

    if (fields.engine_full && ast_create && ast_create->storage)
    {
        rendered->engine_full = formatWithOptions(*ast_create->storage, options);

        static const char * const extra_head = " ENGINE = ";
        if (startsWith(rendered->engine_full, extra_head))
            rendered->engine_full = rendered->engine_full.substr(strlen(extra_head));
    }

    if (fields.as_select && ast_create && ast_create->select)
        rendered->as_select = formatWithOptions(*ast_create->select, options);

    return rendered;
}

}
