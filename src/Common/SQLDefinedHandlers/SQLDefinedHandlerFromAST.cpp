#include <Common/SQLDefinedHandlers/SQLDefinedHandlerFromAST.h>

#include <Parsers/ASTCreateHandlerQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_COMPILE_REGEXP;
}

namespace
{

SQLDefinedHandler::URLMatchType convertURLMatchType(ASTCreateHandlerQuery::URLMatchType type)
{
    switch (type)
    {
        case ASTCreateHandlerQuery::URLMatchType::Exact: return SQLDefinedHandler::URLMatchType::Exact;
        case ASTCreateHandlerQuery::URLMatchType::Prefix: return SQLDefinedHandler::URLMatchType::Prefix;
        case ASTCreateHandlerQuery::URLMatchType::Regexp: return SQLDefinedHandler::URLMatchType::Regexp;
    }
}

}

SQLDefinedHandlerPtr makeSQLDefinedHandler(const ASTCreateHandlerQuery & create)
{
    if (!create.has_url)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` must specify a URL", create.handler_name);

    if (!create.query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` must specify a query (the AS clause)", create.handler_name);

    const String type = create.handler_type.value_or("query");
    if (type != "query")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported handler type `{}`, only `query` is supported", type);

    auto handler = std::make_shared<SQLDefinedHandler>();
    handler->name = create.handler_name;
    handler->protocol = create.protocol;
    handler->url_match_type = convertURLMatchType(create.url_match_type);
    handler->url = create.url;
    handler->type = type;

    if (handler->url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` URL cannot be empty", create.handler_name);

    if (handler->url_match_type == SQLDefinedHandler::URLMatchType::Regexp)
    {
        auto regex = std::make_shared<const re2::RE2>(handler->url);
        if (!regex->ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile re2 regexp for handler `{}`: {}, error: {}",
                create.handler_name, handler->url, regex->error());
        handler->url_regex = regex;
    }

    if (create.methods && !create.methods->empty())
        handler->methods = *create.methods;
    else
        handler->methods = {"GET"};

    handler->query = create.query->formatWithSecretsOneLine();

    /// Build a normalized, complete CREATE HANDLER statement for persistence and introspection.
    auto normalized = create.clone();
    auto & normalized_create = normalized->as<ASTCreateHandlerQuery &>();
    normalized_create.is_alter = false;
    normalized_create.if_not_exists = false;
    normalized_create.cluster.clear();
    normalized_create.methods = handler->methods;
    normalized_create.handler_type = type;
    handler->create_statement = normalized_create.formatWithSecretsOneLine();

    return handler;
}

void mergeAlterIntoCreateHandler(ASTCreateHandlerQuery & create, const ASTCreateHandlerQuery & alter)
{
    if (alter.protocol)
        create.protocol = alter.protocol;

    if (alter.has_url)
    {
        create.has_url = true;
        create.url_match_type = alter.url_match_type;
        create.url = alter.url;
    }

    if (alter.methods)
        create.methods = alter.methods;

    if (alter.handler_type)
        create.handler_type = alter.handler_type;

    if (alter.query)
    {
        /// Replace the query child. The query is the only child of ASTCreateHandlerQuery.
        create.children.clear();
        create.query = alter.query->clone();
        create.children.push_back(create.query);
    }
}

}
