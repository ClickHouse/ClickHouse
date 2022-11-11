#include <Interpreters/InterpreterCreateNamedCollectionQuery.h>

#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Common/escapeForFileName.h>
#include <Storages/NamedCollectionConfiguration.h>
#include <Storages/NamedCollections.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parsers/formatAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BlockIO InterpreterCreateNamedCollectionQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::CREATE_NAMED_COLLECTION);

    const auto & query = query_ptr->as<const ASTCreateNamedCollectionQuery &>();
    const std::string collection_name = query.collection_name;

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    const ASTSetQuery & collection_def = query.collection_def->as<const ASTSetQuery &>();
    const auto config = NamedCollectionConfiguration::createConfiguration(collection_name, collection_def.changes);

    NamedCollectionFactory::instance().add(collection_name, NamedCollection::create(*config, collection_name));

    String collection_name_escaped = escapeForFileName(collection_name);
    fs::path metadata_file_path = fs::canonical(getContext()->getPath())
        / "named_collections"
        / (collection_name_escaped + ".sql");

    if (fs::exists(metadata_file_path))
    {
        NamedCollectionFactory::instance().remove(collection_name);
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Metadata file {} already exists, but according named collection was not loaded");
    }

    try
    {
        WriteBufferFromOwnString statement_buf;
        formatAST(query, statement_buf, false);
        writeChar('\n', statement_buf);
        String statement = statement_buf.str();

        WriteBufferFromFile out(metadata_file_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);

        out.next();
        if (getContext()->getSettingsRef().fsync_metadata)
            out.sync();
        out.close();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        NamedCollectionFactory::instance().remove(collection_name);
    }

    return {};
}

}
