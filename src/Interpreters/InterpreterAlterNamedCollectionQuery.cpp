#include <Interpreters/InterpreterAlterNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/FieldVisitorToString.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Storages/NamedCollections.h>
#include <Common/escapeForFileName.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parsers/formatAST.h>
#include <Databases/DatabaseOnDisk.h>


namespace DB
{

BlockIO InterpreterAlterNamedCollectionQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::ALTER_NAMED_COLLECTION);

    const auto & query = query_ptr->as<const ASTAlterNamedCollectionQuery &>();
    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    const auto & collection_name = query.collection_name;
    const auto & alter_changes = query.changes->as<ASTSetQuery>()->changes;

    auto collection = NamedCollectionFactory::instance().getMutable(collection_name);
    auto lock = collection->lock();

    String collection_name_escaped = escapeForFileName(collection_name);
    fs::path metadata_file_path = fs::canonical(getContext()->getPath())
        / "named_collections"
        / (collection_name_escaped + ".sql");
    fs::path metadata_file_tmp_path = metadata_file_path.string() + ".tmp";

    auto parsed_query = DatabaseOnDisk::parseQueryFromMetadata(
        nullptr, getContext(), metadata_file_path, true);

    auto & create_named_collection_query = parsed_query->as<ASTCreateNamedCollectionQuery &>();
    auto & create_changes = create_named_collection_query.collection_def->as<ASTSetQuery>()->changes;

    std::unordered_map<std::string_view, const Field *> new_collection_def;
    for (const auto & [name, value] : create_changes)
        new_collection_def.emplace(name, &value);

    for (const auto & [name, value] : alter_changes)
    {
        auto it = new_collection_def.find(name);
        if (it == new_collection_def.end())
            new_collection_def.emplace(name, &value);
        else
            it->second = &value;
    }

    WriteBufferFromOwnString statement_buf;
    formatAST(query, statement_buf, false);
    writeChar('\n', statement_buf);
    String statement = statement_buf.str();

    WriteBufferFromFile out(metadata_file_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
    writeString(statement, out);

    fs::rename(metadata_file_tmp_path, metadata_file_path);

    for (const auto & [name, value] : alter_changes)
        collection->setOrUpdate<String>(name, convertFieldToString(value));

    return {};
}

}
