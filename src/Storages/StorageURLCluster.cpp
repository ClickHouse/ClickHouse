#include "Interpreters/Context_fwd.h"

#include <Storages/StorageURLCluster.h>

#include <Core/QueryProcessingStage.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

#include <Processors/Transforms/AddingDefaultsTransform.h>

#include <Processors/Sources/RemoteSource.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Storages/IStorage.h>
#include <Storages/StorageURL.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDictionary.h>
#include <Storages/addColumnsStructureToQueryWithClusterEngine.h>

#include <memory>


namespace DB
{

StorageURLCluster::StorageURLCluster(
    ContextPtr context_,
    const String & cluster_name_,
    const String & uri_,
    const String & format_,
    const String & compression_method_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const StorageURL::Configuration & configuration_,
    bool structure_argument_was_provided_)
    : IStorageCluster(cluster_name_, table_id_, &Poco::Logger::get("StorageURLCluster (" + table_id_.table_name + ")"), structure_argument_was_provided_)
    , uri(uri_)
{
    context_->getRemoteHostFilter().checkURL(Poco::URI(uri));

    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = StorageURL::getTableStructureFromData(format_,
            uri,
            chooseCompressionMethod(Poco::URI(uri).getPath(), compression_method_),
            configuration_.headers,
            std::nullopt,
            context_);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}

void StorageURLCluster::addColumnsStructureToQuery(ASTPtr & query, const String & structure)
{
    addColumnsStructureToQueryWithURLClusterEngine(query, structure);
}

RemoteQueryExecutor::Extension StorageURLCluster::getTaskIteratorExtension(ASTPtr, ContextPtr context) const
{
    auto iterator = std::make_shared<StorageURLSource::DisclosedGlobIterator>(uri, context->getSettingsRef().glob_expansion_max_elements);
    auto callback = std::make_shared<TaskIterator>([iter = std::move(iterator)]() mutable -> String { return iter->next(); });
    return RemoteQueryExecutor::Extension{.task_iterator = std::move(callback)};
}

NamesAndTypesList StorageURLCluster::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};
}

}
