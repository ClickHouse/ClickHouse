#pragma once
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>


namespace DB
{

class IStorage;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct SelectQueryInfo;
class Context;
class ASTSelectQuery;

bool removeJoin(ASTSelectQuery & select);

Block getHeaderForProcessingStage(
        const IStorage & storage,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage);

}
