#pragma once

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class IStorage;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct SelectQueryInfo;
struct TreeRewriterResult;
class ASTSelectQuery;

bool hasJoin(const ASTSelectQuery & select);
bool removeJoin(ASTSelectQuery & select, TreeRewriterResult & rewriter_result, ContextPtr context);

Block getHeaderForProcessingStage(
        const IStorage & storage,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage);

}
