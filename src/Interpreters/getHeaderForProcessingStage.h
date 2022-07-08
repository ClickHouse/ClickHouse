#pragma once

#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class IStorage;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
struct SelectQueryInfo;
struct TreeRewriterResult;
class ASTSelectQuery;

bool hasJoin(const ASTSelectQuery & select);
bool removeJoin(ASTSelectQuery & select, TreeRewriterResult & rewriter_result, ContextPtr context);

Block getHeaderForProcessingStage(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage);

}
