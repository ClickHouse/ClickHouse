/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <Core/Settings.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTWatchQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessFlags.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Storages/IStorage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TOO_MANY_COLUMNS;
    extern const int SUPPORT_IS_DISABLED;
}


BlockIO InterpreterWatchQuery::execute()
{
    BlockIO res;
    res.pipeline = QueryPipelineBuilder::getPipeline(buildQueryPipeline());
    return res;
}

QueryPipelineBuilder InterpreterWatchQuery::buildQueryPipeline()
{
    const ASTWatchQuery & query = typeid_cast<const ASTWatchQuery &>(*query_ptr);
    auto table_id = getContext()->resolveStorageID(query, Context::ResolveOrdinary);

    /// Get storage
    storage = DatabaseCatalog::instance().tryGetTable(table_id, getContext());

    if (!storage)
        throw Exception("Table " + table_id.getNameForLogs() + " doesn't exist.",
        ErrorCodes::UNKNOWN_TABLE);

    auto storage_name = storage->getName();
    if (storage_name == "LiveView"
        && !getContext()->getSettingsRef().allow_experimental_live_view)
        throw Exception("Experimental LIVE VIEW feature is not enabled (the setting 'allow_experimental_live_view')",
                        ErrorCodes::SUPPORT_IS_DISABLED);
    else if (storage_name == "WindowView"
        && !getContext()->getSettingsRef().allow_experimental_window_view)
        throw Exception("Experimental WINDOW VIEW feature is not enabled (the setting 'allow_experimental_window_view')",
                        ErrorCodes::SUPPORT_IS_DISABLED);

    /// List of columns to read to execute the query.
    Names required_columns = storage->getInMemoryMetadataPtr()->getColumns().getNamesOfPhysical();
    getContext()->checkAccess(AccessType::SELECT, table_id, required_columns);

    /// Get context settings for this query
    const Settings & settings = getContext()->getSettingsRef();

    /// Limitation on the number of columns to read.
    if (settings.max_columns_to_read && required_columns.size() > settings.max_columns_to_read)
        throw Exception("Limit for number of columns to read exceeded. "
            "Requested: " + std::to_string(required_columns.size())
            + ", maximum: " + settings.max_columns_to_read.toString(),
            ErrorCodes::TOO_MANY_COLUMNS);

    size_t max_block_size = settings.max_block_size;
    size_t max_streams = 1;

    /// Define query info
    SelectQueryInfo query_info;
    query_info.query = query_ptr;

    /// From stage
    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

    /// Watch storage
    auto pipe = storage->watch(required_columns, query_info, getContext(), from_stage, max_block_size, max_streams);

    /// Constraints on the result, the quota on the result, and also callback for progress.
    {
        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
        limits.size_limits.max_rows = settings.max_result_rows;
        limits.size_limits.max_bytes = settings.max_result_bytes;
        limits.size_limits.overflow_mode = settings.result_overflow_mode;

        pipe.setLimits(limits);
        pipe.setQuota(getContext()->getQuota());
    }

    QueryPipelineBuilder pipeline;
    pipeline.init(std::move(pipe));
    return pipeline;
}

}
