#include "IndexAdvisor.h"
#include <Common/logger_useful.h>

namespace DB
{

void IndexAdvisor::analyzeQuery(const ASTPtr & query_ast, ContextMutablePtr context, size_t max_index_count)
{
    if (!query_ast)
        return;

    LOG_INFO(&Poco::Logger::get("IndexAdvisor"), "Starting index analysis for query");
    selected_candidates = selector->selectIndexes(query_ast, context, max_index_count);
    LOG_DEBUG(&Poco::Logger::get("IndexAdvisor"), "Found {} potential indexes", selected_candidates.size());
}

} // namespace DB 
