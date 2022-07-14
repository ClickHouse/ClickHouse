#include <Analyzer/QueryTreePassManager.h>

#include <Analyzer/QueryAnalysisPass.h>
#include <Analyzer/MultiIfToIfPass.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

QueryTreePassManager::QueryTreePassManager(ContextPtr context_) : WithContext(context_) {}

void QueryTreePassManager::addPass(QueryTreePassPtr pass)
{
    passes.push_back(std::move(pass));
}

void QueryTreePassManager::run(QueryTreeNodePtr query_tree_node)
{
    auto current_context = getContext();
    size_t optimizations_size = passes.size();

    for (size_t i = 0; i < optimizations_size; ++i)
        passes[i]->run(query_tree_node, current_context);
}

void QueryTreePassManager::run(QueryTreeNodePtr query_tree_node, size_t up_to_pass_index)
{
    size_t optimizations_size = passes.size();
    if (up_to_pass_index > optimizations_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Requested to run optimizations up to {} pass. There are only {} pass",
            up_to_pass_index,
            optimizations_size);

    auto current_context = getContext();
    for (size_t i = 0; i < up_to_pass_index; ++i)
        passes[i]->run(query_tree_node, current_context);
}

void QueryTreePassManager::dump(WriteBuffer & buffer)
{
    size_t passes_size = passes.size();

    for (size_t i = 0; i < passes_size; ++i)
    {
        auto & pass = passes[i];
        buffer << "Pass " << (i + 1) << ' ' << pass->getName() << " - " << pass->getDescription();
        if (i < passes_size)
            buffer << '\n';
    }
}

void QueryTreePassManager::dump(WriteBuffer & buffer, size_t up_to_pass_index)
{
    size_t optimizations_size = passes.size();
    if (up_to_pass_index > optimizations_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Requested to dump optimizations up to {} pass. There are only {} pass",
            up_to_pass_index,
            optimizations_size);

    for (size_t i = 0; i < up_to_pass_index; ++i)
    {
        auto & pass = passes[i];
        buffer << "Pass " << (i + 1) << " " << pass->getName() << " - " << pass->getDescription();
        if (i < up_to_pass_index)
            buffer << '\n';
    }
}

void addQueryTreePasses(QueryTreePassManager & manager)
{
    auto context = manager.getContext();
    const auto & settings = context->getSettingsRef();

    manager.addPass(std::make_shared<QueryAnalysisPass>());

    if (settings.optimize_multiif_to_if)
        manager.addPass(std::make_shared<MultiIfToIfPass>());
}

}
