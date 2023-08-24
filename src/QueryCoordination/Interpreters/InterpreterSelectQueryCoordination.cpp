//#include <Interpreters/InterpreterSelectWithUnionQuery.h>
//#include <QueryCoordination/Interpreters/InterpreterSelectQueryCoordination.h>
//#include <QueryCoordination/Interpreters/RewriteDistributedTableVisitor.h>
//#include <Processors/QueryPlan/QueryPlan.h>
//#include <Processors/QueryPlan/Optimizations/Optimizations.h>
//
//
//namespace DB
//{
//
//InterpreterSelectQueryCoordination::InterpreterSelectQueryCoordination(
//    const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_)
//    : query_ptr(query_ptr_), context(Context::createCopy(context_)), options(options_))
//{
//    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
//    {
//        RewriteDistributedTableVisitor visitor(context);
//        visitor.visit(query_ptr);
//
//        if (visitor.has_local_table && visitor.has_distributed_table)
//            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support distributed table and local table mix query");
//
//        String cluster_name;
//        if (visitor.has_distributed_table)
//        {
//            cluster_name = visitor.clusters[0]->getName();
//            for (const auto & cluster : visitor.clusters)
//            {
//                if (cluster_name != cluster->getName())
//                {
//                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support cross cluster query");
//                }
//            }
//
//            std::vector<StorageID> storages;
//            for (const auto & visitor_storage : visitor.storages)
//            {
//                storages.emplace_back(visitor_storage->getStorageID());
//            }
//
//            if (context->addQueryCoordinationMetaInfo(cluster_name, storages, visitor.sharding_key_columns))
//                context->setDistributed(true);
//            else
//                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support cross cluster query"); /// maybe union query
//        }
//
//        if (visitor.has_local_table)
//            context->setDistributed(false);
//    }
//    else
//    {
//        context->setDistributed(true);
//    }
//}
//
//BlockIO InterpreterSelectQueryCoordination::execute()
//{
//    BlockIO res;
//
//    QueryPlan query_plan;
//    InterpreterSelectWithUnionQuery(query_ptr, context, options).buildQueryPlan(query_plan);
//    query_plan.optimize(QueryPlanOptimizationSettings::fromContext(context));
//
//    if (context->isDistributed())
//    {
//        const auto & resources = query_plan.getResources();
//
//        PlanFragmentBuilder builder(storage_limits, context, query_plan);
//        const auto & res_fragments = builder.build();
//
//        /// query_plan resources move to fragments
//        /// Extend lifetime of context, table lock, storage.
//        /// TODO every fragment need context, table lock, storage ?
//        for (const auto & fragment : res_fragments)
//        {
//            for (const auto & context_ : resources.interpreter_context)
//            {
//                fragment->addInterpreterContext(context_);
//            }
//
//            for (const auto & storage_holder : resources.storage_holders)
//            {
//                fragment->addStorageHolder(storage_holder);
//            }
//
//            for (const auto & table_lock_ : resources.table_locks)
//            {
//                fragment->addTableLock(table_lock_);
//            }
//        }
//
//        WriteBufferFromOwnString buffer;
//        fragments.back()->dump(buffer);
//        LOG_INFO(&Poco::Logger::get("InterpreterSelectWithUnionQueryFragments"), "Fragment dump: {}", buffer.str());
//
//        /// save fragments wait for be scheduled
//        res.query_coord_state.fragments = fragments;
//
//        /// schedule fragments
//        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
//        {
//            Coordinator coord(fragments, context, formattedAST(query_ptr));
//            coord.schedulePrepareDistributedPipelines();
//
//            /// local already be scheduled
//            res.query_coord_state.pipelines = std::move(coord.pipelines);
//            res.query_coord_state.remote_host_connection = coord.getRemoteHostConnection();
//            res.pipeline = res.query_coord_state.pipelines.detachRootPipeline();
//
//            /// TODO quota only use to root pipeline?
//            setQuota(res.pipeline);
//        }
//    }
//    else
//    {
//        auto builder = query_plan.buildQueryPipeline(
//                QueryPlanOptimizationSettings::fromContext(context),
//                BuildQueryPipelineSettings::fromContext(context));
//
//        res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
//        setQuota(res.pipeline);
//    }
//
//    return res;
//}
//
//}
