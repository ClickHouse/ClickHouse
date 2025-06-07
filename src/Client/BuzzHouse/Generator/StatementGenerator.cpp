#include <Client/BuzzHouse/Generator/RandomSettings.h>
#include <Client/BuzzHouse/Generator/SQLCatalog.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>


namespace BuzzHouse
{

const std::unordered_map<OutFormat, InFormat> StatementGenerator::outIn
    = {{OutFormat::OUT_Arrow, InFormat::IN_Arrow},
       {OutFormat::OUT_Avro, InFormat::IN_Avro},
       {OutFormat::OUT_BSONEachRow, InFormat::IN_BSONEachRow},
       {OutFormat::OUT_CSV, InFormat::IN_CSV},
       {OutFormat::OUT_CSVWithNames, InFormat::IN_CSVWithNames},
       {OutFormat::OUT_CSVWithNamesAndTypes, InFormat::IN_CSVWithNamesAndTypes},
       {OutFormat::OUT_JSONColumns, InFormat::IN_JSONColumns},
       {OutFormat::OUT_JSONEachRow, InFormat::IN_JSONEachRow},
       {OutFormat::OUT_JSONObjectEachRow, InFormat::IN_JSONObjectEachRow},
       {OutFormat::OUT_JSONStringsEachRow, InFormat::IN_JSONStringsEachRow},
       {OutFormat::OUT_MsgPack, InFormat::IN_MsgPack},
       {OutFormat::OUT_ORC, InFormat::IN_ORC},
       {OutFormat::OUT_Parquet, InFormat::IN_Parquet},
       {OutFormat::OUT_Protobuf, InFormat::IN_Protobuf},
       {OutFormat::OUT_ProtobufSingle, InFormat::IN_ProtobufSingle},
       {OutFormat::OUT_RowBinary, InFormat::IN_RowBinary},
       {OutFormat::OUT_RowBinaryWithNames, InFormat::IN_RowBinaryWithNames},
       {OutFormat::OUT_RowBinaryWithNamesAndTypes, InFormat::IN_RowBinaryWithNamesAndTypes},
       {OutFormat::OUT_TSKV, InFormat::IN_TSKV},
       {OutFormat::OUT_Values, InFormat::IN_Values}};

StatementGenerator::StatementGenerator(FuzzConfig & fuzzc, ExternalIntegrations & conn, const bool scf, const bool rs)
    : fc(fuzzc)
    , connections(conn)
    , supports_cloud_features(scf)
    , replica_setup(rs)
    , deterministic_funcs_limit(
          static_cast<size_t>(
              std::find_if(CHFuncs.begin(), CHFuncs.end(), StatementGenerator::funcNotDeterministicIndexLambda) - CHFuncs.begin()))
    , deterministic_aggrs_limit(
          static_cast<size_t>(
              std::find_if(CHAggrs.begin(), CHAggrs.end(), StatementGenerator::aggrNotDeterministicIndexLambda) - CHAggrs.begin()))
{
    chassert(enum8_ids.size() > enum_values.size() && enum16_ids.size() > enum_values.size());

    for (size_t i = 0; i < deterministic_funcs_limit; i++)
    {
        /// Add single argument functions for non sargable predicates
        const CHFunction & next = CHFuncs[i];

        if (next.min_lambda_param == 0 && next.min_args == 1)
        {
            one_arg_funcs.push_back(next);
        }
    }
    /* Deterministic engines */
    likeEngs
        = {MergeTree,
           ReplacingMergeTree,
           CoalescingMergeTree,
           SummingMergeTree,
           AggregatingMergeTree,
           File,
           Null,
           Set,
           Join,
           StripeLog,
           Log,
           TinyLog,
           EmbeddedRocksDB};
    if (fc.allow_memory_tables)
    {
        likeEngs.emplace_back(Memory);
    }
    if (!fc.keeper_map_path_prefix.empty())
    {
        likeEngs.emplace_back(KeeperMap);
    }
    /* Not deterministic engines */
    likeEngs.emplace_back(Merge);
    if (fc.allow_infinite_tables)
    {
        likeEngs.emplace_back(GenerateRandom);
    }
}

void StatementGenerator::generateStorage(RandomGenerator & rg, Storage * store) const
{
    std::uniform_int_distribution<uint32_t> storage_range(1, static_cast<uint32_t>(Storage::DataStorage_MAX));

    store->set_storage(static_cast<Storage_DataStorage>(storage_range(rg.generator)));
    store->set_storage_name(rg.pickRandomly(fc.disks));
}

void StatementGenerator::setRandomSetting(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SetValue * set)
{
    const String & setting = rg.pickRandomly(settings);

    set->set_property(setting);
    set->set_value(settings.at(setting).random_func(rg));
}

void StatementGenerator::generateSettingValues(
    RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, const size_t nvalues, SettingValues * vals)
{
    for (size_t i = 0; i < nvalues; i++)
    {
        setRandomSetting(rg, settings, vals->has_set_value() ? vals->add_other_values() : vals->mutable_set_value());
    }
}

void StatementGenerator::generateSettingValues(
    RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingValues * vals)
{
    generateSettingValues(rg, settings, std::min<size_t>(settings.size(), static_cast<size_t>((rg.nextRandomUInt32() % 20) + 1)), vals);
}

void StatementGenerator::generateSettingList(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingList * sl)
{
    const size_t nvalues = std::min<size_t>(settings.size(), static_cast<size_t>((rg.nextRandomUInt32() % 7) + 1));

    for (size_t i = 0; i < nvalues; i++)
    {
        const String & next = rg.pickRandomly(settings);

        if (sl->has_setting())
        {
            sl->add_other_settings(next);
        }
        else
        {
            sl->set_setting(next);
        }
    }
}

DatabaseEngineValues StatementGenerator::getNextDatabaseEngine(RandomGenerator & rg)
{
    chassert(this->ids.empty());
    this->ids.emplace_back(DAtomic);
    if (fc.allow_memory_tables)
    {
        this->ids.emplace_back(DMemory);
    }
    if (replica_setup)
    {
        this->ids.emplace_back(DReplicated);
    }
    if (supports_cloud_features)
    {
        this->ids.emplace_back(DShared);
    }
    const auto res = static_cast<DatabaseEngineValues>(rg.pickRandomly(this->ids));
    this->ids.clear();
    return res;
}

void StatementGenerator::generateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd)
{
    SQLDatabase next;
    const uint32_t dname = this->database_counter++;
    DatabaseEngine * deng = cd->mutable_dengine();

    next.deng = this->getNextDatabaseEngine(rg);
    deng->set_engine(next.deng);
    if (next.isReplicatedDatabase())
    {
        next.zoo_path_counter = this->zoo_path_counter++;
    }
    if (!fc.clusters.empty() && !next.isSharedDatabase() && rg.nextSmallNumber() < (next.isReplicatedDatabase() ? 9 : 4))
    {
        next.cluster = rg.pickRandomly(fc.clusters);
        cd->mutable_cluster()->set_cluster(next.cluster.value());
    }
    next.dname = dname;
    next.finishDatabaseSpecification(deng);
    next.setName(cd->mutable_database());
    if (rg.nextSmallNumber() < 3)
    {
        cd->set_comment(nextComment(rg));
    }
    this->staged_databases[dname] = std::make_shared<SQLDatabase>(std::move(next));
}

void StatementGenerator::generateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf)
{
    SQLFunction next;
    const uint32_t fname = this->function_counter++;
    const bool prev_enforce_final = this->enforce_final;
    const bool prev_allow_not_deterministic = this->allow_not_deterministic;

    next.fname = fname;
    next.nargs = std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % fc.max_columns) + UINT32_C(1));
    next.is_deterministic = rg.nextBool();
    /// If this function is later called by an oracle, then don't call it
    this->allow_not_deterministic = !next.is_deterministic;
    this->enforce_final = next.is_deterministic;
    generateLambdaCall(rg, next.nargs, cf->mutable_lexpr());
    this->levels.clear();
    this->enforce_final = prev_enforce_final;
    this->allow_not_deterministic = prev_allow_not_deterministic;
    if (!fc.clusters.empty() && rg.nextSmallNumber() < 4)
    {
        next.cluster = rg.pickRandomly(fc.clusters);
        cf->mutable_cluster()->set_cluster(next.cluster.value());
    }
    next.setName(cf->mutable_function());
    this->staged_functions[fname] = std::move(next);
}

static void SetViewInterval(RandomGenerator & rg, RefreshInterval * ri)
{
    ri->set_interval(rg.nextSmallNumber() - 1);
    ri->set_unit(RefreshInterval_RefreshUnit::RefreshInterval_RefreshUnit_SECOND);
}

void StatementGenerator::generateNextRefreshableView(RandomGenerator & rg, RefreshableView * cv)
{
    const RefreshableView_RefreshPolicy pol = rg.nextBool() ? RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_EVERY
                                                            : RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_AFTER;

    cv->set_policy(pol);
    SetViewInterval(rg, cv->mutable_interval());
    if (pol == RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_EVERY && rg.nextBool())
    {
        SetViewInterval(rg, cv->mutable_offset());
    }
    SetViewInterval(rg, cv->mutable_randomize());
    cv->set_append(rg.nextBool());
}

static void matchQueryAliases(const SQLView & v, Select * osel, Select * nsel)
{
    /// Make sure aliases match
    SelectStatementCore * ssc = nsel->mutable_select_core();
    JoinedTableOrFunction * jtf = ssc->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();

    for (const auto & entry : v.cols)
    {
        const String ncname = "c" + std::to_string(entry);

        ssc->add_result_columns()
            ->mutable_eca()
            ->mutable_expr()
            ->mutable_comp_expr()
            ->mutable_expr_stc()
            ->mutable_col()
            ->mutable_path()
            ->mutable_col()
            ->set_column(ncname);
        jtf->add_col_aliases()->set_column(ncname);
    }
    jtf->mutable_tof()->mutable_select()->mutable_inner_query()->mutable_select()->set_allocated_sel(osel);
}

void StatementGenerator::generateNextCreateView(RandomGenerator & rg, CreateView * cv)
{
    SQLView next;
    uint32_t tname = 0;
    const uint32_t view_ncols = (rg.nextMediumNumber() % fc.max_columns) + UINT32_C(1);
    const bool prev_enforce_final = this->enforce_final;
    const bool prev_allow_not_deterministic = this->allow_not_deterministic;

    SQLBase::setDeterministic(rg, next);
    this->allow_not_deterministic = !next.is_deterministic;
    this->enforce_final = next.is_deterministic;
    const auto replaceViewLambda = [&next](const SQLView & v) { return v.isAttached() && (v.is_deterministic || !next.is_deterministic); };
    const bool replace = collectionCount<SQLView>(replaceViewLambda) > 3 && rg.nextMediumNumber() < 16;
    if (replace)
    {
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(replaceViewLambda));

        next.db = v.db;
        tname = next.tname = v.tname;
    }
    else
    {
        if (collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && rg.nextSmallNumber() < 9)
        {
            next.db = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    cv->set_create_opt(replace ? CreateReplaceOption::Replace : CreateReplaceOption::Create);
    next.is_materialized = rg.nextBool();
    cv->set_materialized(next.is_materialized);
    next.setName(cv->mutable_est(), false);
    if (next.is_materialized)
    {
        TableEngine * te = cv->mutable_engine();
        const uint32_t nopt = rg.nextSmallNumber();

        if (nopt < 4)
        {
            getNextTableEngine(rg, false, next);
            te->set_engine(next.teng);
        }
        else
        {
            next.teng = MergeTree;
        }
        const auto & table_to_lambda = [&view_ncols, &next](const SQLTable & t)
        { return t.isAttached() && t.numberOfInsertableColumns() >= view_ncols && (t.is_deterministic || !next.is_deterministic); };
        next.has_with_cols = collectionHas<SQLTable>(table_to_lambda);
        const bool has_tables = next.has_with_cols || !tables.empty();
        const bool has_to
            = !replace && nopt > 6 && (next.has_with_cols || has_tables) && rg.nextSmallNumber() < (next.has_with_cols ? 9 : 6);

        chassert(this->entries.empty());
        for (uint32_t i = 0; i < view_ncols; i++)
        {
            std::vector<ColumnPathChainEntry> path = {ColumnPathChainEntry("c" + std::to_string(i), nullptr)};
            entries.emplace_back(ColumnPathChain(std::nullopt, ColumnSpecial::NONE, std::nullopt, std::move(path)));
        }
        if (!has_to)
        {
            SQLRelation rel("v" + std::to_string(next.tname));

            for (uint32_t i = 0; i < view_ncols; i++)
            {
                rel.cols.emplace_back(SQLRelationCol(rel.name, {"c" + std::to_string(i)}));
            }
            this->levels[this->current_level].rels.emplace_back(rel);
            this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
            this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
            generateEngineDetails(rg, next, true, te);
            this->levels.clear();
        }
        if (next.isMergeTreeFamily() && !next.is_deterministic && rg.nextMediumNumber() < 16)
        {
            generateNextTTL(rg, std::nullopt, te, te->mutable_ttl_expr());
        }
        this->entries.clear();

        if (has_to)
        {
            CreateMatViewTo * cmvt = cv->mutable_to();
            SQLTable & t = const_cast<SQLTable &>(
                next.has_with_cols ? rg.pickRandomly(filterCollection<SQLTable>(table_to_lambda)).get()
                                   : rg.pickValueRandomlyFromMap(this->tables));

            t.setName(cmvt->mutable_est(), false);
            if (next.has_with_cols)
            {
                for (const auto & col : t.cols)
                {
                    if (col.second.canBeInserted())
                    {
                        filtered_columns.emplace_back(std::ref<const SQLColumn>(col.second));
                    }
                }
                if (rg.nextBool())
                {
                    std::shuffle(filtered_columns.begin(), filtered_columns.end(), rg.generator);
                }
                for (uint32_t i = 0; i < view_ncols; i++)
                {
                    SQLColumn col = filtered_columns[i].get();

                    addTableColumnInternal(rg, t, col.cname, false, false, ColumnSpecial::NONE, fc.type_mask, col, cmvt->add_col_list());
                    next.cols.insert(col.cname);
                }
                filtered_columns.clear();
            }
        }
        if (!replace && (next.is_refreshable = rg.nextBool()))
        {
            generateNextRefreshableView(rg, cv->mutable_refresh());
            cv->set_empty(rg.nextBool());
        }
        else
        {
            cv->set_populate(!has_to && !replace && rg.nextSmallNumber() < 4);
        }
    }
    if (next.cols.empty())
    {
        for (uint32_t i = 0; i < view_ncols; i++)
        {
            next.cols.insert(i);
        }
    }
    setClusterInfo(rg, next);
    if (next.cluster.has_value())
    {
        cv->mutable_cluster()->set_cluster(next.cluster.value());
    }
    this->levels[this->current_level] = QueryLevel(this->current_level);
    this->allow_in_expression_alias = rg.nextSmallNumber() < 3;
    generateSelect(
        rg,
        false,
        false,
        view_ncols,
        next.is_materialized ? (~allow_prewhere) : std::numeric_limits<uint32_t>::max(),
        cv->mutable_select());
    this->levels.clear();
    this->allow_in_expression_alias = true;
    this->enforce_final = prev_enforce_final;
    this->allow_not_deterministic = prev_allow_not_deterministic;
    matchQueryAliases(next, cv->release_select(), cv->mutable_select());
    if (rg.nextSmallNumber() < 3)
    {
        cv->set_comment(nextComment(rg));
    }
    this->staged_views[tname] = std::move(next);
}

void StatementGenerator::generateNextDrop(RandomGenerator & rg, Drop * dp)
{
    SQLObjectName * sot = dp->mutable_object();
    const uint32_t drop_table = 10 * static_cast<uint32_t>(collectionCount<SQLTable>(attached_tables) > 3);
    const uint32_t drop_view = 10 * static_cast<uint32_t>(collectionCount<SQLView>(attached_views) > 3);
    const uint32_t drop_dictionary = 10 * static_cast<uint32_t>(collectionCount<SQLDictionary>(attached_dictionaries) > 3);
    const uint32_t drop_database = 2 * static_cast<uint32_t>(collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3);
    const uint32_t drop_function = 1 * static_cast<uint32_t>(functions.size() > 3);
    const uint32_t prob_space = drop_table + drop_view + drop_dictionary + drop_database + drop_function;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    std::optional<String> cluster;

    if (drop_table && nopt < (drop_table + 1))
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        cluster = t.getCluster();
        dp->set_is_temp(t.is_temp);
        dp->set_sobject(SQLObject::TABLE);
        dp->set_if_empty(rg.nextSmallNumber() < 4);
        t.setName(sot->mutable_est(), false);
    }
    else if (drop_view && nopt < (drop_table + drop_view + 1))
    {
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

        cluster = v.getCluster();
        dp->set_sobject(SQLObject::VIEW);
        v.setName(sot->mutable_est(), false);
    }
    else if (drop_dictionary && nopt < (drop_table + drop_view + drop_dictionary + 1))
    {
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(attached_dictionaries));

        cluster = d.getCluster();
        dp->set_sobject(SQLObject::DICTIONARY);
        d.setName(sot->mutable_est(), false);
    }
    else if (drop_database && nopt < (drop_table + drop_view + drop_dictionary + drop_database + 1))
    {
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        cluster = d->getCluster();
        dp->set_sobject(SQLObject::DATABASE);
        d->setName(sot->mutable_database());
    }
    else if (drop_function && nopt < (drop_table + drop_view + drop_dictionary + drop_database + drop_function + 1))
    {
        const SQLFunction & f = rg.pickValueRandomlyFromMap(this->functions);

        cluster = f.getCluster();
        dp->set_sobject(SQLObject::FUNCTION);
        f.setName(sot->mutable_function());
    }
    else
    {
        chassert(0);
    }
    if (cluster.has_value())
    {
        dp->mutable_cluster()->set_cluster(cluster.value());
    }
    if (dp->sobject() != SQLObject::FUNCTION)
    {
        dp->set_sync(rg.nextSmallNumber() < 3);
        if (rg.nextSmallNumber() < 3)
        {
            generateSettingValues(rg, serverSettings, dp->mutable_setting_values());
        }
    }
}

void StatementGenerator::generateNextTablePartition(RandomGenerator & rg, const bool allow_parts, const SQLTable & t, PartitionExpr * pexpr)
{
    bool set_part = false;

    if (t.isMergeTreeFamily())
    {
        const String dname = t.db ? ("d" + std::to_string(t.db->dname)) : "";
        const String tname = "t" + std::to_string(t.tname);
        const bool table_has_partitions = rg.nextSmallNumber() < 9 && fc.tableHasPartitions(false, dname, tname);

        if (table_has_partitions)
        {
            if (allow_parts && rg.nextBool())
            {
                pexpr->set_part(fc.tableGetRandomPartitionOrPart(false, false, dname, tname));
            }
            else
            {
                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
            }
            set_part = true;
        }
    }
    if (!set_part)
    {
        pexpr->set_tuple(true);
    }
}

static const auto optimize_table_lambda = [](const SQLTable & t) { return t.isAttached() && t.supportsOptimize(); };

void StatementGenerator::generateNextOptimizeTableInternal(RandomGenerator & rg, const SQLTable & t, bool strict, OptimizeTable * ot)
{
    const std::optional<String> & cluster = t.getCluster();

    t.setName(ot->mutable_est(), false);
    if (t.isMergeTreeFamily())
    {
        if (rg.nextBool())
        {
            generateNextTablePartition(rg, false, t, ot->mutable_single_partition()->mutable_partition());
        }
        ot->set_cleanup(rg.nextSmallNumber() < 3);
    }
    if (!strict && rg.nextSmallNumber() < 4)
    {
        const uint32_t noption = rg.nextMediumNumber();
        DeduplicateExpr * dde = ot->mutable_dedup();

        if (noption < 51)
        {
            ColumnPathList * clist = noption < 26 ? dde->mutable_col_list() : dde->mutable_ded_star_except();
            flatTableColumnPath(flat_tuple | flat_nested | skip_nested_node, t.cols, [](const SQLColumn &) { return true; });
            const uint32_t ocols
                = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(4))) + 1;
            std::shuffle(entries.begin(), entries.end(), rg.generator);
            for (uint32_t i = 0; i < ocols; i++)
            {
                columnPathRef(entries[i], i == 0 ? clist->mutable_col() : clist->add_other_cols());
            }
            entries.clear();
        }
        else if (noption < 76)
        {
            dde->set_ded_star(true);
        }
    }
    if (cluster.has_value())
    {
        ot->mutable_cluster()->set_cluster(cluster.value());
    }
    ot->set_final((t.supportsFinal() || t.isMergeTreeFamily()) && (strict || rg.nextSmallNumber() < 3));
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, ot->mutable_setting_values());
    }
}

void StatementGenerator::generateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * ot)
{
    generateNextOptimizeTableInternal(rg, rg.pickRandomly(filterCollection<SQLTable>(optimize_table_lambda)), false, ot);
}

void StatementGenerator::generateNextCheckTable(RandomGenerator & rg, CheckTable * ct)
{
    const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

    t.setName(ct->mutable_est(), false);
    if (t.isMergeTreeFamily() && rg.nextBool())
    {
        generateNextTablePartition(rg, true, t, ct->mutable_single_partition()->mutable_partition());
    }
    if (rg.nextSmallNumber() < 3)
    {
        SettingValues * vals = ct->mutable_setting_values();

        generateSettingValues(rg, serverSettings, vals);
        if (rg.nextSmallNumber() < 3)
        {
            SetValue * sv = vals->add_other_values();

            sv->set_property("check_query_single_value_result");
            sv->set_value(rg.nextBool() ? "1" : "0");
        }
    }
    ct->set_single_result(rg.nextSmallNumber() < 4);
}

void StatementGenerator::generateNextDescTable(RandomGenerator & rg, DescTable * dt)
{
    const uint32_t desc_table = 10 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t desc_view = 10 * static_cast<uint32_t>(collectionHas<SQLView>(attached_views));
    const uint32_t desc_query = 5;
    const uint32_t desc_function = 5;
    const uint32_t desc_system_table = 3 * static_cast<uint32_t>(!systemTables.empty());
    const uint32_t prob_space = desc_table + desc_view + desc_query + desc_function + desc_system_table;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (desc_table && nopt < (desc_table + 1))
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        t.setName(dt->mutable_est(), false);
    }
    else if (desc_view && nopt < (desc_table + desc_view + 1))
    {
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

        v.setName(dt->mutable_est(), false);
    }
    else if (desc_query && nopt < (desc_table + desc_view + desc_query + 1))
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);
        generateSelect(rg, false, false, (rg.nextLargeNumber() % 5) + 1, std::numeric_limits<uint32_t>::max(), dt->mutable_sel());
        this->levels.clear();
    }
    else if (desc_function && nopt < (desc_table + desc_view + desc_query + desc_function + 1))
    {
        generateTableFuncCall(rg, dt->mutable_stf());
        this->levels.clear();
    }
    else if (desc_system_table && nopt < (desc_table + desc_view + desc_query + desc_function + desc_system_table + 1))
    {
        ExprSchemaTable * est = dt->mutable_est();

        est->mutable_database()->set_database("system");
        est->mutable_table()->set_table(rg.pickRandomly(systemTables));
    }
    else
    {
        chassert(0);
    }
    if (rg.nextSmallNumber() < 3)
    {
        SettingValues * vals = dt->mutable_setting_values();

        generateSettingValues(rg, serverSettings, vals);
        if (rg.nextSmallNumber() < 3)
        {
            SetValue * sv = vals->add_other_values();

            sv->set_property("describe_include_subcolumns");
            sv->set_value(rg.nextBool() ? "1" : "0");
        }
    }
}

void StatementGenerator::generateNextInsert(RandomGenerator & rg, const bool in_parallel, Insert * ins)
{
    String buf;
    bool is_url = false;
    TableOrFunction * tof = ins->mutable_tof();
    const uint32_t noption = rg.nextLargeNumber();
    const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));
    const std::optional<String> & cluster = t.getCluster();
    std::uniform_int_distribution<uint64_t> rows_dist(fc.min_insert_rows, fc.max_insert_rows);
    std::uniform_int_distribution<uint64_t> string_length_dist(1, 8192);
    std::uniform_int_distribution<uint64_t> nested_rows_dist(fc.min_nested_rows, fc.max_nested_rows);

    const uint32_t cluster_func = 5 * static_cast<uint32_t>(cluster.has_value() || !fc.clusters.empty());
    const uint32_t remote_func = 5;
    const uint32_t url_func = 5;
    const uint32_t insert_into_table = 95;
    const uint32_t prob_space = cluster_func + remote_func + url_func + insert_into_table;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt2 = next_dist(rg.generator);

    flatTableColumnPath(skip_nested_node | flat_nested, t.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
    std::shuffle(this->entries.begin(), this->entries.end(), rg.generator);
    if (cluster_func && (nopt2 < cluster_func + 1))
    {
        /// If the table is set on cluster, always insert to all replicas/shards
        ClusterFunc * cdf = tof->mutable_tfunc()->mutable_cluster();

        cdf->set_all_replicas(cluster.has_value() || rg.nextSmallNumber() < 4);
        cdf->mutable_cluster()->set_cluster(cluster.has_value() ? cluster.value() : rg.pickRandomly(fc.clusters));
        t.setName(cdf->mutable_tof()->mutable_est(), true);
        if (rg.nextSmallNumber() < 4)
        {
            /// Optional sharding key
            flatTableColumnPath(to_remote_entries, t.cols, [](const SQLColumn &) { return true; });
            cdf->set_sharding_key(rg.pickRandomly(this->remote_entries).getBottomName());
            this->remote_entries.clear();
        }
    }
    else if (remote_func && (nopt2 < cluster_func + remote_func + 1))
    {
        /// Use insert into remote
        setTableRemote(rg, true, false, t, tof->mutable_tfunc());
    }
    else if ((is_url = (url_func && (nopt2 < cluster_func + remote_func + url_func + 1))))
    {
        /// Use insert into URL
        String url;
        String buf2;
        bool first = true;
        URLFunc * ufunc = tof->mutable_tfunc()->mutable_url();
        const OutFormat outf = rg.nextBool() ? rg.pickRandomly(outIn)
                                             : static_cast<OutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(OutFormat_MAX)) + 1);
        const InFormat iinf = (outIn.find(outf) != outIn.end()) && rg.nextBool()
            ? outIn.at(outf)
            : static_cast<InFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(InFormat_MAX)) + 1);

        if (cluster.has_value() || (!fc.clusters.empty() && rg.nextMediumNumber() < 16))
        {
            ufunc->set_fname(URLFunc_FName::URLFunc_FName_urlCluster);
            ufunc->mutable_cluster()->set_cluster(cluster.has_value() ? cluster.value() : rg.pickRandomly(fc.clusters));
        }
        else
        {
            ufunc->set_fname(URLFunc_FName::URLFunc_FName_url);
        }
        url += "http://" + fc.host + ":" + std::to_string(fc.http_port) + "/?query=INSERT+INTO+" + t.getFullName(rg.nextBool()) + "+(";
        for (const auto & entry : this->entries)
        {
            url += fmt::format("{}{}", first ? "" : ",", columnPathRef(entry));
            buf2 += fmt::format(
                "{}{} {}{}{}",
                first ? "" : ", ",
                entry.getBottomName(),
                entry.path.size() > 1 ? "Array(" : "",
                entry.getBottomType()->typeName(true),
                entry.path.size() > 1 ? ")" : "");
            first = false;
        }
        url += ")+FORMAT+" + InFormat_Name(iinf).substr(3);
        ufunc->set_uurl(std::move(url));
        ufunc->set_outformat(outf);
        ufunc->mutable_structure()->mutable_lit_val()->set_string_lit(std::move(buf2));
    }
    else if (insert_into_table && (nopt2 < cluster_func + remote_func + url_func + insert_into_table + 1))
    {
        /// Use insert into table
        t.setName(tof->mutable_est(), false);
    }
    else
    {
        chassert(0);
    }
    if (!is_url)
    {
        for (const auto & entry : this->entries)
        {
            columnPathRef(entry, ins->add_cols());
        }
    }

    if (!in_parallel && noption < 701)
    {
        const uint64_t nrows = rows_dist(rg.generator);

        for (uint64_t i = 0; i < nrows; i++)
        {
            uint64_t j = 0;
            const uint64_t next_nested_rows = nested_rows_dist(rg.generator);

            if (i != 0)
            {
                buf += ", ";
            }
            buf += "(";
            for (const auto & entry : this->entries)
            {
                if (j != 0)
                {
                    buf += ", ";
                }
                if ((entry.dmod.has_value() && entry.dmod.value() == DModifier::DEF_DEFAULT && rg.nextMediumNumber() < 6)
                    || (entry.path.size() == 1 && rg.nextLargeNumber() < 2))
                {
                    buf += "DEFAULT";
                }
                else if (entry.special == ColumnSpecial::SIGN)
                {
                    buf += rg.nextBool() ? "1" : "-1";
                }
                else if (entry.special == ColumnSpecial::IS_DELETED)
                {
                    buf += rg.nextBool() ? "1" : "0";
                }
                else if (entry.path.size() > 1)
                {
                    /// Make sure all nested entries have the same number of rows
                    buf += ArrayType::appendRandomRawValue(rg, *this, entry.getBottomType(), next_nested_rows);
                }
                else
                {
                    buf += strAppendAnyValue(rg, entry.getBottomType());
                }
                j++;
            }
            buf += ")";
        }
        ins->set_query(buf);
    }
    else if (!in_parallel && noption < 751)
    {
        const uint32_t nrows = (rg.nextSmallNumber() % 3) + 1;
        ValuesStatement * vs = ins->mutable_values();

        this->levels[this->current_level] = QueryLevel(this->current_level);
        this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
        this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
        for (uint32_t i = 0; i < nrows; i++)
        {
            bool first = true;
            ExprList * elist = i == 0 ? vs->mutable_expr_list() : vs->add_extra_expr_lists();

            for (const auto & entry : this->entries)
            {
                Expr * expr = first ? elist->mutable_expr() : elist->add_extra_exprs();

                if (entry.special == ColumnSpecial::SIGN)
                {
                    expr->mutable_lit_val()->mutable_int_lit()->set_int_lit(rg.nextBool() ? 1 : -1);
                }
                else if (entry.special == ColumnSpecial::IS_DELETED)
                {
                    expr->mutable_lit_val()->mutable_int_lit()->set_int_lit(rg.nextBool() ? 1 : 0);
                }
                else
                {
                    generateExpression(rg, expr);
                }
                first = false;
            }
        }
        this->levels.clear();
    }
    else
    {
        Select * sel = ins->mutable_select();

        if (noption < 901)
        {
            /// Use generateRandom
            bool first = true;
            SelectStatementCore * ssc = sel->mutable_select_core();
            GenerateRandomFunc * grf = ssc->mutable_from()
                                           ->mutable_tos()
                                           ->mutable_join_clause()
                                           ->mutable_tos()
                                           ->mutable_joined_table()
                                           ->mutable_tof()
                                           ->mutable_tfunc()
                                           ->mutable_grandom();

            for (const auto & entry : this->entries)
            {
                const String & bottomName = entry.getBottomName();

                buf += fmt::format(
                    "{}{} {}{}{}",
                    first ? "" : ", ",
                    bottomName,
                    entry.path.size() > 1 ? "Array(" : "",
                    entry.getBottomType()->typeName(false),
                    entry.path.size() > 1 ? ")" : "");
                ssc->add_result_columns()->mutable_etc()->mutable_col()->mutable_path()->mutable_col()->set_column(bottomName);
                first = false;
            }
            grf->mutable_structure()->mutable_lit_val()->set_string_lit(std::move(buf));
            grf->set_random_seed(rg.nextRandomUInt64());
            grf->set_max_string_length(string_length_dist(rg.generator));
            grf->set_max_array_length(nested_rows_dist(rg.generator));
            ssc->mutable_limit()->mutable_limit()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(rows_dist(rg.generator));
        }
        else
        {
            this->levels[this->current_level] = QueryLevel(this->current_level);
            if (rg.nextMediumNumber() < 13)
            {
                this->addCTEs(rg, std::numeric_limits<uint32_t>::max(), ins->mutable_ctes());
            }
            generateSelect(rg, true, false, static_cast<uint32_t>(this->entries.size()), std::numeric_limits<uint32_t>::max(), sel);
            this->levels.clear();
        }
    }
    this->entries.clear();
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, formatSettings, ins->mutable_setting_values());
    }
}

void StatementGenerator::generateUptDelWhere(RandomGenerator & rg, const SQLTable & t, Expr * expr)
{
    if (rg.nextSmallNumber() < 10)
    {
        addTableRelation(rg, true, "", t);
        this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
        this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
        generateWherePredicate(rg, expr);
        this->levels.clear();
    }
    else
    {
        expr->mutable_lit_val()->mutable_special_val()->set_val(SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_TRUE);
    }
}

void StatementGenerator::generateNextDelete(RandomGenerator & rg, LightDelete * del)
{
    const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));
    const std::optional<String> & cluster = t.getCluster();

    t.setName(del->mutable_est(), false);
    if (cluster.has_value())
    {
        del->mutable_cluster()->set_cluster(cluster.value());
    }
    if (t.isMergeTreeFamily() && rg.nextBool())
    {
        generateNextTablePartition(rg, false, t, del->mutable_single_partition()->mutable_partition());
    }
    generateUptDelWhere(rg, t, del->mutable_where()->mutable_expr()->mutable_expr());
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, del->mutable_setting_values());
    }
}

void StatementGenerator::generateNextTruncate(RandomGenerator & rg, Truncate * trunc)
{
    const bool trunc_database = collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases);
    const uint32_t trunc_table = 980 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t trunc_db_tables = 15 * static_cast<uint32_t>(trunc_database);
    const uint32_t trunc_db = 5 * static_cast<uint32_t>(trunc_database);
    const uint32_t prob_space = trunc_table + trunc_db_tables + trunc_db;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    std::optional<String> cluster;

    if (trunc_table && nopt < (trunc_table + 1))
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        cluster = t.getCluster();
        t.setName(trunc->mutable_est(), false);
    }
    else if (trunc_db_tables && nopt < (trunc_table + trunc_db_tables + 1))
    {
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        cluster = d->getCluster();
        d->setName(trunc->mutable_all_tables());
    }
    else if (trunc_db && nopt < (trunc_table + trunc_db_tables + trunc_db + 1))
    {
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        cluster = d->getCluster();
        d->setName(trunc->mutable_database());
    }
    else
    {
        chassert(0);
    }
    if (cluster.has_value())
    {
        trunc->mutable_cluster()->set_cluster(cluster.value());
    }
    trunc->set_sync(rg.nextSmallNumber() < 4);
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, trunc->mutable_setting_values());
    }
}

static const auto exchange_table_lambda = [](const SQLTable & t)
{
    /// I would need to track the table clusters to do this correctly, ie ensure tables to be exchanged are on same cluster
    return t.isAttached() && !t.is_deterministic && !t.hasDatabasePeer();
};

void StatementGenerator::generateNextExchange(RandomGenerator & rg, Exchange * exc)
{
    ExprSchemaTable * est1 = exc->mutable_object1()->mutable_est();
    ExprSchemaTable * est2 = exc->mutable_object2()->mutable_est();
    const uint32_t exchange_table = 10 * static_cast<uint32_t>(collectionCount<SQLTable>(exchange_table_lambda) > 1);
    const uint32_t exchange_view = 10 * static_cast<uint32_t>(collectionCount<SQLView>(attached_views) > 1);
    const uint32_t exchange_dictionary = 10 * static_cast<uint32_t>(collectionCount<SQLDictionary>(attached_dictionaries) > 1);
    const uint32_t prob_space = exchange_table + exchange_view + exchange_dictionary;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    std::optional<String> cluster1;
    std::optional<String> cluster2;

    if (exchange_table && nopt < (exchange_table + 1))
    {
        const auto & input = filterCollection<SQLTable>(exchange_table_lambda);

        exc->set_sobject(SQLObject::TABLE);
        for (const auto & entry : input)
        {
            this->ids.push_back(entry.get().tname);
        }
        std::shuffle(this->ids.begin(), this->ids.end(), rg.generator);
        const SQLTable & t1 = this->tables[this->ids[0]];
        const SQLTable & t2 = this->tables[this->ids[1]];

        cluster1 = t1.cluster;
        cluster2 = t2.cluster;
        t1.setName(est1, false);
        t2.setName(est2, false);
    }
    else if (exchange_view && nopt < (exchange_table + exchange_view + 1))
    {
        const auto & input = filterCollection<SQLView>(attached_views);

        exc->set_sobject(SQLObject::TABLE);
        for (const auto & entry : input)
        {
            this->ids.push_back(entry.get().tname);
        }
        std::shuffle(this->ids.begin(), this->ids.end(), rg.generator);
        const SQLView & v1 = this->views[this->ids[0]];
        const SQLView & v2 = this->views[this->ids[1]];

        cluster1 = v1.cluster;
        cluster2 = v2.cluster;
        v1.setName(est1, false);
        v2.setName(est2, false);
    }
    else if (exchange_dictionary && nopt < (exchange_table + exchange_view + exchange_dictionary + 1))
    {
        const auto & input = filterCollection<SQLDictionary>(attached_dictionaries);

        exc->set_sobject(SQLObject::DICTIONARY);
        for (const auto & entry : input)
        {
            this->ids.push_back(entry.get().tname);
        }
        std::shuffle(this->ids.begin(), this->ids.end(), rg.generator);
        const SQLDictionary & d1 = this->dictionaries[this->ids[0]];
        const SQLDictionary & d2 = this->dictionaries[this->ids[1]];

        cluster1 = d1.cluster;
        cluster2 = d2.cluster;
        d1.setName(est1, false);
        d2.setName(est2, false);
    }
    else
    {
        chassert(0);
    }
    this->ids.clear();
    if (cluster1.has_value() && cluster2.has_value() && cluster1 == cluster2)
    {
        exc->mutable_cluster()->set_cluster(cluster1.value());
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, exc->mutable_setting_values());
    }
}

static const auto alter_table_lambda = [](const SQLTable & t) { return t.isAttached() && !t.isFileEngine(); };

void StatementGenerator::generateAlter(RandomGenerator & rg, Alter * at)
{
    SQLObjectName * sot = at->mutable_object();
    const uint32_t alter_view = 5 * static_cast<uint32_t>(collectionHas<SQLView>(attached_views));
    const uint32_t alter_table = 15 * static_cast<uint32_t>(collectionHas<SQLTable>(alter_table_lambda));
    const uint32_t alter_database = 2 * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases));
    const uint32_t prob_space2 = alter_view + alter_table + alter_database;
    std::uniform_int_distribution<uint32_t> next_dist2(1, prob_space2);
    const uint32_t nopt2 = next_dist2(rg.generator);
    std::optional<String> cluster;
    const bool prev_enforce_final = this->enforce_final;
    const bool prev_allow_not_deterministic = this->allow_not_deterministic;
    const uint32_t nalters = rg.nextBool() ? 1 : ((rg.nextMediumNumber() % 4) + 1);

    if (alter_view && nopt2 < (alter_view + 1))
    {
        SQLView & v = const_cast<SQLView &>(rg.pickRandomly(filterCollection<SQLView>(attached_views)).get());

        this->allow_not_deterministic = !v.is_deterministic;
        this->enforce_final = v.is_deterministic;
        cluster = v.getCluster();
        at->set_sobject(SQLObject::TABLE);
        v.setName(sot->mutable_est(), false);
        for (uint32_t i = 0; i < nalters; i++)
        {
            const uint32_t alter_refresh = 1 * static_cast<uint32_t>(v.is_refreshable);
            const uint32_t alter_query = 3;
            const uint32_t comment_view = 2;
            const uint32_t prob_space = alter_refresh + alter_query + comment_view;
            AlterItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            if (alter_refresh && nopt < (alter_refresh + 1))
            {
                generateNextRefreshableView(rg, ati->mutable_refresh());
            }
            else if (alter_query && nopt < (alter_refresh + alter_query + 1))
            {
                v.staged_ncols
                    = v.has_with_cols ? static_cast<uint32_t>(v.cols.size()) : ((rg.nextMediumNumber() % fc.max_columns) + UINT32_C(1));
                this->levels[this->current_level] = QueryLevel(this->current_level);
                this->allow_in_expression_alias = rg.nextSmallNumber() < 3;
                generateSelect(
                    rg,
                    false,
                    false,
                    v.staged_ncols,
                    v.is_materialized ? (~allow_prewhere) : std::numeric_limits<uint32_t>::max(),
                    ati->mutable_modify_query());
                this->levels.clear();
                this->allow_in_expression_alias = true;
                matchQueryAliases(v, ati->release_modify_query(), ati->mutable_modify_query());
            }
            else if (comment_view && nopt < (alter_refresh + alter_query + comment_view + 1))
            {
                ati->set_comment(nextComment(rg));
            }
            else
            {
                chassert(0);
            }
        }
    }
    else if (alter_table && nopt2 < (alter_view + alter_table + 1))
    {
        SQLTable & t = const_cast<SQLTable &>(rg.pickRandomly(filterCollection<SQLTable>(alter_table_lambda)).get());
        const String dname = t.db ? ("d" + std::to_string(t.db->dname)) : "";
        const String tname = "t" + std::to_string(t.tname);
        const bool table_has_partitions = t.isMergeTreeFamily() && fc.tableHasPartitions(false, dname, tname);

        this->allow_not_deterministic = !t.is_deterministic;
        this->enforce_final = t.is_deterministic;
        cluster = t.getCluster();
        at->set_is_temp(t.is_temp);
        at->set_sobject(SQLObject::TABLE);
        t.setName(sot->mutable_est(), false);
        for (uint32_t i = 0; i < nalters; i++)
        {
            const uint32_t alter_order_by = 3 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t heavy_delete = 30;
            const uint32_t heavy_update = 40;
            const uint32_t add_column = 2 * static_cast<uint32_t>(!t.hasDatabasePeer() && t.cols.size() < 10);
            const uint32_t materialize_column = 2;
            const uint32_t drop_column = 2 * static_cast<uint32_t>(!t.hasDatabasePeer() && t.cols.size() > 1);
            const uint32_t rename_column = 2 * static_cast<uint32_t>(!t.hasDatabasePeer());
            const uint32_t clear_column = 2;
            const uint32_t modify_column = 2 * static_cast<uint32_t>(!t.hasDatabasePeer());
            const uint32_t comment_column = 2;
            const uint32_t add_stats = 3 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t mod_stats = 3 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t drop_stats = 3 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t clear_stats = 3 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t mat_stats = 3 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t delete_mask = 8 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t add_idx = 2 * static_cast<uint32_t>(t.idxs.size() < 3);
            const uint32_t materialize_idx = 2 * static_cast<uint32_t>(!t.idxs.empty());
            const uint32_t clear_idx = 2 * static_cast<uint32_t>(!t.idxs.empty());
            const uint32_t drop_idx = 2 * static_cast<uint32_t>(!t.idxs.empty());
            const uint32_t column_remove_property = 2;
            const uint32_t column_modify_setting = 2 * static_cast<uint32_t>(!allColumnSettings.at(t.teng).empty());
            const uint32_t column_remove_setting = 2 * static_cast<uint32_t>(!allColumnSettings.at(t.teng).empty());
            const uint32_t table_modify_setting = 2;
            const uint32_t table_remove_setting = 2;
            const uint32_t add_projection = 2 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t remove_projection = 2 * static_cast<uint32_t>(t.isMergeTreeFamily() && !t.projs.empty());
            const uint32_t materialize_projection = 2 * static_cast<uint32_t>(t.isMergeTreeFamily() && !t.projs.empty());
            const uint32_t clear_projection = 2 * static_cast<uint32_t>(t.isMergeTreeFamily() && !t.projs.empty());
            const uint32_t add_constraint = 2 * static_cast<uint32_t>(t.constrs.size() < 4);
            const uint32_t remove_constraint = 2 * static_cast<uint32_t>(!t.constrs.empty());
            const uint32_t detach_partition = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t drop_partition = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t drop_detached_partition = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t forget_partition = 5 * static_cast<uint32_t>(table_has_partitions);
            const uint32_t attach_partition = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t move_partition_to = 5 * static_cast<uint32_t>(table_has_partitions);
            const uint32_t clear_column_partition = 5 * static_cast<uint32_t>(table_has_partitions);
            const uint32_t freeze_partition = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t unfreeze_partition = 7 * static_cast<uint32_t>(!t.frozen_partitions.empty());
            const uint32_t clear_index_partition = 5 * static_cast<uint32_t>(table_has_partitions && !t.idxs.empty());
            const uint32_t move_partition = 5 * static_cast<uint32_t>(table_has_partitions && !fc.disks.empty());
            const uint32_t modify_ttl = 5 * static_cast<uint32_t>(t.isMergeTreeFamily() && !t.is_deterministic);
            const uint32_t remove_ttl = 2 * static_cast<uint32_t>(t.isMergeTreeFamily() && !t.is_deterministic);
            const uint32_t attach_partition_from = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t replace_partition_from = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t comment_table = 2;
            const uint32_t prob_space = alter_order_by + heavy_delete + heavy_update + add_column + materialize_column + drop_column
                + rename_column + clear_column + modify_column + comment_column + delete_mask + add_stats + mod_stats + drop_stats
                + clear_stats + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property
                + column_modify_setting + column_remove_setting + table_modify_setting + table_remove_setting + add_projection
                + remove_projection + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition
                + drop_partition + drop_detached_partition + forget_partition + attach_partition + move_partition_to
                + clear_column_partition + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl
                + remove_ttl + attach_partition_from + replace_partition_from + comment_table;
            AlterItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            if (alter_order_by && nopt < (alter_order_by + 1))
            {
                TableKey * tkey = ati->mutable_order();

                if (rg.nextSmallNumber() < 6)
                {
                    flatTableColumnPath(
                        flat_tuple | flat_nested | flat_json | skip_nested_node, t.cols, [](const SQLColumn &) { return true; });
                    generateTableKey(rg, t.teng, true, tkey);
                    this->entries.clear();
                    this->levels.clear();
                }
            }
            else if (heavy_delete && nopt < (heavy_delete + alter_order_by + 1))
            {
                HeavyDelete * hdel = ati->mutable_del();

                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, hdel->mutable_single_partition()->mutable_partition());
                }
                generateUptDelWhere(rg, t, hdel->mutable_del()->mutable_expr()->mutable_expr());
            }
            else if (add_column && nopt < (heavy_delete + alter_order_by + add_column + 1))
            {
                const uint32_t next_option = rg.nextSmallNumber();
                AddColumn * add_col = ati->mutable_add_column();

                addTableColumn(
                    rg, t, t.col_counter++, true, false, rg.nextMediumNumber() < 6, ColumnSpecial::NONE, add_col->mutable_new_col());
                if (next_option < 4)
                {
                    flatTableColumnPath(flat_tuple | flat_nested, t.cols, [](const SQLColumn &) { return true; });
                    columnPathRef(rg.pickRandomly(this->entries), add_col->mutable_add_where()->mutable_col());
                    this->entries.clear();
                }
                else if (next_option < 8)
                {
                    add_col->mutable_add_where()->set_first(true);
                }
            }
            else if (materialize_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + 1))
            {
                ColInPartition * mcol = ati->mutable_materialize_column();

                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), mcol->mutable_col());
                this->entries.clear();
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, mcol->mutable_single_partition()->mutable_partition());
                }
            }
            else if (drop_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + 1))
            {
                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), ati->mutable_drop_column());
                this->entries.clear();
            }
            else if (
                rename_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + 1))
            {
                const uint32_t ncname = t.col_counter++;
                RenameCol * rcol = ati->mutable_rename_column();

                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), rcol->mutable_old_name());
                this->entries.clear();

                rcol->mutable_new_name()->CopyFrom(rcol->old_name());
                const uint32_t size = rcol->new_name().sub_cols_size();
                Column & ncol = const_cast<Column &>(size ? rcol->new_name().sub_cols(size - 1) : rcol->new_name().col());
                ncol.set_column("c" + std::to_string(ncname));
            }
            else if (
                clear_column
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column + 1))
            {
                ColInPartition * ccol = ati->mutable_clear_column();

                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), ccol->mutable_col());
                this->entries.clear();
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, ccol->mutable_single_partition()->mutable_partition());
                }
            }
            else if (
                modify_column
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + 1))
            {
                const uint32_t next_option = rg.nextSmallNumber();
                AddColumn * add_col = ati->mutable_modify_column();

                addTableColumn(
                    rg, t, rg.pickRandomly(t.cols), true, true, rg.nextMediumNumber() < 6, ColumnSpecial::NONE, add_col->mutable_new_col());
                if (next_option < 4)
                {
                    flatTableColumnPath(flat_tuple | flat_nested, t.cols, [](const SQLColumn &) { return true; });
                    columnPathRef(rg.pickRandomly(this->entries), add_col->mutable_add_where()->mutable_col());
                    this->entries.clear();
                }
                else if (next_option < 8)
                {
                    add_col->mutable_add_where()->set_first(true);
                }
            }
            else if (
                comment_column
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + 1))
            {
                CommentColumn * ccol = ati->mutable_comment_column();

                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), ccol->mutable_col());
                this->entries.clear();
                ccol->set_comment(nextComment(rg));
            }
            else if (
                delete_mask
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + 1))
            {
                ApplyDeleteMask * adm = ati->mutable_delete_mask();

                if (rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, adm->mutable_single_partition()->mutable_partition());
                }
            }
            else if (
                heavy_update
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + 1))
            {
                Update * upt = ati->mutable_update();

                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, upt->mutable_single_partition()->mutable_partition());
                }
                flatTableColumnPath(0, t.cols, [](const SQLColumn & c) { return c.tp->getTypeClass() != SQLTypeClass::NESTED; });
                if (this->entries.empty())
                {
                    UpdateSet * upset = upt->mutable_update();

                    upset->mutable_col()->mutable_col()->set_column("c0");
                    upset->mutable_expr()->mutable_lit_val()->mutable_int_lit()->set_int_lit(0);
                }
                else
                {
                    const uint32_t nupdates
                        = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(4))) + 1;

                    std::shuffle(this->entries.begin(), this->entries.end(), rg.generator);
                    for (uint32_t j = 0; j < nupdates; j++)
                    {
                        columnPathRef(
                            this->entries[j], j == 0 ? upt->mutable_update()->mutable_col() : upt->add_other_updates()->mutable_col());
                    }
                    addTableRelation(rg, true, "", t);
                    this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
                    this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
                    for (uint32_t j = 0; j < nupdates; j++)
                    {
                        const ColumnPathChain & entry = this->entries[j];
                        UpdateSet & uset = const_cast<UpdateSet &>(j == 0 ? upt->update() : upt->other_updates(j - 1));
                        Expr * expr = uset.mutable_expr();

                        if (rg.nextSmallNumber() < 9)
                        {
                            /// Set constant value
                            String buf;
                            LiteralValue * lv = expr->mutable_lit_val();

                            if ((entry.dmod.has_value() && entry.dmod.value() == DModifier::DEF_DEFAULT && rg.nextMediumNumber() < 6)
                                || (entry.path.size() == 1 && rg.nextLargeNumber() < 2))
                            {
                                buf = "DEFAULT";
                            }
                            else if (entry.special == ColumnSpecial::SIGN)
                            {
                                buf = rg.nextBool() ? "1" : "-1";
                            }
                            else if (entry.special == ColumnSpecial::IS_DELETED)
                            {
                                buf = rg.nextBool() ? "1" : "0";
                            }
                            else
                            {
                                buf = strAppendAnyValue(rg, entry.getBottomType());
                            }
                            lv->set_no_quote_str(buf);
                        }
                        else
                        {
                            generateExpression(rg, expr);
                        }
                    }
                    this->levels.clear();
                    this->entries.clear();
                }

                generateUptDelWhere(rg, t, upt->mutable_where()->mutable_expr()->mutable_expr());
            }
            else if (
                add_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + 1))
            {
                AddStatistics * ads = ati->mutable_add_stats();

                pickUpNextCols(rg, t, ads->mutable_cols());
                generateNextStatistics(rg, ads->mutable_stats());
            }
            else if (
                mod_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + 1))
            {
                AddStatistics * ads = ati->mutable_mod_stats();

                pickUpNextCols(rg, t, ads->mutable_cols());
                generateNextStatistics(rg, ads->mutable_stats());
            }
            else if (
                drop_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + 1))
            {
                pickUpNextCols(rg, t, ati->mutable_drop_stats());
            }
            else if (
                clear_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + 1))
            {
                pickUpNextCols(rg, t, ati->mutable_clear_stats());
            }
            else if (
                mat_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + 1))
            {
                pickUpNextCols(rg, t, ati->mutable_mat_stats());
            }
            else if (
                add_idx
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + 1))
            {
                AddIndex * add_index = ati->mutable_add_index();

                addTableIndex(rg, t, true, add_index->mutable_new_idx());
                if (!t.idxs.empty())
                {
                    const uint32_t next_option = rg.nextSmallNumber();

                    if (next_option < 4)
                    {
                        add_index->mutable_add_where()->mutable_idx()->set_index("i" + std::to_string(rg.pickRandomly(t.idxs)));
                    }
                    else if (next_option < 8)
                    {
                        add_index->mutable_add_where()->set_first(true);
                    }
                }
            }
            else if (
                materialize_idx
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + 1))
            {
                IdxInPartition * iip = ati->mutable_materialize_index();

                iip->mutable_idx()->set_index("i" + std::to_string(rg.pickRandomly(t.idxs)));
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, iip->mutable_single_partition()->mutable_partition());
                }
            }
            else if (
                clear_idx
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + 1))
            {
                IdxInPartition * iip = ati->mutable_clear_index();

                iip->mutable_idx()->set_index("i" + std::to_string(rg.pickRandomly(t.idxs)));
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, iip->mutable_single_partition()->mutable_partition());
                }
            }
            else if (
                drop_idx
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + 1))
            {
                ati->mutable_drop_index()->set_index("i" + std::to_string(rg.pickRandomly(t.idxs)));
            }
            else if (
                column_remove_property
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + 1))
            {
                RemoveColumnProperty * rcs = ati->mutable_column_remove_property();
                std::uniform_int_distribution<uint32_t> prop_range(1, static_cast<uint32_t>(RemoveColumnProperty::ColumnProperties_MAX));

                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), rcs->mutable_col());
                this->entries.clear();
                rcs->set_property(static_cast<RemoveColumnProperty_ColumnProperties>(prop_range(rg.generator)));
            }
            else if (
                column_modify_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + 1))
            {
                ModifyColumnSetting * mcp = ati->mutable_column_modify_setting();
                const auto & csettings = allColumnSettings.at(t.teng);

                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), mcp->mutable_col());
                this->entries.clear();
                generateSettingValues(rg, csettings, mcp->mutable_setting_values());
            }
            else if (
                column_remove_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + 1))
            {
                RemoveColumnSetting * rcp = ati->mutable_column_remove_setting();
                const auto & csettings = allColumnSettings.at(t.teng);

                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), rcp->mutable_col());
                this->entries.clear();
                generateSettingList(rg, csettings, rcp->mutable_setting_values());
            }
            else if (
                table_modify_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + 1))
            {
                SettingValues * svs = ati->mutable_table_modify_setting();
                const auto & engineSettings = allTableSettings.at(t.teng);

                if (!engineSettings.empty() && rg.nextSmallNumber() < 9)
                {
                    /// Modify table engine settings
                    generateSettingValues(rg, engineSettings, svs);
                }
                if (!svs->has_set_value() || rg.nextSmallNumber() < 4)
                {
                    /// Modify server settings
                    generateSettingValues(rg, serverSettings, svs);
                }
            }
            else if (
                table_remove_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + 1))
            {
                SettingList * sl = ati->mutable_table_remove_setting();
                const auto & engineSettings = allTableSettings.at(t.teng);

                if (!engineSettings.empty() && rg.nextSmallNumber() < 9)
                {
                    /// Remove table engine settings
                    generateSettingList(rg, engineSettings, sl);
                }
                if (!sl->has_setting() || rg.nextSmallNumber() < 4)
                {
                    /// Remove server settings
                    generateSettingList(rg, serverSettings, sl);
                }
            }
            else if (
                add_projection
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + 1))
            {
                addTableProjection(rg, t, true, ati->mutable_add_projection());
            }
            else if (
                remove_projection
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection + 1))
            {
                ati->mutable_remove_projection()->set_projection("p" + std::to_string(rg.pickRandomly(t.projs)));
            }
            else if (
                materialize_projection
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + 1))
            {
                ProjectionInPartition * pip = ati->mutable_materialize_projection();

                pip->mutable_proj()->set_projection("p" + std::to_string(rg.pickRandomly(t.projs)));
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, pip->mutable_single_partition()->mutable_partition());
                }
            }
            else if (
                clear_projection
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + 1))
            {
                ProjectionInPartition * pip = ati->mutable_clear_projection();

                pip->mutable_proj()->set_projection("p" + std::to_string(rg.pickRandomly(t.projs)));
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, pip->mutable_single_partition()->mutable_partition());
                }
            }
            else if (
                add_constraint
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + 1))
            {
                addTableConstraint(rg, t, true, ati->mutable_add_constraint());
            }
            else if (
                remove_constraint
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + 1))
            {
                ati->mutable_remove_constraint()->set_constraint("c" + std::to_string(rg.pickRandomly(t.constrs)));
            }
            else if (
                detach_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + 1))
            {
                const uint32_t nopt3 = rg.nextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_detach_partition()->mutable_partition();

                if (table_has_partitions && nopt3 < 5)
                {
                    pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
                }
                else if (table_has_partitions && nopt3 < 9)
                {
                    pexpr->set_part(fc.tableGetRandomPartitionOrPart(false, false, dname, tname));
                }
                else
                {
                    pexpr->set_all(true);
                }
            }
            else if (
                drop_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + 1))
            {
                const uint32_t nopt3 = rg.nextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_drop_partition()->mutable_partition();

                if (table_has_partitions && nopt3 < 5)
                {
                    pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
                }
                else if (table_has_partitions && nopt3 < 9)
                {
                    pexpr->set_part(fc.tableGetRandomPartitionOrPart(false, false, dname, tname));
                }
                else
                {
                    pexpr->set_all(true);
                }
            }
            else if (
                drop_detached_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition
                       + drop_detached_partition + 1))
            {
                const uint32_t nopt3 = rg.nextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_drop_detached_partition()->mutable_partition();
                const bool table_has_detached_partitions = fc.tableHasPartitions(true, dname, tname);

                if (table_has_detached_partitions && nopt3 < 5)
                {
                    pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(true, true, dname, tname));
                }
                else if (table_has_detached_partitions && nopt3 < 9)
                {
                    pexpr->set_part(fc.tableGetRandomPartitionOrPart(true, false, dname, tname));
                }
                else
                {
                    pexpr->set_all(true);
                }
            }
            else if (
                forget_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + 1))
            {
                PartitionExpr * pexpr = ati->mutable_forget_partition()->mutable_partition();

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
            }
            else if (
                attach_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + 1))
            {
                const uint32_t nopt3 = rg.nextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_attach_partition()->mutable_partition();
                const bool table_has_detached_partitions = fc.tableHasPartitions(true, dname, tname);

                if (table_has_detached_partitions && nopt3 < 5)
                {
                    pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(true, true, dname, tname));
                }
                else if (table_has_detached_partitions && nopt3 < 9)
                {
                    pexpr->set_part(fc.tableGetRandomPartitionOrPart(true, false, dname, tname));
                }
                else
                {
                    pexpr->set_all(true);
                }
            }
            else if (
                move_partition_to
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + 1))
            {
                AttachPartitionFrom * apf = ati->mutable_move_partition_to();
                PartitionExpr * pexpr = apf->mutable_single_partition()->mutable_partition();
                const SQLTable & t2 = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
                t2.setName(apf->mutable_est(), false);
            }
            else if (
                clear_column_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition + 1))
            {
                ClearColumnInPartition * ccip = ati->mutable_clear_column_partition();
                PartitionExpr * pexpr = ccip->mutable_single_partition()->mutable_partition();

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), ccip->mutable_col());
                this->entries.clear();
            }
            else if (
                freeze_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + 1))
            {
                FreezePartition * fp = ati->mutable_freeze_partition();

                if (table_has_partitions && rg.nextSmallNumber() < 9)
                {
                    fp->mutable_single_partition()->mutable_partition()->set_partition_id(
                        fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
                }
                fp->set_fname(t.freeze_counter++);
            }
            else if (
                unfreeze_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + 1))
            {
                FreezePartition * fp = ati->mutable_unfreeze_partition();
                const uint32_t fname = rg.pickRandomly(t.frozen_partitions);
                const String & partition_id = t.frozen_partitions[fname];

                if (!partition_id.empty())
                {
                    fp->mutable_single_partition()->mutable_partition()->set_partition_id(partition_id);
                }
                fp->set_fname(fname);
            }
            else if (
                clear_index_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + 1))
            {
                ClearIndexInPartition * ccip = ati->mutable_clear_index_partition();
                PartitionExpr * pexpr = ccip->mutable_single_partition()->mutable_partition();

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
                ccip->mutable_idx()->set_index("i" + std::to_string(rg.pickRandomly(t.idxs)));
            }
            else if (
                move_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + 1))
            {
                MovePartition * mp = ati->mutable_move_partition();
                PartitionExpr * pexpr = mp->mutable_single_partition()->mutable_partition();

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
                generateStorage(rg, mp->mutable_storage());
            }
            else if (
                modify_ttl
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl + 1))
            {
                flatTableColumnPath(0, t.cols, [](const SQLColumn & c) { return c.tp->getTypeClass() != SQLTypeClass::NESTED; });
                generateNextTTL(rg, t, nullptr, ati->mutable_modify_ttl());
                this->entries.clear();
            }
            else if (
                remove_ttl
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl + remove_ttl + 1))
            {
                ati->set_remove_ttl(true);
            }
            else if (
                attach_partition_from
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl + remove_ttl
                       + attach_partition_from + 1))
            {
                AttachPartitionFrom * apf = ati->mutable_attach_partition_from();
                PartitionExpr * pexpr = apf->mutable_single_partition()->mutable_partition();
                const SQLTable & t2 = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));
                const String dname2 = t2.db ? ("d" + std::to_string(t2.db->dname)) : "";
                const String tname2 = "t" + std::to_string(t2.tname);
                const bool table_has_partitions2 = t2.isMergeTreeFamily() && fc.tableHasPartitions(false, dname2, tname2);

                pexpr->set_partition_id(table_has_partitions2 ? fc.tableGetRandomPartitionOrPart(false, true, dname2, tname2) : "0");
                t2.setName(apf->mutable_est(), false);
            }
            else if (
                replace_partition_from
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl + remove_ttl
                       + attach_partition_from + replace_partition_from + 1))
            {
                AttachPartitionFrom * apf = ati->mutable_replace_partition_from();
                PartitionExpr * pexpr = apf->mutable_single_partition()->mutable_partition();
                const SQLTable & t2 = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));
                const String dname2 = t2.db ? ("d" + std::to_string(t2.db->dname)) : "";
                const String tname2 = "t" + std::to_string(t2.tname);
                const bool table_has_partitions2 = t2.isMergeTreeFamily() && fc.tableHasPartitions(false, dname2, tname2);

                pexpr->set_partition_id(table_has_partitions2 ? fc.tableGetRandomPartitionOrPart(false, true, dname2, tname2) : "0");
                t2.setName(apf->mutable_est(), false);
            }
            else if (
                comment_table
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl + remove_ttl
                       + attach_partition_from + replace_partition_from + comment_table + 1))
            {
                ati->set_comment(nextComment(rg));
            }
            else
            {
                chassert(0);
            }
        }
    }
    else if (alter_database && nopt2 < (alter_view + alter_table + alter_database + 1))
    {
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        cluster = d->getCluster();
        at->set_sobject(SQLObject::DATABASE);
        d->setName(sot->mutable_database());
        for (uint32_t i = 0; i < nalters; i++)
        {
            AlterItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();

            ati->set_comment(nextComment(rg));
        }
    }
    else
    {
        chassert(0);
    }
    if (cluster.has_value())
    {
        at->mutable_cluster()->set_cluster(cluster.value());
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, at->mutable_setting_values());
    }
    this->enforce_final = prev_enforce_final;
    this->allow_not_deterministic = prev_allow_not_deterministic;
}

void StatementGenerator::generateAttach(RandomGenerator & rg, Attach * att)
{
    SQLObjectName * sot = att->mutable_object();
    const uint32_t attach_table = 10 * static_cast<uint32_t>(collectionHas<SQLTable>(detached_tables));
    const uint32_t attach_view = 10 * static_cast<uint32_t>(collectionHas<SQLView>(detached_views));
    const uint32_t attach_dictionary = 10 * static_cast<uint32_t>(collectionHas<SQLDictionary>(detached_dictionaries));
    const uint32_t attach_database = 2 * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(detached_databases));
    const uint32_t prob_space = attach_table + attach_view + attach_dictionary + attach_database;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    std::optional<String> cluster;

    if (attach_table && nopt < (attach_table + 1))
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(detached_tables));

        cluster = t.getCluster();
        att->set_sobject(SQLObject::TABLE);
        t.setName(sot->mutable_est(), false);
    }
    else if (attach_view && nopt < (attach_table + attach_view + 1))
    {
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(detached_views));

        cluster = v.getCluster();
        att->set_sobject(SQLObject::TABLE);
        v.setName(sot->mutable_est(), false);
    }
    else if (attach_dictionary && nopt < (attach_table + attach_view + attach_dictionary + 1))
    {
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(detached_dictionaries));

        cluster = d.getCluster();
        att->set_sobject(SQLObject::DICTIONARY);
        d.setName(sot->mutable_est(), false);
    }
    else if (attach_database && nopt < (attach_table + attach_view + attach_dictionary + attach_database + 1))
    {
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(detached_databases));

        cluster = d->getCluster();
        att->set_sobject(SQLObject::DATABASE);
        d->setName(sot->mutable_database());
    }
    else
    {
        chassert(0);
    }
    if (cluster.has_value())
    {
        att->mutable_cluster()->set_cluster(cluster.value());
    }
    if (att->sobject() != SQLObject::DATABASE && rg.nextSmallNumber() < 3)
    {
        att->set_as_replicated(rg.nextBool());
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, att->mutable_setting_values());
    }
}

void StatementGenerator::generateDetach(RandomGenerator & rg, Detach * det)
{
    SQLObjectName * sot = det->mutable_object();
    const uint32_t detach_table = 10 * static_cast<uint32_t>(collectionCount<SQLTable>(attached_tables) > 3);
    const uint32_t detach_view = 10 * static_cast<uint32_t>(collectionCount<SQLView>(attached_views) > 3);
    const uint32_t detach_dictionary = 10 * static_cast<uint32_t>(collectionCount<SQLDictionary>(attached_dictionaries) > 3);
    const uint32_t detach_database = 2 * static_cast<uint32_t>(collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3);
    const uint32_t prob_space = detach_table + detach_view + detach_dictionary + detach_database;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    std::optional<String> cluster;

    if (detach_table && nopt < (detach_table + 1))
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        cluster = t.getCluster();
        det->set_sobject(SQLObject::TABLE);
        t.setName(sot->mutable_est(), false);
    }
    else if (detach_view && nopt < (detach_table + detach_view + 1))
    {
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

        cluster = v.getCluster();
        det->set_sobject(SQLObject::TABLE);
        v.setName(sot->mutable_est(), false);
    }
    else if (detach_dictionary && nopt < (detach_table + detach_view + detach_dictionary + 1))
    {
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(attached_dictionaries));

        cluster = d.getCluster();
        det->set_sobject(SQLObject::DICTIONARY);
        d.setName(sot->mutable_est(), false);
    }
    else if (detach_database && nopt < (detach_table + detach_view + detach_dictionary + detach_database + 1))
    {
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        cluster = d->getCluster();
        det->set_sobject(SQLObject::DATABASE);
        d->setName(sot->mutable_database());
    }
    else
    {
        chassert(0);
    }
    if (cluster.has_value())
    {
        det->mutable_cluster()->set_cluster(cluster.value());
    }
    det->set_permanently(det->sobject() != SQLObject::DATABASE && rg.nextSmallNumber() < 4);
    det->set_sync(rg.nextSmallNumber() < 4);
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, det->mutable_setting_values());
    }
}

static const auto has_merge_tree_func = [](const SQLTable & t) { return t.isAttached() && t.isMergeTreeFamily(); };

static const auto has_distributed_table_func = [](const SQLTable & t) { return t.isAttached() && t.isDistributedEngine(); };

static const auto has_refreshable_view_func = [](const SQLView & v) { return v.isAttached() && v.is_refreshable; };

void StatementGenerator::generateNextSystemStatement(RandomGenerator & rg, const bool allow_table_statements, SystemCommand * sc)
{
    const uint32_t has_merge_tree = static_cast<uint32_t>(allow_table_statements && collectionHas<SQLTable>(has_merge_tree_func));
    const uint32_t has_refreshable_view
        = static_cast<uint32_t>(allow_table_statements && collectionHas<SQLView>(has_refreshable_view_func));
    const uint32_t has_distributed_table
        = static_cast<uint32_t>(allow_table_statements && collectionHas<SQLTable>(has_distributed_table_func));

    const uint32_t reload_embedded_dictionaries = 1;
    const uint32_t reload_dictionaries = 3;
    const uint32_t reload_models = 3;
    const uint32_t reload_functions = 3;
    const uint32_t reload_function = 0 * static_cast<uint32_t>(!functions.empty());
    const uint32_t reload_asynchronous_metrics = 3;
    const uint32_t drop_dns_cache = 3;
    const uint32_t drop_mark_cache = 3;
    const uint32_t drop_uncompressed_cache = 9;
    const uint32_t drop_compiled_expression_cache = 3;
    const uint32_t drop_query_cache = 3;
    const uint32_t drop_format_schema_cache = 3;
    const uint32_t flush_logs = 3;
    const uint32_t reload_config = 3;
    const uint32_t reload_users = 3;
    /// For merge trees
    const uint32_t stop_merges = 0 * has_merge_tree;
    const uint32_t start_merges = 0 * has_merge_tree;
    const uint32_t stop_ttl_merges = 8 * has_merge_tree;
    const uint32_t start_ttl_merges = 8 * has_merge_tree;
    const uint32_t stop_moves = 8 * has_merge_tree;
    const uint32_t start_moves = 8 * has_merge_tree;
    const uint32_t wait_loading_parts = 8 * has_merge_tree;
    /// For replicated merge trees
    const uint32_t stop_fetches = 8 * has_merge_tree;
    const uint32_t start_fetches = 8 * has_merge_tree;
    const uint32_t stop_replicated_sends = 8 * has_merge_tree;
    const uint32_t start_replicated_sends = 8 * has_merge_tree;
    const uint32_t stop_replication_queues = 0 * has_merge_tree;
    const uint32_t start_replication_queues = 0 * has_merge_tree;
    const uint32_t stop_pulling_replication_log = 0 * has_merge_tree;
    const uint32_t start_pulling_replication_log = 0 * has_merge_tree;
    const uint32_t sync_replica = 8 * has_merge_tree;
    const uint32_t sync_replicated_database = 8 * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases));
    const uint32_t restart_replica = 8 * has_merge_tree;
    const uint32_t restore_replica = 8 * has_merge_tree;
    const uint32_t restart_replicas = 3;
    const uint32_t drop_filesystem_cache = 3;
    const uint32_t sync_file_cache = 1;
    /// For merge trees
    const uint32_t load_pks = 3;
    const uint32_t load_pk = 8 * has_merge_tree;
    const uint32_t unload_pks = 3;
    const uint32_t unload_pk = 8 * has_merge_tree;
    /// for refreshable views
    const uint32_t refresh_views = 0;
    const uint32_t refresh_view = 8 * has_refreshable_view;
    const uint32_t stop_views = 3;
    const uint32_t stop_view = 8 * has_refreshable_view;
    const uint32_t start_views = 3;
    const uint32_t start_view = 8 * has_refreshable_view;
    const uint32_t cancel_view = 8 * has_refreshable_view;
    const uint32_t wait_view = 8 * has_refreshable_view;
    const uint32_t prewarm_cache = 8 * has_merge_tree;
    const uint32_t prewarm_primary_index_cache = 8 * has_merge_tree;
    const uint32_t drop_connections_cache = 3;
    const uint32_t drop_primary_index_cache = 3;
    const uint32_t drop_index_mark_cache = 3;
    const uint32_t drop_index_uncompressed_cache = 3;
    const uint32_t drop_mmap_cache = 3;
    const uint32_t drop_page_cache = 3;
    const uint32_t drop_schema_cache = 3;
    const uint32_t drop_s3_client_cache = 3;
    const uint32_t flush_async_insert_queue = 3;
    const uint32_t sync_filesystem_cache = 3;
    const uint32_t drop_vector_similarity_index_cache = 3;
    /// for dictionaries
    const uint32_t reload_dictionary = 8 * static_cast<uint32_t>(collectionHas<SQLDictionary>(attached_dictionaries));
    /// for distributed tables
    const uint32_t flush_distributed = 8 * has_distributed_table;
    const uint32_t stop_distributed_sends = 8 * has_distributed_table;
    const uint32_t start_distributed_sends = 8 * has_distributed_table;
    const uint32_t drop_query_condition_cache = 3;
    const uint32_t prob_space = reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
        + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
        + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
        + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
        + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues + stop_pulling_replication_log
        + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica + restore_replica + restart_replicas
        + drop_filesystem_cache + sync_file_cache + load_pks + load_pk + unload_pks + unload_pk + refresh_views + refresh_view + stop_views
        + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache + prewarm_primary_index_cache
        + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache + drop_index_uncompressed_cache + drop_mmap_cache
        + drop_page_cache + drop_schema_cache + drop_s3_client_cache + flush_async_insert_queue + sync_filesystem_cache
        + drop_vector_similarity_index_cache + reload_dictionary + flush_distributed + stop_distributed_sends + start_distributed_sends
        + drop_query_condition_cache;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    std::optional<String> cluster;

    if (reload_embedded_dictionaries && nopt < (reload_embedded_dictionaries + 1))
    {
        sc->set_reload_embedded_dictionaries(true);
    }
    else if (reload_dictionaries && nopt < (reload_embedded_dictionaries + reload_dictionaries + 1))
    {
        sc->set_reload_dictionaries(true);
    }
    else if (reload_models && nopt < (reload_embedded_dictionaries + reload_dictionaries + reload_models + 1))
    {
        sc->set_reload_models(true);
    }
    else if (reload_functions && nopt < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + 1))
    {
        sc->set_reload_functions(true);
    }
    else if (
        reload_function
        && nopt < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function + 1))
    {
        const SQLFunction & f = rg.pickValueRandomlyFromMap(this->functions);

        f.setName(sc->mutable_reload_function());
        cluster = f.getCluster();
    }
    else if (
        reload_asynchronous_metrics
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + 1))
    {
        sc->set_reload_asynchronous_metrics(true);
    }
    else if (
        drop_dns_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + 1))
    {
        sc->set_drop_dns_cache(true);
    }
    else if (
        drop_mark_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + 1))
    {
        sc->set_drop_mark_cache(true);
    }
    else if (
        drop_uncompressed_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + 1))
    {
        sc->set_drop_uncompressed_cache(true);
    }
    else if (
        drop_compiled_expression_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + 1))
    {
        sc->set_drop_compiled_expression_cache(true);
    }
    else if (
        drop_query_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + 1))
    {
        sc->set_drop_query_cache(true);
    }
    else if (
        drop_format_schema_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + 1))
    {
        sc->set_drop_format_schema_cache(rg.nextBool());
    }
    else if (
        flush_logs
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + 1))
    {
        sc->set_flush_logs(true);
    }
    else if (
        reload_config
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + 1))
    {
        sc->set_reload_config(true);
    }
    else if (
        reload_users
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + 1))
    {
        sc->set_reload_users(true);
    }
    else if (
        stop_merges
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_merges());
    }
    else if (
        start_merges
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_merges());
    }
    else if (
        stop_ttl_merges
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_ttl_merges());
    }
    else if (
        start_ttl_merges
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_ttl_merges());
    }
    else if (
        stop_moves
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_moves());
    }
    else if (
        start_moves
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_moves());
    }
    else if (
        wait_loading_parts
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_wait_loading_parts());
    }
    else if (
        stop_fetches
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_fetches());
    }
    else if (
        start_fetches
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_fetches());
    }
    else if (
        stop_replicated_sends
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_replicated_sends());
    }
    else if (
        start_replicated_sends
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_replicated_sends());
    }
    else if (
        stop_replication_queues
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_replication_queues());
    }
    else if (
        start_replication_queues
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_replication_queues());
    }
    else if (
        stop_pulling_replication_log
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_pulling_replication_log());
    }
    else if (
        start_pulling_replication_log
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_pulling_replication_log());
    }
    else if (
        sync_replica
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + 1))
    {
        SyncReplica * srep = sc->mutable_sync_replica();
        std::uniform_int_distribution<uint32_t> sync_range(1, static_cast<uint32_t>(SyncReplica::SyncPolicy_MAX));

        srep->set_policy(static_cast<SyncReplica_SyncPolicy>(sync_range(rg.generator)));
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, srep->mutable_est());
    }
    else if (
        sync_replicated_database
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + 1))
    {
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        d->setName(sc->mutable_sync_replicated_database());
        cluster = d->getCluster();
    }
    else if (
        restart_replica
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_restart_replica());
    }
    else if (
        restore_replica
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_restore_replica());
    }
    else if (
        restart_replicas
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + 1))
    {
        sc->set_restart_replicas(true);
    }
    else if (
        drop_filesystem_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + drop_filesystem_cache + 1))
    {
        sc->set_drop_filesystem_cache(true);
    }
    else if (
        sync_file_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + 1))
    {
        sc->set_sync_file_cache(true);
    }
    else if (
        load_pks
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + 1))
    {
        sc->set_load_pks(true);
    }
    else if (
        load_pk
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_load_pk());
    }
    else if (
        unload_pks
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + 1))
    {
        sc->set_unload_pks(true);
    }
    else if (
        unload_pk
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_unload_pk());
    }
    else if (
        refresh_views
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + 1))
    {
        sc->set_refresh_views(true);
    }
    else if (
        refresh_view
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + 1))
    {
        cluster = setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_refresh_view());
    }
    else if (
        stop_views
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + 1))
    {
        sc->set_stop_views(true);
    }
    else if (
        stop_view
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + 1))
    {
        cluster = setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_stop_view());
    }
    else if (
        start_views
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + 1))
    {
        sc->set_start_views(true);
    }
    else if (
        start_view
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + 1))
    {
        cluster = setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_start_view());
    }
    else if (
        cancel_view
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + 1))
    {
        cluster = setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_cancel_view());
    }
    else if (
        wait_view
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + 1))
    {
        cluster = setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_wait_view());
    }
    else if (
        prewarm_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_prewarm_cache());
    }
    else if (
        prewarm_primary_index_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_prewarm_primary_index_cache());
    }
    else if (
        drop_connections_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + 1))
    {
        sc->set_drop_connections_cache(true);
    }
    else if (
        drop_primary_index_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + 1))
    {
        sc->set_drop_primary_index_cache(true);
    }
    else if (
        drop_index_mark_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache + 1))
    {
        sc->set_drop_index_mark_cache(true);
    }
    else if (
        drop_index_uncompressed_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + 1))
    {
        sc->set_drop_index_uncompressed_cache(true);
    }
    else if (
        drop_mmap_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + 1))
    {
        sc->set_drop_mmap_cache(true);
    }
    else if (
        drop_page_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + 1))
    {
        sc->set_drop_page_cache(true);
    }
    else if (
        drop_schema_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + 1))
    {
        sc->set_drop_schema_cache(true);
    }
    else if (
        drop_s3_client_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache + 1))
    {
        sc->set_drop_s3_client_cache(true);
    }
    else if (
        flush_async_insert_queue
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache
               + flush_async_insert_queue + 1))
    {
        sc->set_flush_async_insert_queue(true);
    }
    else if (
        sync_filesystem_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache
               + flush_async_insert_queue + sync_filesystem_cache + 1))
    {
        sc->set_sync_filesystem_cache(true);
    }
    else if (
        drop_vector_similarity_index_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache
               + flush_async_insert_queue + sync_filesystem_cache + drop_vector_similarity_index_cache + 1))
    {
        sc->set_drop_vector_similarity_index_cache(true);
    }
    else if (
        reload_dictionary
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache
               + flush_async_insert_queue + sync_filesystem_cache + drop_vector_similarity_index_cache + reload_dictionary + 1))
    {
        cluster = setTableSystemStatement<SQLDictionary>(rg, attached_dictionaries, sc->mutable_reload_dictionary());
    }
    else if (
        flush_distributed
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache
               + flush_async_insert_queue + sync_filesystem_cache + drop_vector_similarity_index_cache + reload_dictionary
               + flush_distributed + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_distributed_table_func, sc->mutable_flush_distributed());
    }
    else if (
        stop_distributed_sends
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache
               + flush_async_insert_queue + sync_filesystem_cache + drop_vector_similarity_index_cache + reload_dictionary
               + flush_distributed + stop_distributed_sends + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_distributed_table_func, sc->mutable_stop_distributed_sends());
    }
    else if (
        start_distributed_sends
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache
               + flush_async_insert_queue + sync_filesystem_cache + drop_vector_similarity_index_cache + reload_dictionary
               + flush_distributed + stop_distributed_sends + start_distributed_sends + 1))
    {
        cluster = setTableSystemStatement<SQLTable>(rg, has_distributed_table_func, sc->mutable_start_distributed_sends());
    }
    else if (
        drop_query_condition_cache
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
               + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues
               + stop_pulling_replication_log + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica
               + restore_replica + restart_replicas + sync_file_cache + drop_filesystem_cache + load_pks + load_pk + unload_pks + unload_pk
               + refresh_views + refresh_view + stop_views + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache
               + prewarm_primary_index_cache + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache
               + drop_index_uncompressed_cache + drop_mmap_cache + drop_page_cache + drop_schema_cache + drop_s3_client_cache
               + flush_async_insert_queue + sync_filesystem_cache + drop_vector_similarity_index_cache + reload_dictionary
               + flush_distributed + stop_distributed_sends + start_distributed_sends + drop_query_condition_cache + 1))
    {
        sc->set_drop_query_condition_cache(true);
    }
    else
    {
        chassert(0);
    }

    /// Set cluster option when that's the case
    if (cluster.has_value())
    {
        sc->mutable_cluster()->set_cluster(cluster.value());
    }
    else if (!fc.clusters.empty() && rg.nextSmallNumber() < 4)
    {
        sc->mutable_cluster()->set_cluster(rg.pickRandomly(fc.clusters));
    }
}

static std::optional<String> backupOrRestoreObject(BackupRestoreObject * bro, const SQLObject obj, const SQLBase & b)
{
    bro->set_is_temp(b.is_temp);
    bro->set_sobject(obj);
    return b.getCluster();
}

static void backupOrRestoreSystemTable(BackupRestoreObject * bro, const String & name)
{
    ExprSchemaTable * est = bro->mutable_object()->mutable_est();

    bro->set_sobject(SQLObject::TABLE);
    est->mutable_database()->set_database("system");
    est->mutable_table()->set_table(name);
}

static std::optional<String> backupOrRestoreDatabase(BackupRestoreObject * bro, const std::shared_ptr<SQLDatabase> & d)
{
    bro->set_sobject(SQLObject::DATABASE);
    d->setName(bro->mutable_object()->mutable_database());
    return d->getCluster();
}

void StatementGenerator::generateNextBackup(RandomGenerator & rg, BackupRestore * br)
{
    const uint32_t backup_table = 10 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t backup_system_table = 3 * static_cast<uint32_t>(!systemTables.empty());
    const uint32_t backup_view = 10 * static_cast<uint32_t>(collectionHas<SQLView>(attached_views));
    const uint32_t backup_dictionary = 10 * static_cast<uint32_t>(collectionHas<SQLDictionary>(attached_dictionaries));
    const uint32_t backup_database = 10 * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases));
    const uint32_t everything = 3;
    const uint32_t prob_space = backup_table + backup_system_table + backup_view + backup_dictionary + backup_database + everything;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    BackupRestoreElement * bre = br->mutable_backup_element();
    std::optional<String> cluster;

    br->set_command(BackupRestore_BackupCommand_BACKUP);
    if (backup_table && nopt < (backup_table + 1))
    {
        BackupRestoreObject * bro = bre->mutable_bobject();
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));
        const String dname = t.db ? ("d" + std::to_string(t.db->dname)) : "";
        const String tname = "t" + std::to_string(t.tname);
        const bool table_has_partitions = t.isMergeTreeFamily() && fc.tableHasPartitions(false, dname, tname);

        t.setName(bro->mutable_object()->mutable_est(), false);
        cluster = backupOrRestoreObject(bro, SQLObject::TABLE, t);
        if (table_has_partitions && rg.nextSmallNumber() < 4)
        {
            bro->add_partitions()->set_partition_id(fc.tableGetRandomPartitionOrPart(false, true, dname, tname));
        }
    }
    else if (backup_system_table && nopt < (backup_table + backup_system_table + 1))
    {
        backupOrRestoreSystemTable(bre->mutable_bobject(), rg.pickRandomly(systemTables));
    }
    else if (backup_view && nopt < (backup_table + backup_system_table + backup_view + 1))
    {
        BackupRestoreObject * bro = bre->mutable_bobject();
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

        v.setName(bro->mutable_object()->mutable_est(), false);
        cluster = backupOrRestoreObject(bro, SQLObject::VIEW, v);
    }
    else if (backup_dictionary && nopt < (backup_table + backup_system_table + backup_view + backup_dictionary + 1))
    {
        BackupRestoreObject * bro = bre->mutable_bobject();
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(attached_dictionaries));

        d.setName(bro->mutable_object()->mutable_est(), false);
        cluster = backupOrRestoreObject(bro, SQLObject::DICTIONARY, d);
    }
    else if (backup_database && nopt < (backup_table + backup_system_table + backup_view + backup_dictionary + backup_database + 1))
    {
        cluster = backupOrRestoreDatabase(
            bre->mutable_bobject(), rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases)));
    }
    else if (everything && nopt < (backup_table + backup_system_table + backup_view + backup_dictionary + backup_database + everything + 1))
    {
        bre->set_all(true);
    }
    else
    {
        chassert(0);
    }
    if (cluster.has_value())
    {
        br->mutable_cluster()->set_cluster(cluster.value());
    }

    const uint32_t out_to_disk = 10 * static_cast<uint32_t>(!fc.disks.empty());
    const uint32_t out_to_file = 10;
    const uint32_t out_to_s3 = 10 * static_cast<uint32_t>(connections.hasMinIOConnection());
    const uint32_t out_to_azure = 10 * static_cast<uint32_t>(connections.hasAzuriteConnection());
    const uint32_t out_to_memory = 5;
    const uint32_t out_to_null = 3;
    const uint32_t prob_space2 = out_to_disk + out_to_file + out_to_s3 + out_to_azure + out_to_memory + out_to_null;
    std::uniform_int_distribution<uint32_t> next_dist2(1, prob_space2);
    const uint32_t nopt2 = next_dist2(rg.generator);
    String backup_file = "backup";
    BackupRestore_BackupOutput outf = BackupRestore_BackupOutput_Null;

    br->set_backup_number(backup_counter++);
    /// Set backup file
    if (nopt2 < (out_to_disk + out_to_file + out_to_s3 + out_to_memory + 1))
    {
        backup_file += std::to_string(br->backup_number());
    }
    if (nopt2 < (out_to_disk + out_to_file + out_to_s3 + 1) && rg.nextBool())
    {
        static const DB::Strings & backupFormats = {"tar", "zip", "tzst", "tgz"};
        const String & nsuffix = rg.pickRandomly(backupFormats);

        backup_file += ".";
        backup_file += nsuffix;
        if (nsuffix == "tar" && rg.nextBool())
        {
            static const DB::Strings & tarSuffixes = {"gz", "bz2", "lzma", "zst", "xz"};

            backup_file += ".";
            backup_file += rg.pickRandomly(tarSuffixes);
        }
    }
    if (out_to_disk && (nopt2 < out_to_disk + 1))
    {
        outf = BackupRestore_BackupOutput_Disk;
        br->add_out_params(rg.pickRandomly(fc.disks));
        br->add_out_params(std::move(backup_file));
    }
    else if (out_to_file && (nopt2 < out_to_disk + out_to_file + 1))
    {
        outf = BackupRestore_BackupOutput_File;
        br->add_out_params((fc.server_file_path / std::move(backup_file)).generic_string());
    }
    else if (out_to_s3 && (nopt2 < out_to_disk + out_to_file + out_to_s3 + 1))
    {
        outf = BackupRestore_BackupOutput_S3;
        connections.setBackupDetails(IntegrationCall::MinIO, backup_file, br);
    }
    else if (out_to_azure && (nopt2 < out_to_disk + out_to_file + out_to_s3 + out_to_azure + 1))
    {
        outf = BackupRestore_BackupOutput_AzureBlobStorage;
        connections.setBackupDetails(IntegrationCall::Azurite, backup_file, br);
    }
    else if (out_to_memory && nopt2 < (out_to_disk + out_to_file + out_to_s3 + out_to_azure + out_to_memory + 1))
    {
        outf = BackupRestore_BackupOutput_Memory;
        br->add_out_params(std::move(backup_file));
    }
    else if (out_to_null && nopt2 < (out_to_disk + out_to_file + out_to_s3 + out_to_azure + out_to_memory + out_to_null + 1))
    {
        outf = BackupRestore_BackupOutput_Null;
    }
    else
    {
        chassert(0);
    }
    br->set_out(outf);
    if (rg.nextBool())
    {
        /// Most of the times, use formats that can be read later
        br->set_outformat(
            rg.nextBool() ? rg.pickRandomly(outIn)
                          : static_cast<OutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(OutFormat_MAX)) + 1));
    }
}

void StatementGenerator::generateNextRestore(RandomGenerator & rg, BackupRestore * br)
{
    const CatalogBackup & backup = rg.pickValueRandomlyFromMap(backups);
    BackupRestoreElement * bre = br->mutable_backup_element();
    std::optional<String> cluster;

    br->set_command(BackupRestore_BackupCommand_RESTORE);
    if (backup.everything)
    {
        bre->set_all(true);
    }
    else
    {
        const uint32_t restore_table = 10 * static_cast<uint32_t>(!backup.tables.empty());
        const uint32_t restore_system_table = 3 * static_cast<uint32_t>(backup.system_table.has_value());
        const uint32_t restore_view = 10 * static_cast<uint32_t>(!backup.views.empty());
        const uint32_t restore_dictionary = 10 * static_cast<uint32_t>(!backup.dictionaries.empty());
        const uint32_t restore_database = 10 * static_cast<uint32_t>(!backup.databases.empty());
        const uint32_t prob_space = restore_table + restore_system_table + restore_view + restore_dictionary + restore_database;
        std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
        const uint32_t nopt = next_dist(rg.generator);

        if (restore_table && (nopt < restore_table + 1))
        {
            BackupRestoreObject * bro = bre->mutable_bobject();
            const SQLTable & t = rg.pickValueRandomlyFromMap(backup.tables);

            t.setName(bro->mutable_object()->mutable_est(), false);
            cluster = backupOrRestoreObject(bro, SQLObject::TABLE, t);
            if (backup.partition_id.has_value() && rg.nextSmallNumber() < 4)
            {
                bro->add_partitions()->set_partition_id(backup.partition_id.value());
            }
        }
        else if (restore_system_table && (nopt < restore_table + restore_system_table + 1))
        {
            backupOrRestoreSystemTable(bre->mutable_bobject(), backup.system_table.value());
        }
        else if (restore_view && nopt < (restore_table + restore_system_table + restore_view + 1))
        {
            BackupRestoreObject * bro = bre->mutable_bobject();
            const SQLView & v = rg.pickValueRandomlyFromMap(backup.views);

            v.setName(bro->mutable_object()->mutable_est(), false);
            cluster = backupOrRestoreObject(bro, SQLObject::VIEW, v);
        }
        else if (restore_dictionary && nopt < (restore_table + restore_system_table + restore_view + restore_dictionary + 1))
        {
            BackupRestoreObject * bro = bre->mutable_bobject();
            const SQLDictionary & d = rg.pickValueRandomlyFromMap(backup.dictionaries);

            d.setName(bro->mutable_object()->mutable_est(), false);
            cluster = backupOrRestoreObject(bro, SQLObject::DICTIONARY, d);
        }
        else if (
            restore_database && nopt < (restore_table + restore_system_table + restore_view + restore_dictionary + restore_database + 1))
        {
            cluster = backupOrRestoreDatabase(bre->mutable_bobject(), rg.pickValueRandomlyFromMap(backup.databases));
        }
        else
        {
            chassert(0);
        }
    }

    if (cluster.has_value())
    {
        br->mutable_cluster()->set_cluster(cluster.value());
    }
    br->set_out(backup.outf);
    for (const auto & entry : backup.out_params)
    {
        br->add_out_params(entry);
    }
    if (backup.out_format.has_value())
    {
        br->set_informat(
            (outIn.find(backup.out_format.value()) != outIn.end()) && rg.nextBool()
                ? outIn.at(backup.out_format.value())
                : static_cast<InFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(InFormat_MAX)) + 1));
    }
    br->set_backup_number(backup.backup_num);
}

void StatementGenerator::generateNextBackupOrRestore(RandomGenerator & rg, BackupRestore * br)
{
    SettingValues * vals = nullptr;
    const bool isBackup = backups.empty() || rg.nextBool();

    if (isBackup)
    {
        generateNextBackup(rg, br);
    }
    else
    {
        generateNextRestore(rg, br);
    }
    if (rg.nextSmallNumber() < 4)
    {
        vals = vals ? vals : br->mutable_setting_values();
        generateSettingValues(rg, isBackup ? backupSettings : restoreSettings, vals);
    }
    if (isBackup && !backups.empty() && rg.nextBool())
    {
        /// Do an incremental backup
        String info;
        vals = vals ? vals : br->mutable_setting_values();
        SetValue * sv = vals->has_set_value() ? vals->add_other_values() : vals->mutable_set_value();
        const CatalogBackup & backup = rg.pickValueRandomlyFromMap(backups);

        sv->set_property("base_backup");
        info += BackupRestore_BackupOutput_Name(backup.outf);
        info += "(";
        for (size_t i = 0; i < backup.out_params.size(); i++)
        {
            if (i != 0)
            {
                info += ", ";
            }
            info += "'";
            info += backup.out_params[i];
            info += "'";
        }
        info += ")";
        sv->set_value(std::move(info));
    }
    if (rg.nextSmallNumber() < 4)
    {
        vals = vals ? vals : br->mutable_setting_values();
        generateSettingValues(rg, formatSettings, vals);
    }
    br->set_async(rg.nextSmallNumber() < 4);
}

void StatementGenerator::generateNextRename(RandomGenerator & rg, Rename * ren)
{
    SQLObjectName * oldn = ren->mutable_old_object();
    SQLObjectName * newn = ren->mutable_new_object();
    const bool has_database = collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases);
    const uint32_t rename_table = 10 * static_cast<uint32_t>(collectionHas<SQLTable>(exchange_table_lambda));
    const uint32_t rename_view = 10 * static_cast<uint32_t>(collectionHas<SQLView>(attached_views));
    const uint32_t rename_dictionary = 10 * static_cast<uint32_t>(collectionHas<SQLDictionary>(attached_dictionaries));
    const uint32_t rename_database = 10 * static_cast<uint32_t>(has_database);
    const uint32_t prob_space = rename_table + rename_view + rename_dictionary + rename_database;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    std::optional<String> cluster;

    if (rename_table && nopt < (rename_table + 1))
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(exchange_table_lambda));

        cluster = t.getCluster();
        ren->set_sobject(SQLObject::TABLE);
        t.setName(oldn->mutable_est(), true);
        SQLTable::setName(newn->mutable_est(), true, t.db, this->table_counter++);
    }
    else if (rename_view && nopt < (rename_table + rename_view + 1))
    {
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

        cluster = v.getCluster();
        ren->set_sobject(SQLObject::TABLE);
        v.setName(oldn->mutable_est(), true);
        SQLView::setName(newn->mutable_est(), true, v.db, this->table_counter++);
    }
    else if (rename_dictionary && nopt < (rename_table + rename_view + rename_dictionary + 1))
    {
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(attached_dictionaries));

        cluster = d.getCluster();
        ren->set_sobject(SQLObject::DICTIONARY);
        d.setName(oldn->mutable_est(), true);
        SQLDictionary::setName(newn->mutable_est(), true, d.db, this->table_counter++);
    }
    else if (rename_database && nopt < (rename_table + rename_view + rename_dictionary + rename_database + 1))
    {
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        cluster = d->getCluster();
        ren->set_sobject(SQLObject::DATABASE);
        d->setName(oldn->mutable_database());
        SQLDatabase::setName(newn->mutable_database(), this->database_counter++);
    }
    else
    {
        chassert(0);
    }
    if (newn->has_est() && rg.nextBool())
    {
        /// Change database
        Database * db = const_cast<ExprSchemaTable &>(newn->est()).mutable_database();

        if (!has_database || rg.nextSmallNumber() < 4)
        {
            db->set_database("default");
        }
        else
        {
            const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

            d->setName(db);
        }
    }
    if (cluster.has_value())
    {
        ren->mutable_cluster()->set_cluster(cluster.value());
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, ren->mutable_setting_values());
    }
}

void StatementGenerator::generateNextQuery(RandomGenerator & rg, const bool in_parallel, SQLQueryInner * sq)
{
    const bool has_databases = collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases);
    const bool has_tables = collectionHas<SQLTable>(attached_tables);
    const bool has_views = collectionHas<SQLView>(attached_views);
    const bool has_dictionaries = collectionHas<SQLDictionary>(attached_dictionaries);

    const uint32_t create_table = 6 * static_cast<uint32_t>(static_cast<uint32_t>(tables.size()) < this->fc.max_tables);
    const uint32_t create_view = 10 * static_cast<uint32_t>(static_cast<uint32_t>(views.size()) < this->fc.max_views);
    const uint32_t drop = 2
        * static_cast<uint32_t>(
                              !in_parallel
                              && (collectionCount<SQLTable>(attached_tables) > 3 || collectionCount<SQLView>(attached_views) > 3
                                  || collectionCount<SQLDictionary>(attached_dictionaries) > 3
                                  || collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3 || functions.size() > 3));
    const uint32_t insert = 180 * static_cast<uint32_t>(has_tables);
    const uint32_t light_delete = 6 * static_cast<uint32_t>(has_tables);
    const uint32_t truncate = 2 * static_cast<uint32_t>(has_databases || has_tables);
    const uint32_t optimize_table = 2 * static_cast<uint32_t>(collectionHas<SQLTable>(optimize_table_lambda));
    const uint32_t check_table = 2 * static_cast<uint32_t>(has_tables);
    const uint32_t desc_table = 2;
    const uint32_t exchange = 1
        * static_cast<uint32_t>(!in_parallel
                                && (collectionCount<SQLTable>(exchange_table_lambda) > 1 || collectionCount<SQLView>(attached_views) > 1
                                    || collectionCount<SQLDictionary>(attached_dictionaries) > 1));
    const uint32_t alter = 6
        * static_cast<uint32_t>(
                               collectionHas<SQLTable>(alter_table_lambda) || collectionHas<SQLView>(attached_views)
                               || collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases));
    const uint32_t set_values = 5;
    const uint32_t attach = 2
        * static_cast<uint32_t>(!in_parallel
                                && (collectionHas<SQLTable>(detached_tables) || collectionHas<SQLView>(detached_views)
                                    || collectionHas<SQLDictionary>(detached_dictionaries)
                                    || collectionHas<std::shared_ptr<SQLDatabase>>(detached_databases)));
    const uint32_t detach = 2
        * static_cast<uint32_t>(!in_parallel
                                && (collectionCount<SQLTable>(attached_tables) > 3 || collectionCount<SQLView>(attached_views) > 3
                                    || collectionCount<SQLDictionary>(attached_dictionaries) > 3
                                    || collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3));
    const uint32_t create_database = 2 * static_cast<uint32_t>(static_cast<uint32_t>(databases.size()) < this->fc.max_databases);
    const uint32_t create_function = 5 * static_cast<uint32_t>(static_cast<uint32_t>(functions.size()) < this->fc.max_functions);
    const uint32_t system_stmt = 1;
    const uint32_t backup_or_restore = 1;
    const uint32_t create_dictionary = 10 * static_cast<uint32_t>(static_cast<uint32_t>(dictionaries.size()) < this->fc.max_dictionaries);
    const uint32_t rename = 1
        * static_cast<uint32_t>(!in_parallel
                                && (collectionHas<SQLTable>(exchange_table_lambda) || has_views || has_dictionaries || has_databases));
    const uint32_t select_query = 800 * static_cast<uint32_t>(!in_parallel);
    const uint32_t prob_space = create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table
        + desc_table + exchange + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
        + create_dictionary + rename + select_query;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    chassert(this->ids.empty());
    if (create_table && nopt < (create_table + 1))
    {
        generateNextCreateTable(rg, in_parallel, sq->mutable_create_table());
    }
    else if (create_view && nopt < (create_table + create_view + 1))
    {
        generateNextCreateView(rg, sq->mutable_create_view());
    }
    else if (drop && nopt < (create_table + create_view + drop + 1))
    {
        generateNextDrop(rg, sq->mutable_drop());
    }
    else if (insert && nopt < (create_table + create_view + drop + insert + 1))
    {
        generateNextInsert(rg, in_parallel, sq->mutable_insert());
    }
    else if (light_delete && nopt < (create_table + create_view + drop + insert + light_delete + 1))
    {
        generateNextDelete(rg, sq->mutable_del());
    }
    else if (truncate && nopt < (create_table + create_view + drop + insert + light_delete + truncate + 1))
    {
        generateNextTruncate(rg, sq->mutable_trunc());
    }
    else if (optimize_table && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + 1))
    {
        generateNextOptimizeTable(rg, sq->mutable_opt());
    }
    else if (
        check_table && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + 1))
    {
        generateNextCheckTable(rg, sq->mutable_check());
    }
    else if (
        desc_table
        && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + 1))
    {
        generateNextDescTable(rg, sq->mutable_desc());
    }
    else if (
        exchange
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + 1))
    {
        generateNextExchange(rg, sq->mutable_exchange());
    }
    else if (
        alter
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + 1))
    {
        generateAlter(rg, sq->mutable_alter());
    }
    else if (
        set_values
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + 1))
    {
        generateSettingValues(rg, serverSettings, sq->mutable_setting_values());
    }
    else if (
        attach
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + 1))
    {
        generateAttach(rg, sq->mutable_attach());
    }
    else if (
        detach
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + 1))
    {
        generateDetach(rg, sq->mutable_detach());
    }
    else if (
        create_database
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + 1))
    {
        generateNextCreateDatabase(rg, sq->mutable_create_database());
    }
    else if (
        create_function
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + 1))
    {
        generateNextCreateFunction(rg, sq->mutable_create_function());
    }
    else if (
        system_stmt
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + 1))
    {
        generateNextSystemStatement(rg, true, sq->mutable_system_cmd());
    }
    else if (
        backup_or_restore
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore + 1))
    {
        generateNextBackupOrRestore(rg, sq->mutable_backup_restore());
    }
    else if (
        create_dictionary
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
               + create_dictionary + 1))
    {
        generateNextCreateDictionary(rg, sq->mutable_create_dictionary());
    }
    else if (
        rename
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
               + create_dictionary + rename + 1))
    {
        generateNextRename(rg, sq->mutable_rename());
    }
    else if (
        select_query
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
               + create_dictionary + rename + select_query + 1))
    {
        generateTopSelect(rg, false, std::numeric_limits<uint32_t>::max(), sq->mutable_select());
    }
    else
    {
        chassert(0);
    }
}

struct ExplainOptValues
{
    const ExplainOption_ExplainOpt opt;
    const std::function<uint32_t(RandomGenerator &)> random_func;

    ExplainOptValues(const ExplainOption_ExplainOpt & e, const std::function<uint32_t(RandomGenerator &)> & rf)
        : opt(e)
        , random_func(rf)
    {
    }
};

static const std::function<uint32_t(RandomGenerator &)> trueOrFalseInt = [](RandomGenerator & rg) { return rg.nextBool() ? 1 : 0; };

static const std::vector<ExplainOptValues> explainSettings{
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_graph, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_optimize, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_oneline, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_dump_ast, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_dump_passes, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_dump_tree, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_run_passes, trueOrFalseInt),
    ExplainOptValues(
        ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_passes, [](RandomGenerator & rg) { return rg.randomInt<uint32_t>(0, 32); }),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_distributed, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_sorting, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_json, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_description, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_indexes, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_keep_logical_steps, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_actions, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_header, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_compact, trueOrFalseInt)};

void StatementGenerator::generateNextExplain(RandomGenerator & rg, bool in_parallel, ExplainQuery * eq)
{
    std::optional<ExplainQuery_ExplainValues> val;

    eq->set_is_explain(true);
    if (rg.nextSmallNumber() < 9)
    {
        std::uniform_int_distribution<uint32_t> exp_range(1, static_cast<uint32_t>(ExplainQuery::ExplainValues_MAX));

        val = std::optional<ExplainQuery_ExplainValues>(static_cast<ExplainQuery_ExplainValues>(exp_range(rg.generator)));
        eq->set_expl(val.value());
    }
    if (rg.nextBool())
    {
        chassert(this->ids.empty());
        if (val.has_value())
        {
            switch (val.value())
            {
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_AST:
                    this->ids.emplace_back(0);
                    this->ids.emplace_back(1);
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_SYNTAX:
                    this->ids.emplace_back(2);
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE:
                    this->ids.emplace_back(3);
                    this->ids.emplace_back(4);
                    this->ids.emplace_back(5);
                    this->ids.emplace_back(6);
                    this->ids.emplace_back(7);
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PLAN:
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_ESTIMATE:
                    this->ids.emplace_back(1);
                    this->ids.emplace_back(8);
                    this->ids.emplace_back(9);
                    this->ids.emplace_back(10);
                    this->ids.emplace_back(11);
                    this->ids.emplace_back(12);
                    this->ids.emplace_back(13);
                    this->ids.emplace_back(14);
                    this->ids.emplace_back(15);
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PIPELINE:
                    this->ids.emplace_back(0);
                    this->ids.emplace_back(15);
                    this->ids.emplace_back(16);
                    break;
                default:
                    break;
            }
        }
        else
        {
            this->ids.emplace_back(1);
            this->ids.emplace_back(9);
            this->ids.emplace_back(10);
            this->ids.emplace_back(11);
            this->ids.emplace_back(12);
            this->ids.emplace_back(13);
            this->ids.emplace_back(14);
            this->ids.emplace_back(15);
        }
        if (!this->ids.empty())
        {
            const size_t noptions = (static_cast<size_t>(rg.nextRandomUInt32()) % this->ids.size()) + 1;
            std::shuffle(ids.begin(), ids.end(), rg.generator);

            for (size_t i = 0; i < noptions; i++)
            {
                const auto & nopt = explainSettings[this->ids[i]];
                ExplainOption * eopt = eq->add_opts();

                eopt->set_opt(nopt.opt);
                eopt->set_val(nopt.random_func(rg));
            }
            this->ids.clear();
        }
    }
    generateNextQuery(rg, in_parallel, eq->mutable_inner_query());
}

void StatementGenerator::generateNextStatement(RandomGenerator & rg, SQLQuery & sq)
{
    const uint32_t nqueries = rg.nextMediumNumber() < 96 ? 1 : (rg.nextMediumNumber() % 4) + 1;
    const uint32_t start_transaction = 2 * static_cast<uint32_t>(nqueries == 1 && !this->in_transaction);
    const uint32_t commit = 50 * static_cast<uint32_t>(nqueries == 1 && this->in_transaction);
    const uint32_t explain_query = 10;
    const uint32_t run_query = 120;
    const uint32_t prob_space = start_transaction + commit + explain_query + run_query;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);

    for (uint32_t i = 0; i < nqueries; i++)
    {
        const uint32_t nopt = next_dist(rg.generator);
        SingleSQLQuery * ssq = i == 0 ? sq.mutable_single_query() : sq.add_parallel_queries();

        this->aliases_counter = 0;
        chassert(this->levels.empty());
        if (start_transaction && nopt < (start_transaction + 1))
        {
            ssq->set_start_trans(true);
        }
        else if (commit && nopt < (start_transaction + commit + 1))
        {
            if (rg.nextSmallNumber() < 7)
            {
                ssq->set_commit_trans(true);
            }
            else
            {
                ssq->set_rollback_trans(true);
            }
        }
        else if (explain_query && nopt < (start_transaction + commit + explain_query + 1))
        {
            generateNextExplain(rg, nqueries > 1, ssq->mutable_explain());
        }
        else if (run_query)
        {
            generateNextQuery(rg, nqueries > 1, ssq->mutable_explain()->mutable_inner_query());
        }
        else
        {
            chassert(0);
        }
    }
}

void StatementGenerator::dropTable(const bool staged, bool drop_peer, const uint32_t tname)
{
    auto & map_to_delete = staged ? this->staged_tables : this->tables;

    if (map_to_delete.find(tname) != map_to_delete.end())
    {
        if (drop_peer)
        {
            connections.dropPeerTableOnRemote(map_to_delete[tname]);
        }
        map_to_delete.erase(tname);
    }
}

void StatementGenerator::dropDatabase(const uint32_t dname)
{
    for (auto it = this->tables.cbegin(), next_it = it; it != this->tables.cend(); it = next_it)
    {
        ++next_it;
        if (it->second.db && it->second.db->dname == dname)
        {
            dropTable(false, true, it->first);
        }
    }
    for (auto it = this->views.cbegin(), next_it = it; it != this->views.cend(); it = next_it)
    {
        ++next_it;
        if (it->second.db && it->second.db->dname == dname)
        {
            this->views.erase(it);
        }
    }
    for (auto it = this->dictionaries.cbegin(), next_it = it; it != this->dictionaries.cend(); it = next_it)
    {
        ++next_it;
        if (it->second.db && it->second.db->dname == dname)
        {
            this->dictionaries.erase(it);
        }
    }
    this->databases.erase(dname);
}

template <typename T>
void StatementGenerator::exchangeObjects(const uint32_t tname1, const uint32_t tname2)
{
    auto & container = const_cast<std::unordered_map<uint32_t, T> &>(getNextCollection<T>());
    T obj1 = std::move(container.at(tname1));
    T obj2 = std::move(container.at(tname2));
    auto db_tmp = obj1.db;

    obj1.tname = tname2;
    obj1.db = obj2.db;
    obj2.tname = tname1;
    obj2.db = db_tmp;
    container[tname2] = std::move(obj1);
    container[tname1] = std::move(obj2);
}

template <typename T>
void StatementGenerator::renameObjects(const uint32_t old_tname, const uint32_t new_tname, const std::optional<uint32_t> & new_db)
{
    auto & container = const_cast<std::unordered_map<uint32_t, T> &>(getNextCollection<T>());
    T obj = std::move(container.at(old_tname));

    if constexpr (std::is_same_v<T, std::shared_ptr<SQLDatabase>>)
    {
        obj->dname = new_tname;
        UNUSED(new_db);
    }
    else
    {
        obj.tname = new_tname;
        obj.db = new_db.has_value() ? this->databases.at(new_db.value()) : nullptr;
    }
    container[new_tname] = std::move(obj);
    container.erase(old_tname);
}

template <typename T>
void StatementGenerator::attachOrDetachObject(const uint32_t tname, const DetachStatus status)
{
    auto & container = const_cast<std::unordered_map<uint32_t, T> &>(getNextCollection<T>());
    T & obj = container.at(tname);

    if constexpr (std::is_same_v<T, std::shared_ptr<SQLDatabase>>)
    {
        obj->attached = status;
        for (auto & [_, table] : this->tables)
        {
            if (table.db && table.db->dname == tname)
            {
                table.attached = std::max(table.attached, status);
            }
        }
        for (auto & [_, view] : this->views)
        {
            if (view.db && view.db->dname == tname)
            {
                view.attached = std::max(view.attached, status);
            }
        }
        for (auto & [_, dictionary] : this->dictionaries)
        {
            if (dictionary.db && dictionary.db->dname == tname)
            {
                dictionary.attached = std::max(dictionary.attached, status);
            }
        }
    }
    else
    {
        obj.attached = status;
    }
}

void StatementGenerator::updateGeneratorFromSingleQuery(const SingleSQLQuery & ssq, ExternalIntegrations & ei, bool success)
{
    const SQLQueryInner & query = ssq.explain().inner_query();

    success &= (!ei.getRequiresExternalCallCheck() || ei.getNextExternalCallSucceeded());

    if (ssq.has_explain() && query.has_create_table())
    {
        const uint32_t tname = static_cast<uint32_t>(std::stoul(query.create_table().est().table().table().substr(1)));

        if (!ssq.explain().is_explain() && success)
        {
            if (query.create_table().create_opt() == CreateReplaceOption::Replace)
            {
                dropTable(false, true, tname);
            }
            this->tables[tname] = std::move(this->staged_tables[tname]);
        }
        dropTable(true, !success, tname);
    }
    else if (ssq.has_explain() && query.has_create_view())
    {
        const uint32_t tname = static_cast<uint32_t>(std::stoul(query.create_view().est().table().table().substr(1)));

        if (!ssq.explain().is_explain() && success)
        {
            if (query.create_view().create_opt() == CreateReplaceOption::Replace)
            {
                this->views.erase(tname);
            }
            this->views[tname] = std::move(this->staged_views[tname]);
        }
        this->staged_views.erase(tname);
    }
    else if (ssq.has_explain() && query.has_create_dictionary())
    {
        const uint32_t dname = static_cast<uint32_t>(std::stoul(query.create_dictionary().est().table().table().substr(1)));

        if (!ssq.explain().is_explain() && success)
        {
            if (query.create_view().create_opt() == CreateReplaceOption::Replace)
            {
                this->dictionaries.erase(dname);
            }
            this->dictionaries[dname] = std::move(this->staged_dictionaries[dname]);
        }
        this->staged_dictionaries.erase(dname);
    }
    else if (ssq.has_explain() && !ssq.explain().is_explain() && query.has_drop() && success)
    {
        const Drop & drp = query.drop();
        const bool istable = drp.object().has_est() && drp.object().est().table().table()[0] == 't';
        const bool isview = drp.object().has_est() && drp.object().est().table().table()[0] == 'v';
        const bool isdictionary = drp.object().has_est() && drp.object().est().table().table()[0] == 'd';
        const bool isdatabase = drp.object().has_database();
        const bool isfunction = drp.object().has_function();

        if (istable)
        {
            dropTable(false, true, static_cast<uint32_t>(std::stoul(drp.object().est().table().table().substr(1))));
        }
        else if (isview)
        {
            this->views.erase(static_cast<uint32_t>(std::stoul(drp.object().est().table().table().substr(1))));
        }
        else if (isdictionary)
        {
            this->dictionaries.erase(static_cast<uint32_t>(std::stoul(drp.object().est().table().table().substr(1))));
        }
        else if (isdatabase)
        {
            dropDatabase(static_cast<uint32_t>(std::stoul(drp.object().database().database().substr(1))));
        }
        else if (isfunction)
        {
            this->functions.erase(static_cast<uint32_t>(std::stoul(drp.object().function().function().substr(1))));
        }
    }
    else if (ssq.has_explain() && !ssq.explain().is_explain() && query.has_exchange() && success)
    {
        const Exchange & ex = query.exchange();
        const SQLObjectName & obj1 = ex.object1();
        const bool istable = obj1.has_est() && obj1.est().table().table()[0] == 't';
        const bool isview = obj1.has_est() && obj1.est().table().table()[0] == 'v';
        const bool isdictionary = obj1.has_est() && obj1.est().table().table()[0] == 'd';
        const uint32_t tname1 = static_cast<uint32_t>(std::stoul(obj1.est().table().table().substr(1)));
        const uint32_t tname2 = static_cast<uint32_t>(std::stoul(query.exchange().object2().est().table().table().substr(1)));

        if (istable)
        {
            this->exchangeObjects<SQLTable>(tname1, tname2);
        }
        else if (isview)
        {
            this->exchangeObjects<SQLView>(tname1, tname2);
        }
        else if (isdictionary)
        {
            this->exchangeObjects<SQLDictionary>(tname1, tname2);
        }
    }
    else if (ssq.has_explain() && !ssq.explain().is_explain() && query.has_rename() && success)
    {
        const Rename & ren = query.rename();
        const SQLObjectName & oobj = ren.old_object();
        const SQLObjectName & nobj = ren.new_object();
        const bool istable = oobj.has_est() && oobj.est().table().table()[0] == 't';
        const bool isview = oobj.has_est() && oobj.est().table().table()[0] == 'v';
        const bool isdictionary = oobj.has_est() && oobj.est().table().table()[0] == 'd';
        const bool isdatabase = oobj.has_database();
        const uint32_t old_tname
            = static_cast<uint32_t>(std::stoul(isdatabase ? oobj.database().database().substr(1) : oobj.est().table().table().substr(1)));
        const uint32_t new_tname
            = static_cast<uint32_t>(std::stoul(isdatabase ? nobj.database().database().substr(1) : nobj.est().table().table().substr(1)));
        std::optional<uint32_t> new_db;

        if (!isdatabase && nobj.est().database().database() != "default")
        {
            new_db = static_cast<uint32_t>(std::stoul(nobj.est().database().database().substr(1)));
        }
        if (istable)
        {
            this->renameObjects<SQLTable>(old_tname, new_tname, new_db);
        }
        else if (isview)
        {
            this->renameObjects<SQLView>(old_tname, new_tname, new_db);
        }
        else if (isdictionary)
        {
            this->renameObjects<SQLDictionary>(old_tname, new_tname, new_db);
        }
        else if (isdatabase)
        {
            this->renameObjects<std::shared_ptr<SQLDatabase>>(old_tname, new_tname, new_db);
        }
    }
    else if (ssq.has_explain() && !ssq.explain().is_explain() && query.has_alter())
    {
        const Alter & at = query.alter();
        const bool istable = at.object().has_est() && at.object().est().table().table()[0] == 't';
        const bool isview = at.object().has_est() && at.object().est().table().table()[0] == 'v';

        if (isview)
        {
            SQLView & v = this->views[static_cast<uint32_t>(std::stoul(at.object().est().table().table().substr(1)))];

            for (int i = 0; i < at.other_alters_size() + 1; i++)
            {
                const AlterItem & ati = i == 0 ? at.alter() : at.other_alters(i - 1);

                if (success && ati.has_add_column() && !v.has_with_cols)
                {
                    v.cols.clear();
                    for (uint32_t j = 0; j < v.staged_ncols; j++)
                    {
                        v.cols.insert(j);
                    }
                }
            }
        }
        else if (istable)
        {
            SQLTable & t = this->tables[static_cast<uint32_t>(std::stoul(at.object().est().table().table().substr(1)))];

            for (int i = 0; i < at.other_alters_size() + 1; i++)
            {
                const AlterItem & ati = i == 0 ? at.alter() : at.other_alters(i - 1);

                chassert(!ati.has_modify_query() && !ati.has_refresh());
                if (ati.has_add_column())
                {
                    const uint32_t cname = static_cast<uint32_t>(std::stoul(ati.add_column().new_col().col().column().substr(1)));

                    if (success)
                    {
                        t.cols[cname] = std::move(t.staged_cols[cname]);
                    }
                    t.staged_cols.erase(cname);
                }
                else if (ati.has_drop_column() && success)
                {
                    const ColumnPath & path = ati.drop_column();
                    const uint32_t cname = static_cast<uint32_t>(std::stoul(path.col().column().substr(1)));

                    if (path.sub_cols_size() == 0)
                    {
                        t.cols.erase(cname);
                    }
                    else
                    {
                        SQLColumn & col = t.cols.at(cname);
                        NestedType * ntp;

                        chassert(path.sub_cols_size() == 1);
                        if ((ntp = dynamic_cast<NestedType *>(col.tp)))
                        {
                            const uint32_t ncname = static_cast<uint32_t>(std::stoul(path.sub_cols(0).column().substr(1)));

                            for (auto it = ntp->subtypes.cbegin(), next_it = it; it != ntp->subtypes.cend(); it = next_it)
                            {
                                ++next_it;
                                if (it->cname == ncname)
                                {
                                    ntp->subtypes.erase(it);
                                    break;
                                }
                            }
                            if (ntp->subtypes.empty())
                            {
                                t.cols.erase(cname);
                            }
                        }
                    }
                }
                else if (ati.has_rename_column() && success)
                {
                    const ColumnPath & path = ati.rename_column().old_name();
                    const uint32_t old_cname = static_cast<uint32_t>(std::stoul(path.col().column().substr(1)));

                    if (path.sub_cols_size() == 0)
                    {
                        const uint32_t new_cname
                            = static_cast<uint32_t>(std::stoul(ati.rename_column().new_name().col().column().substr(1)));

                        t.cols[new_cname] = std::move(t.cols[old_cname]);
                        t.cols[new_cname].cname = new_cname;
                        t.cols.erase(old_cname);
                    }
                    else
                    {
                        SQLColumn & col = t.cols.at(old_cname);
                        NestedType * ntp;

                        chassert(path.sub_cols_size() == 1);
                        if ((ntp = dynamic_cast<NestedType *>(col.tp)))
                        {
                            const uint32_t nocname = static_cast<uint32_t>(std::stoul(path.sub_cols(0).column().substr(1)));

                            for (auto it = ntp->subtypes.begin(), next_it = it; it != ntp->subtypes.end(); it = next_it)
                            {
                                ++next_it;
                                if (it->cname == nocname)
                                {
                                    it->cname
                                        = static_cast<uint32_t>(std::stoul(ati.rename_column().new_name().sub_cols(0).column().substr(1)));
                                    break;
                                }
                            }
                        }
                    }
                }
                else if (ati.has_modify_column())
                {
                    const uint32_t cname = static_cast<uint32_t>(std::stoul(ati.modify_column().new_col().col().column().substr(1)));

                    if (success)
                    {
                        t.cols.erase(cname);
                        t.cols[cname] = std::move(t.staged_cols[cname]);
                    }
                    t.staged_cols.erase(cname);
                }
                else if (
                    ati.has_column_remove_property() && success
                    && ati.column_remove_property().property() < RemoveColumnProperty_ColumnProperties_CODEC)
                {
                    const ColumnPath & path = ati.column_remove_property().col();
                    const uint32_t cname = static_cast<uint32_t>(std::stoul(path.col().column().substr(1)));

                    if (path.sub_cols_size() == 0)
                    {
                        t.cols.at(cname).dmod = std::nullopt;
                    }
                }
                else if (ati.has_add_index())
                {
                    const uint32_t iname = static_cast<uint32_t>(std::stoul(ati.add_index().new_idx().idx().index().substr(1)));

                    if (success)
                    {
                        t.idxs[iname] = std::move(t.staged_idxs[iname]);
                    }
                    t.staged_idxs.erase(iname);
                }
                else if (ati.has_drop_index() && success)
                {
                    const uint32_t iname = static_cast<uint32_t>(std::stoul(ati.drop_index().index().substr(1)));

                    t.idxs.erase(iname);
                }
                else if (ati.has_add_projection())
                {
                    const uint32_t pname = static_cast<uint32_t>(std::stoul(ati.add_projection().proj().projection().substr(1)));

                    if (success)
                    {
                        t.projs.insert(pname);
                    }
                    t.staged_projs.erase(pname);
                }
                else if (ati.has_remove_projection() && success)
                {
                    const uint32_t pname = static_cast<uint32_t>(std::stoul(ati.remove_projection().projection().substr(1)));

                    t.projs.erase(pname);
                }
                else if (ati.has_add_constraint())
                {
                    const uint32_t pname = static_cast<uint32_t>(std::stoul(ati.add_constraint().constr().constraint().substr(1)));

                    if (success)
                    {
                        t.constrs.insert(pname);
                    }
                    t.staged_constrs.erase(pname);
                }
                else if (ati.has_remove_constraint() && success)
                {
                    const uint32_t pname = static_cast<uint32_t>(std::stoul(ati.remove_constraint().constraint().substr(1)));

                    t.constrs.erase(pname);
                }
                else if (ati.has_freeze_partition() && success)
                {
                    const FreezePartition & fp = ati.freeze_partition();

                    t.frozen_partitions[fp.fname()] = fp.has_single_partition() ? fp.single_partition().partition().partition_id() : "";
                }
                else if (ati.has_unfreeze_partition() && success)
                {
                    t.frozen_partitions.erase(ati.unfreeze_partition().fname());
                }
            }
        }
    }
    else if (ssq.has_explain() && !ssq.explain().is_explain() && (query.has_attach() || query.has_detach()) && success)
    {
        const SQLObjectName & oobj = query.has_attach() ? query.attach().object() : query.detach().object();
        const bool istable = oobj.has_est() && oobj.est().table().table()[0] == 't';
        const bool isview = oobj.has_est() && oobj.est().table().table()[0] == 'v';
        const bool isdictionary = oobj.has_est() && oobj.est().table().table()[0] == 'd';
        const bool isdatabase = oobj.has_database();
        const uint32_t tname
            = static_cast<uint32_t>(std::stoul(isdatabase ? oobj.database().database().substr(1) : oobj.est().table().table().substr(1)));
        const DetachStatus status = query.has_attach()
            ? DetachStatus::ATTACHED
            : (query.detach().permanently() ? DetachStatus::PERM_DETACHED : DetachStatus::DETACHED);

        if (istable)
        {
            this->attachOrDetachObject<SQLTable>(tname, status);
        }
        else if (isview)
        {
            this->attachOrDetachObject<SQLView>(tname, status);
        }
        else if (isdictionary)
        {
            this->attachOrDetachObject<SQLDictionary>(tname, status);
        }
        else if (isdatabase)
        {
            this->attachOrDetachObject<std::shared_ptr<SQLDatabase>>(tname, status);
        }
    }
    else if (ssq.has_explain() && query.has_create_database())
    {
        const uint32_t dname = static_cast<uint32_t>(std::stoul(query.create_database().database().database().substr(1)));

        if (!ssq.explain().is_explain() && success)
        {
            this->databases[dname] = std::move(this->staged_databases[dname]);
        }
        this->staged_databases.erase(dname);
    }
    else if (ssq.has_explain() && query.has_create_function())
    {
        const uint32_t fname = static_cast<uint32_t>(std::stoul(query.create_function().function().function().substr(1)));

        if (!ssq.explain().is_explain() && success)
        {
            this->functions[fname] = std::move(this->staged_functions[fname]);
        }
        this->staged_functions.erase(fname);
    }
    else if (ssq.has_explain() && !ssq.explain().is_explain() && query.has_trunc() && query.trunc().has_database())
    {
        dropDatabase(static_cast<uint32_t>(std::stoul(query.trunc().database().database().substr(1))));
    }
    else if (ssq.has_explain() && query.has_backup_restore() && !ssq.explain().is_explain() && success)
    {
        const BackupRestore & br = query.backup_restore();
        const BackupRestoreElement & bre = br.backup_element();

        if (br.command() == BackupRestore_BackupCommand_BACKUP)
        {
            CatalogBackup newb;

            newb.backup_num = br.backup_number();
            newb.outf = br.out();
            if (br.has_outformat())
            {
                newb.out_format = br.outformat();
            }
            for (int i = 0; i < br.out_params_size(); i++)
            {
                newb.out_params.push_back(br.out_params(i));
            }
            if (bre.has_all())
            {
                newb.tables = this->tables;
                newb.views = this->views;
                newb.databases = this->databases;
                newb.dictionaries = this->dictionaries;
                newb.everything = true;
            }
            else if (bre.has_bobject() && bre.bobject().sobject() == SQLObject::TABLE)
            {
                const BackupRestoreObject & bro = bre.bobject();

                if (!bro.object().est().has_database() || bro.object().est().database().database() != "system")
                {
                    const uint32_t tname = static_cast<uint32_t>(std::stoul(bro.object().est().table().table().substr(1)));

                    newb.tables[tname] = this->tables[tname];
                    if (bro.partitions_size())
                    {
                        newb.partition_id = bro.partitions(0).partition_id();
                    }
                }
                else
                {
                    newb.system_table = bro.object().est().table().table();
                }
            }
            else if (bre.has_bobject() && bre.bobject().sobject() == SQLObject::VIEW)
            {
                const uint32_t vname = static_cast<uint32_t>(std::stoul(bre.bobject().object().est().table().table().substr(1)));

                newb.views[vname] = this->views[vname];
            }
            else if (bre.has_bobject() && bre.bobject().sobject() == SQLObject::DICTIONARY)
            {
                const uint32_t dname = static_cast<uint32_t>(std::stoul(bre.bobject().object().est().table().table().substr(1)));

                newb.dictionaries[dname] = this->dictionaries[dname];
            }
            else if (bre.has_bobject() && bre.bobject().sobject() == SQLObject::DATABASE)
            {
                const uint32_t dname = static_cast<uint32_t>(std::stoul(bre.bobject().object().database().database().substr(1)));

                for (const auto & [key, val] : this->tables)
                {
                    if (val.db && val.db->dname == dname)
                    {
                        newb.tables[key] = val;
                    }
                }
                for (const auto & [key, val] : this->views)
                {
                    if (val.db && val.db->dname == dname)
                    {
                        newb.views[key] = val;
                    }
                }
                for (const auto & [key, val] : this->dictionaries)
                {
                    if (val.db && val.db->dname == dname)
                    {
                        newb.dictionaries[key] = val;
                    }
                }
                newb.databases[dname] = this->databases[dname];
            }
            this->backups[br.backup_number()] = std::move(newb);
        }
        else
        {
            const CatalogBackup & backup = backups.at(br.backup_number());

            if (!backup.partition_id.has_value())
            {
                for (const auto & [key, val] : backup.databases)
                {
                    this->databases[key] = val;
                }
                for (const auto & [key, val] : backup.tables)
                {
                    if (!val.db || this->databases.find(val.db->dname) != this->databases.end())
                    {
                        this->tables[key] = val;
                    }
                }
                for (const auto & [key, val] : backup.views)
                {
                    if (!val.db || this->databases.find(val.db->dname) != this->databases.end())
                    {
                        this->views[key] = val;
                    }
                }
                for (const auto & [key, val] : backup.dictionaries)
                {
                    if (!val.db || this->databases.find(val.db->dname) != this->databases.end())
                    {
                        this->dictionaries[key] = val;
                    }
                }
            }
        }
    }
    else if (ssq.has_start_trans() && success)
    {
        this->in_transaction = true;
    }
    else if ((ssq.has_commit_trans() || ssq.has_rollback_trans()) && success)
    {
        this->in_transaction = false;
    }

    ei.resetExternalStatus();
}

void StatementGenerator::updateGenerator(const SQLQuery & sq, ExternalIntegrations & ei, bool success)
{
    updateGeneratorFromSingleQuery(sq.single_query(), ei, success);
    for (int i = 0; i < sq.parallel_queries_size(); i++)
    {
        updateGeneratorFromSingleQuery(sq.parallel_queries(i), ei, success);
    }
}

}
