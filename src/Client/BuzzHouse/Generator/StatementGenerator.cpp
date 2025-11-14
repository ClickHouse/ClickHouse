#include <Client/BuzzHouse/Generator/RandomSettings.h>
#include <Client/BuzzHouse/Generator/SQLCatalog.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>


namespace BuzzHouse
{

const std::vector<std::vector<OutFormat>> StatementGenerator::outFormats
    = {{OutFormat::OUT_Arrow},
       {OutFormat::OUT_Avro},
       {OutFormat::OUT_BSONEachRow},
       {OutFormat::OUT_CSV, OutFormat::OUT_CSVWithNames, OutFormat::OUT_CSVWithNamesAndTypes},
       {OutFormat::OUT_CustomSeparated, OutFormat::OUT_CustomSeparatedWithNames, OutFormat::OUT_CustomSeparatedWithNamesAndTypes},
       {OutFormat::OUT_JSON,
        OutFormat::OUT_JSONColumns,
        OutFormat::OUT_JSONColumnsWithMetadata,
        OutFormat::OUT_JSONCompact,
        OutFormat::OUT_JSONCompactColumns,
        OutFormat::OUT_JSONCompactEachRow,
        OutFormat::OUT_JSONCompactEachRowWithNames,
        OutFormat::OUT_JSONCompactEachRowWithNamesAndTypes,
        OutFormat::OUT_JSONCompactStringsEachRow,
        OutFormat::OUT_JSONCompactStringsEachRowWithNames,
        OutFormat::OUT_JSONCompactStringsEachRowWithNamesAndTypes,
        OutFormat::OUT_JSONEachRow,
        OutFormat::OUT_JSONLines,
        OutFormat::OUT_JSONObjectEachRow,
        OutFormat::OUT_JSONStringsEachRow},
       {OutFormat::OUT_LineAsString},
       {OutFormat::OUT_MsgPack},
       {OutFormat::OUT_Native},
       {OutFormat::OUT_ORC},
       {OutFormat::OUT_Parquet},
       {OutFormat::OUT_Protobuf, OutFormat::OUT_ProtobufSingle},
       {OutFormat::OUT_RawBLOB},
       {OutFormat::OUT_RowBinary, OutFormat::OUT_RowBinaryWithNames, OutFormat::OUT_RowBinaryWithNamesAndTypes},
       {OutFormat::OUT_TabSeparated,
        OutFormat::OUT_TabSeparatedRaw,
        OutFormat::OUT_TabSeparatedRawWithNames,
        OutFormat::OUT_TabSeparatedRawWithNamesAndTypes,
        OutFormat::OUT_TabSeparatedWithNames,
        OutFormat::OUT_TabSeparatedWithNamesAndTypes},
       {OutFormat::OUT_TSKV},
       {OutFormat::OUT_Values}};

const std::unordered_map<OutFormat, InFormat> StatementGenerator::outIn
    = {{OutFormat::OUT_Arrow, InFormat::IN_Arrow},
       {OutFormat::OUT_Avro, InFormat::IN_Avro},
       {OutFormat::OUT_BSONEachRow, InFormat::IN_BSONEachRow},
       {OutFormat::OUT_CSV, InFormat::IN_CSV},
       {OutFormat::OUT_CSVWithNames, InFormat::IN_CSVWithNames},
       {OutFormat::OUT_CSVWithNamesAndTypes, InFormat::IN_CSVWithNamesAndTypes},
       {OutFormat::OUT_CustomSeparated, InFormat::IN_CustomSeparated},
       {OutFormat::OUT_CustomSeparatedWithNames, InFormat::IN_CustomSeparatedWithNames},
       {OutFormat::OUT_CustomSeparatedWithNamesAndTypes, InFormat::IN_CustomSeparatedWithNamesAndTypes},
       {OutFormat::OUT_JSON, InFormat::IN_JSON},
       {OutFormat::OUT_JSONColumns, InFormat::IN_JSONColumns},
       {OutFormat::OUT_JSONColumnsWithMetadata, InFormat::IN_JSONColumnsWithMetadata},
       {OutFormat::OUT_JSONCompact, InFormat::IN_JSONCompact},
       {OutFormat::OUT_JSONCompactColumns, InFormat::IN_JSONCompactColumns},
       {OutFormat::OUT_JSONCompactEachRow, InFormat::IN_JSONCompactEachRow},
       {OutFormat::OUT_JSONCompactEachRowWithNames, InFormat::IN_JSONCompactEachRowWithNames},
       {OutFormat::OUT_JSONCompactEachRowWithNamesAndTypes, InFormat::IN_JSONCompactEachRowWithNamesAndTypes},
       {OutFormat::OUT_JSONCompactStringsEachRow, InFormat::IN_JSONCompactStringsEachRow},
       {OutFormat::OUT_JSONCompactStringsEachRowWithNames, InFormat::IN_JSONCompactStringsEachRowWithNames},
       {OutFormat::OUT_JSONCompactStringsEachRowWithNamesAndTypes, InFormat::IN_JSONCompactStringsEachRowWithNamesAndTypes},
       {OutFormat::OUT_JSONEachRow, InFormat::IN_JSONEachRow},
       {OutFormat::OUT_JSONLines, InFormat::IN_JSONLines},
       {OutFormat::OUT_JSONObjectEachRow, InFormat::IN_JSONObjectEachRow},
       {OutFormat::OUT_JSONStringsEachRow, InFormat::IN_JSONStringsEachRow},
       {OutFormat::OUT_LineAsString, InFormat::IN_LineAsString},
       {OutFormat::OUT_MsgPack, InFormat::IN_MsgPack},
       {OutFormat::OUT_Native, InFormat::IN_Native},
       {OutFormat::OUT_ORC, InFormat::IN_ORC},
       {OutFormat::OUT_Parquet, InFormat::IN_Parquet},
       {OutFormat::OUT_Protobuf, InFormat::IN_Protobuf},
       {OutFormat::OUT_ProtobufSingle, InFormat::IN_ProtobufSingle},
       {OutFormat::OUT_RawBLOB, InFormat::IN_RawBLOB},
       {OutFormat::OUT_RowBinary, InFormat::IN_RowBinary},
       {OutFormat::OUT_RowBinaryWithNames, InFormat::IN_RowBinaryWithNames},
       {OutFormat::OUT_RowBinaryWithNamesAndTypes, InFormat::IN_RowBinaryWithNamesAndTypes},
       {OutFormat::OUT_TabSeparated, InFormat::IN_TabSeparated},
       {OutFormat::OUT_TabSeparatedRaw, InFormat::IN_TabSeparatedRaw},
       {OutFormat::OUT_TabSeparatedRawWithNames, InFormat::IN_TabSeparatedRawWithNames},
       {OutFormat::OUT_TabSeparatedRawWithNamesAndTypes, InFormat::IN_TabSeparatedRawWithNamesAndTypes},
       {OutFormat::OUT_TabSeparatedWithNames, InFormat::IN_TabSeparatedWithNames},
       {OutFormat::OUT_TabSeparatedWithNamesAndTypes, InFormat::IN_TabSeparatedWithNamesAndTypes},
       {OutFormat::OUT_TSKV, InFormat::IN_TSKV},
       {OutFormat::OUT_Values, InFormat::IN_Values}};

const std::unordered_map<JoinType, std::vector<JoinConst>> StatementGenerator::joinMappings
    = {{J_LEFT, {J_ANY, J_ALL, J_SEMI, J_ANTI, J_ASOF}},
       {J_INNER, {J_ANY, J_ALL, J_ASOF}},
       {J_RIGHT, {J_ANY, J_ALL, J_SEMI, J_ANTI}},
       {J_FULL, {J_ANY, J_ALL}},
       {J_PASTE, {}},
       {J_CROSS, {}}};

StatementGenerator::StatementGenerator(FuzzConfig & fuzzc, ExternalIntegrations & conn, const bool supports_cloud_features_)
    : fc(fuzzc)
    , next_type_mask(fc.type_mask)
    , connections(conn)
    , supports_cloud_features(supports_cloud_features_)
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
    for (const auto & entry : CommonCHFuncs)
    {
        if (entry.min_lambda_param == 0 && entry.min_args == 1)
        {
            one_arg_funcs.push_back(entry);
        }
    }
    /* Deterministic engines */
    likeEngsDeterministic = {MergeTree};
    if ((fc.engine_mask & allow_replacing_mergetree) != 0)
    {
        likeEngsDeterministic.emplace_back(ReplacingMergeTree);
    }
    if ((fc.engine_mask & allow_coalescing_mergetree) != 0)
    {
        likeEngsDeterministic.emplace_back(CoalescingMergeTree);
    }
    if ((fc.engine_mask & allow_summing_mergetree) != 0)
    {
        likeEngsDeterministic.emplace_back(SummingMergeTree);
    }
    if ((fc.engine_mask & allow_aggregating_mergetree) != 0)
    {
        likeEngsDeterministic.emplace_back(AggregatingMergeTree);
    }
    if ((fc.engine_mask & allow_file) != 0)
    {
        likeEngsDeterministic.emplace_back(File);
    }
    if ((fc.engine_mask & allow_null) != 0)
    {
        likeEngsDeterministic.emplace_back(Null);
    }
    if ((fc.engine_mask & allow_setengine) != 0)
    {
        likeEngsDeterministic.emplace_back(Set);
    }
    if ((fc.engine_mask & allow_join) != 0)
    {
        likeEngsDeterministic.emplace_back(Join);
    }
    if ((fc.engine_mask & allow_stripelog) != 0)
    {
        likeEngsDeterministic.emplace_back(StripeLog);
    }
    if ((fc.engine_mask & allow_log) != 0)
    {
        likeEngsDeterministic.emplace_back(Log);
    }
    if ((fc.engine_mask & allow_tinylog) != 0)
    {
        likeEngsDeterministic.emplace_back(TinyLog);
    }
    if ((fc.engine_mask & allow_embedded_rocksdb) != 0)
    {
        likeEngsDeterministic.emplace_back(EmbeddedRocksDB);
    }
    if (fc.allow_memory_tables && (fc.engine_mask & allow_memory) != 0)
    {
        likeEngsDeterministic.emplace_back(Memory);
    }
    if (!fc.keeper_map_path_prefix.empty() && (fc.engine_mask & allow_keepermap) != 0)
    {
        likeEngsDeterministic.emplace_back(KeeperMap);
    }
    likeEngsNotDeterministic.insert(likeEngsNotDeterministic.end(), likeEngsDeterministic.begin(), likeEngsDeterministic.end());
    /* Not deterministic engines */
    if ((fc.engine_mask & allow_merge) != 0)
    {
        likeEngsNotDeterministic.emplace_back(Merge);
    }
    likeEngsInfinite.insert(likeEngsInfinite.end(), likeEngsNotDeterministic.begin(), likeEngsNotDeterministic.end());
    if (fc.allow_infinite_tables && (fc.engine_mask & allow_generaterandom) != 0)
    {
        likeEngsInfinite.emplace_back(GenerateRandom);
    }
}

void StatementGenerator::generateStorage(RandomGenerator & rg, Storage * store) const
{
    std::uniform_int_distribution<uint32_t> storage_range(1, static_cast<uint32_t>(Storage::DataStorage_MAX));

    store->set_storage(static_cast<Storage_DataStorage>(storage_range(rg.generator)));
    store->set_storage_name(rg.pickRandomly(fc.disks));
}

void StatementGenerator::generateHotTableSettingsValues(RandomGenerator & rg, const bool create, SettingValues * vals)
{
    std::uniform_int_distribution<size_t> settings_range(1, fc.hot_table_settings.size());
    const size_t nsets = settings_range(rg.generator);

    /// Add hot table settings
    chassert(this->ids.empty());
    for (size_t i = 0; i < fc.hot_table_settings.size(); i++)
    {
        this->ids.emplace_back(i);
    }
    std::shuffle(this->ids.begin(), this->ids.end(), rg.generator);

    for (size_t i = 0; i < nsets; i++)
    {
        const String & next = fc.hot_table_settings[this->ids[i]];
        SetValue * sv = vals->has_set_value() ? vals->add_other_values() : vals->mutable_set_value();

        sv->set_property(next);
        sv->set_value(
            ((create
              && (startsWith(next, "add_") || startsWith(next, "allow_") || startsWith(next, "index_") || startsWith(next, "enable_")))
             || rg.nextBool())
                ? "1"
                : "0");
    }
    this->ids.clear();
}

void StatementGenerator::generateSettingValues(
    RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, const size_t nvalues, SettingValues * vals)
{
    for (size_t i = 0; i < nvalues; i++)
    {
        const String & setting = rg.pickRandomly(settings);
        SetValue * set = vals->has_set_value() ? vals->add_other_values() : vals->mutable_set_value();

        set->set_property(setting);
        set->set_value(settings.at(setting).random_func(rg, fc));
    }
}

void StatementGenerator::generateSettingValues(
    RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingValues * vals)
{
    std::uniform_int_distribution<size_t> settings_range(1, std::min<size_t>(settings.size(), 40));

    generateSettingValues(rg, settings, settings_range(rg.generator), vals);
}

void StatementGenerator::generateHotTableSettingList(RandomGenerator & rg, SettingList * sl)
{
    std::uniform_int_distribution<size_t> settings_range(1, std::min<size_t>(fc.hot_table_settings.size(), 3));
    const size_t nvalues = settings_range(rg.generator);

    chassert(this->ids.empty());
    for (size_t i = 0; i < fc.hot_table_settings.size(); i++)
    {
        this->ids.emplace_back(i);
    }
    std::shuffle(this->ids.begin(), this->ids.end(), rg.generator);
    for (size_t i = 0; i < nvalues; i++)
    {
        const String & next = fc.hot_table_settings[this->ids[i]];

        if (sl->has_setting())
        {
            sl->add_other_settings(next);
        }
        else
        {
            sl->set_setting(next);
        }
    }
    this->ids.clear();
}

void StatementGenerator::generateSettingList(RandomGenerator & rg, const std::unordered_map<String, CHSetting> & settings, SettingList * sl)
{
    std::uniform_int_distribution<size_t> settings_range(1, std::min<size_t>(settings.size(), 5));
    const size_t nvalues = settings_range(rg.generator);

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

void StatementGenerator::generateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf)
{
    SQLFunction next;
    const uint32_t fname = this->function_counter++;
    const bool prev_enforce_final = this->enforce_final;
    const bool prev_allow_not_deterministic = this->allow_not_deterministic;

    next.fname = fname;
    next.nargs = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(1, fc.max_columns));
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

void StatementGenerator::generateNextRefreshableView(RandomGenerator & rg, RefreshableView * rv)
{
    const RefreshableView_RefreshPolicy pol = rg.nextBool() ? RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_EVERY
                                                            : RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_AFTER;

    rv->set_policy(pol);
    SetViewInterval(rg, rv->mutable_interval());
    if (pol == RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_EVERY && rg.nextBool())
    {
        const bool has_tables = collectionHas<SQLTable>(attached_tables);
        const bool has_views = collectionHas<SQLView>(attached_views);
        const bool has_dictionaries = collectionHas<SQLDictionary>(attached_dictionaries);

        SetViewInterval(rg, rv->mutable_offset());
        if (has_tables || !systemTables.empty() || has_views || has_dictionaries)
        {
            const uint32_t depend_table = 10 * static_cast<uint32_t>(has_tables);
            const uint32_t depend_system_table = 3 * static_cast<uint32_t>(!systemTables.empty());
            const uint32_t depend_view = 10 * static_cast<uint32_t>(has_views);
            const uint32_t depend_dictionary = 10 * static_cast<uint32_t>(has_dictionaries);
            const uint32_t prob_space = depend_table + depend_system_table + depend_view + depend_dictionary;
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            if (depend_table && nopt < (depend_table + 1))
            {
                const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

                t.setName(rv->mutable_depends()->mutable_est(), false);
            }
            else if (depend_system_table && nopt < (depend_table + depend_system_table + 1))
            {
                const auto & ntable = rg.pickRandomly(systemTables);

                ntable.setName(rv->mutable_depends()->mutable_est());
            }
            else if (depend_view && nopt < (depend_table + depend_system_table + depend_view + 1))
            {
                const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

                v.setName(rv->mutable_depends()->mutable_est(), false);
            }
            else if (depend_dictionary && nopt < (depend_table + depend_system_table + depend_view + depend_dictionary + 1))
            {
                const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(attached_dictionaries));

                d.setName(rv->mutable_depends()->mutable_est(), false);
            }
            else
            {
                UNREACHABLE();
            }
        }
    }
    SetViewInterval(rg, rv->mutable_randomize());
    rv->set_append(rg.nextBool());
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
    const uint32_t view_ncols = rg.randomInt<uint32_t>(1, fc.max_columns);
    const bool prev_enforce_final = this->enforce_final;
    const bool prev_allow_not_deterministic = this->allow_not_deterministic;
    SelectParen * sparen = cv->mutable_select();

    SQLBase::setDeterministic(rg, next);
    this->allow_not_deterministic = !next.is_deterministic;
    this->enforce_final = next.is_deterministic;
    next.is_temp = fc.allow_memory_tables && rg.nextMediumNumber() < 11;
    cv->set_is_temp(next.is_temp);
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
        if (!next.is_temp && collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && rg.nextSmallNumber() < 9)
        {
            next.db = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    cv->set_create_opt(replace ? CreateReplaceOption::Replace : CreateReplaceOption::Create);
    next.is_materialized = !next.is_temp && rg.nextBool();
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
        { return t.isAttached() && t.cols.size() >= view_ncols && (t.is_deterministic || !next.is_deterministic); };
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
            for (uint32_t i = 0; i < view_ncols; i++)
            {
                next.cols.insert(i);
            }
            generateEngineDetails(rg, createViewRelation("", next), next, true, te);
        }
        if ((next.isMergeTreeFamily() || rg.nextLargeNumber() < 8) && !next.is_deterministic && rg.nextMediumNumber() < 26)
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
                std::vector<uint32_t> nids;
                const bool allCols = rg.nextBool();
                const bool newdef = rg.nextSmallNumber() < 4;

                for (const auto & [key, val] : t.cols)
                {
                    if (allCols || val.canBeInserted())
                    {
                        nids.push_back(key);
                    }
                }
                if (nids.empty())
                {
                    nids.push_back(rg.pickRandomly(t.cols));
                }
                else if (rg.nextBool())
                {
                    std::shuffle(nids.begin(), nids.end(), rg.generator);
                }
                const uint32_t limit = std::min(view_ncols, static_cast<uint32_t>(nids.size()));

                chassert(limit > 0);
                for (uint32_t i = 0; i < limit; i++)
                {
                    SQLColumn col = t.cols.at(nids[i]);

                    if (newdef)
                    {
                        addTableColumnInternal(rg, t, col.cname, false, false, ColumnSpecial::NONE, col, cmvt->add_col_list());
                    }
                    next.cols.insert(col.cname);
                }
            }
        }
        if (!replace && !next.is_deterministic && (next.is_refreshable = rg.nextBool()))
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
    if (rg.nextSmallNumber() < 4)
    {
        cv->set_uuid(rg.nextUUID());
    }
    setClusterInfo(rg, next);
    if (next.cluster.has_value())
    {
        cv->mutable_cluster()->set_cluster(next.cluster.value());
    }
    sparen->set_paren(rg.nextSmallNumber() < 9);
    this->levels[this->current_level] = QueryLevel(this->current_level);
    this->allow_in_expression_alias = rg.nextSmallNumber() < 3;
    generateSelect(
        rg,
        false,
        false,
        view_ncols,
        next.is_materialized && rg.nextBool() ? (~allow_prewhere) : std::numeric_limits<uint32_t>::max(),
        std::nullopt,
        sparen->mutable_select());
    this->levels.clear();
    this->allow_in_expression_alias = true;
    this->enforce_final = prev_enforce_final;
    this->allow_not_deterministic = prev_allow_not_deterministic;
    matchQueryAliases(next, sparen->release_select(), sparen->mutable_select());
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
        dp->set_is_temp(v.is_temp);
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
        UNREACHABLE();
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
        const String dname = t.getDatabaseName();
        const String tname = t.getTableName();
        const bool table_has_partitions = rg.nextSmallNumber() < 9 && fc.tableHasPartitions(false, dname, tname);

        if (table_has_partitions)
        {
            if (allow_parts && rg.nextBool())
            {
                pexpr->set_part(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, false, dname, tname));
            }
            else
            {
                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
            }
            set_part = true;
        }
    }
    if (!set_part)
    {
        pexpr->set_tuple(true);
    }
}

void StatementGenerator::generateNextOptimizeTableInternal(RandomGenerator & rg, const SQLTable & t, bool strict, OptimizeTable * ot)
{
    const std::optional<String> & cluster = t.getCluster();

    t.setName(ot->mutable_est(), false);
    if (rg.nextBool())
    {
        generateNextTablePartition(rg, false, t, ot->mutable_single_partition()->mutable_partition());
    }
    ot->set_cleanup(rg.nextSmallNumber() < 3);
    if (!strict && rg.nextSmallNumber() < 4)
    {
        const uint32_t noption = rg.nextMediumNumber();
        DeduplicateExpr * dde = ot->mutable_dedup();

        if (noption < 51)
        {
            ColumnPathList * clist = noption < 26 ? dde->mutable_col_list() : dde->mutable_ded_star_except();
            flatTableColumnPath(flat_tuple | flat_nested | skip_nested_node, t.cols, [](const SQLColumn &) { return true; });
            const uint32_t ocols
                = (rg.nextLargeNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(4))) + 1;
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
    ot->set_final((t.supportsFinal() || t.isMergeTreeFamily() || rg.nextMediumNumber() < 21) && (strict || rg.nextSmallNumber() < 4));
    ot->set_use_force(rg.nextBool());
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, ot->mutable_setting_values());
    }
}

void StatementGenerator::generateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * ot)
{
    if (systemTables.empty() || rg.nextMediumNumber() < 91)
    {
        generateNextOptimizeTableInternal(rg, rg.pickRandomly(filterCollection<SQLTable>(attached_tables)), false, ot);
    }
    else
    {
        /// Optimize system table
        rg.pickRandomly(systemTables).setName(ot->mutable_est());
        ot->set_final(rg.nextBool());
        ot->set_use_force(rg.nextBool());
        if (rg.nextBool())
        {
            DeduplicateExpr * dde = ot->mutable_dedup();

            if (rg.nextBool())
            {
                dde->set_ded_star(true);
            }
        }
        if (rg.nextSmallNumber() < 3)
        {
            generateSettingValues(rg, serverSettings, ot->mutable_setting_values());
        }
    }
}

void StatementGenerator::generateNextCheckTable(RandomGenerator & rg, CheckTable * ct)
{
    if (systemTables.empty() || rg.nextMediumNumber() < 91)
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        t.setName(ct->mutable_est(), false);
        if (rg.nextBool())
        {
            generateNextTablePartition(rg, true, t, ct->mutable_single_partition()->mutable_partition());
        }
    }
    else
    {
        /// Check system table
        rg.pickRandomly(systemTables).setName(ct->mutable_est());
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

bool StatementGenerator::tableOrFunctionRef(RandomGenerator & rg, const SQLTable & t, TableOrFunction * tof)
{
    bool is_url = false;
    bool cluster_or_remote = false;
    const std::optional<String> & cluster = t.getCluster();
    const uint32_t cluster_func = 5 * static_cast<uint32_t>(cluster.has_value() || !fc.clusters.empty());
    const uint32_t remote_func = 5;
    const uint32_t no_remote_or_cluster = 90;
    const uint32_t prob_space = cluster_func + remote_func + no_remote_or_cluster;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    const bool allCols = rg.nextMediumNumber() < 2;

    flatTableColumnPath(skip_nested_node | flat_nested, t.cols, [&](const SQLColumn & c) { return allCols || c.canBeInserted(); });
    std::shuffle(this->entries.begin(), this->entries.end(), rg.generator);
    if (nopt < (cluster_func + remote_func + 1))
    {
        /// Use table function
        const bool isCluster = (cluster_func && (nopt < cluster_func + 1));

        setTableFunction(rg, isCluster ? TableFunctionUsage::ClusterCall : TableFunctionUsage::RemoteCall, t, tof->mutable_tfunc());
        tof = isCluster ? const_cast<ClusterFunc &>(tof->tfunc().cluster()).mutable_tof()
                        : const_cast<RemoteFunc &>(tof->tfunc().remote()).mutable_tof();
        cluster_or_remote = true;
    }

    /// Only schema, table declarations are allowed inside cluster and remote functions
    const uint32_t engine_func = 10 * static_cast<uint32_t>(t.isEngineReplaceable() && !cluster_or_remote);
    const uint32_t url_func = 5 * static_cast<uint32_t>(!cluster_or_remote);
    const uint32_t simple_est = 85;
    const uint32_t prob_space2 = engine_func + url_func + simple_est;
    std::uniform_int_distribution<uint32_t> next_dist2(1, prob_space2);
    const uint32_t nopt2 = next_dist2(rg.generator);

    if (engine_func && (nopt2 < engine_func + 1))
    {
        setTableFunction(rg, TableFunctionUsage::EngineReplace, t, tof->mutable_tfunc());
    }
    else if ((is_url = (url_func && (nopt2 < engine_func + url_func + 1))))
    {
        /// Use URL table function
        String url;
        String buf;
        bool first = true;
        URLFunc * ufunc = tof->mutable_tfunc()->mutable_url();
        const OutFormat outf = rg.nextBool() ? rg.pickRandomly(rg.pickRandomly(outFormats))
                                             : static_cast<OutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(OutFormat_MAX)) + 1);
        const InFormat iinf = (outIn.contains(outf)) && rg.nextBool()
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
        url += getNextHTTPURL(rg, rg.nextSmallNumber() < 4) + "/?query=INSERT+INTO+" + t.getFullName(rg.nextBool()) + "+(";
        for (const auto & entry : this->entries)
        {
            url += fmt::format("{}{}", first ? "" : ",", entry.columnPathRef());
            buf += fmt::format(
                "{}{} {}{}{}",
                first ? "" : ", ",
                entry.getBottomName(),
                entry.path.size() > 1 ? "Array(" : "",
                entry.getBottomType()->typeName(false, false),
                entry.path.size() > 1 ? ")" : "");
            first = false;
        }
        url += ")+FORMAT+" + InFormat_Name(iinf).substr(3);
        ufunc->set_uurl(std::move(url));
        ufunc->set_outformat(outf);
        ufunc->mutable_structure()->mutable_lit_val()->set_string_lit(std::move(buf));
    }
    else if (simple_est && (nopt2 < engine_func + url_func + simple_est + 1))
    {
        /// Use simple schema.table call
        t.setName(tof->mutable_est(), false);
    }
    else
    {
        UNREACHABLE();
    }
    this->entries.clear();
    return is_url;
}

void StatementGenerator::generateNextDescTable(RandomGenerator & rg, DescribeStatement * dt)
{
    const uint32_t desc_table = 10 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t desc_view = 10 * static_cast<uint32_t>(collectionHas<SQLView>(attached_views));
    const uint32_t desc_dict = 10 * static_cast<uint32_t>(collectionHas<SQLDictionary>(attached_dictionaries));
    const uint32_t desc_query = 5;
    const uint32_t desc_function = 5;
    const uint32_t desc_system_table = 3 * static_cast<uint32_t>(!systemTables.empty());
    const uint32_t prob_space = desc_table + desc_view + desc_query + desc_function + desc_system_table;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (desc_table && nopt < (desc_table + 1))
    {
        const auto is_url = tableOrFunctionRef(rg, rg.pickRandomly(filterCollection<SQLTable>(attached_tables)), dt->mutable_tof());
        UNUSED(is_url);
    }
    else if (desc_view && nopt < (desc_table + desc_view + 1))
    {
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

        v.setName(dt->mutable_tof()->mutable_est(), false);
    }
    else if (desc_dict && nopt < (desc_table + desc_view + desc_dict + 1))
    {
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(attached_dictionaries));

        d.setName(dt->mutable_tof()->mutable_est(), false);
    }
    else if (desc_query && nopt < (desc_table + desc_view + desc_dict + desc_query + 1))
    {
        ExplainQuery * eq = dt->mutable_sel();

        if (rg.nextMediumNumber() < 6)
        {
            prepareNextExplain(rg, eq);
        }
        else
        {
            this->levels[this->current_level] = QueryLevel(this->current_level);
            generateSelect(
                rg,
                false,
                false,
                rg.randomInt<uint32_t>(1, 5),
                std::numeric_limits<uint32_t>::max(),
                std::nullopt,
                eq->mutable_inner_query()->mutable_select()->mutable_sel());
        }
        this->levels.clear();
    }
    else if (desc_function && nopt < (desc_table + desc_view + desc_dict + desc_query + desc_function + 1))
    {
        generateTableFuncCall(rg, dt->mutable_stf());
        this->levels.clear();
    }
    else if (desc_system_table && nopt < (desc_table + desc_view + desc_dict + desc_query + desc_function + desc_system_table + 1))
    {
        rg.pickRandomly(systemTables).setName(dt->mutable_tof()->mutable_est());
    }
    else
    {
        UNREACHABLE();
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
    const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));
    const uint32_t hardcoded_insert = 70 * static_cast<uint32_t>(fc.allow_hardcoded_inserts && !in_parallel);
    const uint32_t random_values = 5 * static_cast<uint32_t>(!in_parallel);
    const uint32_t generate_random = 30;
    const uint32_t number_func = 30;
    const uint32_t insert_select = 10;
    const uint32_t prob_space = hardcoded_insert + random_values + generate_random + number_func + insert_select;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);
    std::uniform_int_distribution<uint64_t> rows_dist(fc.min_insert_rows, fc.max_insert_rows);
    std::uniform_int_distribution<uint64_t> string_length_dist(1, 8192);
    std::uniform_int_distribution<uint64_t> nested_rows_dist(fc.min_nested_rows, fc.max_nested_rows);
    const bool is_url = tableOrFunctionRef(rg, t, ins->mutable_tof());

    flatTableColumnPath(skip_nested_node | flat_nested, t.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
    std::shuffle(this->entries.begin(), this->entries.end(), rg.generator);
    if (!is_url)
    {
        for (const auto & entry : this->entries)
        {
            columnPathRef(entry, ins->add_cols());
        }
    }
    if (hardcoded_insert && (nopt < hardcoded_insert + 1))
    {
        const uint64_t nrows = rows_dist(rg.generator);
        const bool allow_cast = rg.nextSmallNumber() < 3;

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
                    buf += strAppendAnyValue(rg, allow_cast, entry.getBottomType());
                }
                j++;
            }
            buf += ")";
        }
        ins->set_query(buf);
    }
    else if (random_values && nopt < (hardcoded_insert + random_values + 1))
    {
        const uint32_t nrows = rg.randomInt<uint32_t>(1, 3);
        ValuesStatement * vs = ins->mutable_values();

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
                    this->levels[this->current_level] = QueryLevel(this->current_level);
                    this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
                    this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
                    generateExpression(rg, expr);
                    this->levels.clear();
                }
                first = false;
            }
        }
    }
    else
    {
        SelectParen * sparen = ins->mutable_select();
        Select * sel = sparen->mutable_select();

        if (generate_random && nopt < (hardcoded_insert + random_values + generate_random + 1))
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
                    entry.getBottomType()->typeName(false, false),
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
        else if (number_func && nopt < (hardcoded_insert + random_values + generate_random + number_func + 1))
        {
            /// Use numbers function
            bool first = true;
            bool has_aggr = false;
            SelectStatementCore * ssc = sel->mutable_select_core();
            GenerateSeriesFunc * gsf = ssc->mutable_from()
                                           ->mutable_tos()
                                           ->mutable_join_clause()
                                           ->mutable_tos()
                                           ->mutable_joined_table()
                                           ->mutable_tof()
                                           ->mutable_tfunc()
                                           ->mutable_gseries();
            const uint32_t nested_nrows = nested_rows_dist(rg.generator);

            for (const auto & entry : this->entries)
            {
                const String nval
                    = entry.getBottomType()->insertNumberEntry(rg, *this, string_length_dist(rg.generator), nested_rows_dist(rg.generator));

                buf += fmt::format(
                    "{}{}{}{}",
                    first ? "" : ", ",
                    entry.path.size() > 1 ? "arrayResize([" : "",
                    nval,
                    entry.path.size() > 1 ? ("], " + std::to_string(nested_nrows) + ", " + nval + ")") : "");
                first = false;
                has_aggr |= entry.getBottomType()->getTypeClass() == SQLTypeClass::AGGREGATEFUNCTION;
            }
            ssc->add_result_columns()->mutable_eca()->mutable_expr()->mutable_lit_val()->set_no_quote_str(std::move(buf));
            gsf->set_fname(GenerateSeriesFunc_GSName::GenerateSeriesFunc_GSName_numbers);
            gsf->mutable_expr1()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(rows_dist(rg.generator));
            if (has_aggr || rg.nextMediumNumber() < 21)
            {
                /// Add GROUP BY for AggregateFunction type
                ssc->mutable_groupby()
                    ->mutable_glist()
                    ->mutable_exprs()
                    ->mutable_expr()
                    ->mutable_comp_expr()
                    ->mutable_expr_stc()
                    ->mutable_col()
                    ->mutable_path()
                    ->mutable_col()
                    ->set_column("number");
            }
        }
        else if (insert_select && nopt < (hardcoded_insert + random_values + generate_random + number_func + insert_select + 1))
        {
            /// Use insert select combination
            sparen->set_paren(rg.nextSmallNumber() < 4);
            this->levels[this->current_level] = QueryLevel(this->current_level);
            if (rg.nextMediumNumber() < 13)
            {
                this->addCTEs(rg, std::numeric_limits<uint32_t>::max(), ins->mutable_ctes());
            }
            generateSelect(
                rg, true, false, static_cast<uint32_t>(this->entries.size()), std::numeric_limits<uint32_t>::max(), std::nullopt, sel);
            this->levels.clear();
        }
        else
        {
            UNREACHABLE();
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

void StatementGenerator::generateNextUpdate(RandomGenerator & rg, const SQLTable & t, Update * upt)
{
    if (rg.nextBool())
    {
        generateNextTablePartition(rg, false, t, upt->mutable_single_partition()->mutable_partition());
    }
    flatTableColumnPath(flat_tuple | flat_nested, t.cols, [](const SQLColumn &) { return true; });
    if (this->entries.empty())
    {
        UpdateSet * upset = upt->mutable_update();

        upset->mutable_col()->mutable_col()->set_column("c0");
        upset->mutable_expr()->mutable_lit_val()->mutable_int_lit()->set_int_lit(0);
    }
    else
    {
        const uint32_t nupdates = (rg.nextLargeNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(4))) + 1;
        const bool allow_cast = rg.nextSmallNumber() < 3;

        std::shuffle(this->entries.begin(), this->entries.end(), rg.generator);
        for (uint32_t j = 0; j < nupdates; j++)
        {
            columnPathRef(this->entries[j], j == 0 ? upt->mutable_update()->mutable_col() : upt->add_other_updates()->mutable_col());
        }
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
                    buf = strAppendAnyValue(rg, allow_cast, entry.getBottomType());
                }
                lv->set_no_quote_str(buf);
            }
            else
            {
                addTableRelation(rg, true, "", t);
                this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
                this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
                generateExpression(rg, expr);
                this->levels.clear();
            }
        }
        this->entries.clear();
    }

    generateUptDelWhere(rg, t, upt->mutable_where()->mutable_expr()->mutable_expr());
}

void StatementGenerator::generateNextDelete(RandomGenerator & rg, const SQLTable & t, Delete * del)
{
    if (rg.nextBool())
    {
        generateNextTablePartition(rg, false, t, del->mutable_single_partition()->mutable_partition());
    }
    generateUptDelWhere(rg, t, del->mutable_where()->mutable_expr()->mutable_expr());
}

template <typename T>
void StatementGenerator::generateNextUpdateOrDelete(RandomGenerator & rg, T * st)
{
    const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));
    const std::optional<String> & cluster = t.getCluster();

    t.setName(st->mutable_est(), false);
    if (cluster.has_value())
    {
        st->mutable_cluster()->set_cluster(cluster.value());
    }
    if constexpr (std::is_same_v<T, LightDelete>)
    {
        generateNextDelete(rg, t, st->mutable_del());
    }
    else
    {
        generateNextUpdate(rg, t, st->mutable_upt());
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, st->mutable_setting_values());
    }
}

void StatementGenerator::generateNextTruncate(RandomGenerator & rg, Truncate * trunc)
{
    const bool trunc_database = collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases);
    const uint32_t trunc_table = 950 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t trunc_db_tables = 15 * static_cast<uint32_t>(trunc_database);
    const uint32_t trunc_db = 5 * static_cast<uint32_t>(trunc_database);
    const uint32_t trunc_system_table = 30 * static_cast<uint32_t>(!systemTables.empty());
    const uint32_t prob_space = trunc_table + trunc_db_tables + trunc_db + trunc_system_table;
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
    else if (trunc_system_table && nopt < (trunc_table + trunc_db_tables + trunc_db + trunc_system_table + 1))
    {
        rg.pickRandomly(systemTables).setName(trunc->mutable_est());
    }
    else
    {
        UNREACHABLE();
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
        UNREACHABLE();
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

uint32_t StatementGenerator::getIdentifierFromString(const String & tname) const
{
    const uint32_t offset = startsWith(tname, "test.") ? 6 : 1;
    return static_cast<uint32_t>(std::stoul(tname.substr(offset)));
}

void StatementGenerator::generateAlter(RandomGenerator & rg, Alter * at)
{
    SQLObjectName * sot = at->mutable_object();
    const uint32_t alter_view = 5 * static_cast<uint32_t>(collectionHas<SQLView>(attached_views));
    const uint32_t alter_table = 15 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t alter_database = 2 * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases));
    const uint32_t prob_space2 = alter_view + alter_table + alter_database;
    std::uniform_int_distribution<uint32_t> next_dist2(1, prob_space2);
    const uint32_t nopt2 = next_dist2(rg.generator);
    std::optional<String> cluster;
    const bool prev_enforce_final = this->enforce_final;
    const bool prev_allow_not_deterministic = this->allow_not_deterministic;
    const uint32_t nalters = rg.randomInt<uint32_t>(1, fc.max_number_alters);

    if (alter_view && nopt2 < (alter_view + 1))
    {
        SQLView & v = const_cast<SQLView &>(rg.pickRandomly(filterCollection<SQLView>(attached_views)).get());

        this->allow_not_deterministic = !v.is_deterministic;
        this->enforce_final = v.is_deterministic;
        cluster = v.getCluster();
        at->set_is_temp(v.is_temp);
        at->set_sobject(SQLObject::TABLE);
        v.setName(sot->mutable_est(), false);
        for (uint32_t i = 0; i < nalters; i++)
        {
            const uint32_t alter_refresh = 1 * static_cast<uint32_t>(!v.is_deterministic);
            const uint32_t alter_query = 3;
            const uint32_t comment_view = 2;
            const uint32_t prob_space = alter_refresh + alter_query + comment_view;
            AlterItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            ati->set_paren(rg.nextSmallNumber() < 9);
            if (alter_refresh && nopt < (alter_refresh + 1))
            {
                generateNextRefreshableView(rg, ati->mutable_refresh());
            }
            else if (alter_query && nopt < (alter_refresh + alter_query + 1))
            {
                SelectParen * sparen = ati->mutable_modify_query();

                v.staged_ncols = v.has_with_cols ? static_cast<uint32_t>(v.cols.size()) : rg.randomInt<uint32_t>(1, fc.max_columns);
                sparen->set_paren(rg.nextSmallNumber() < 9);
                this->levels[this->current_level] = QueryLevel(this->current_level);
                this->allow_in_expression_alias = rg.nextSmallNumber() < 3;
                generateSelect(
                    rg,
                    false,
                    false,
                    v.staged_ncols,
                    v.is_materialized && rg.nextBool() ? (~allow_prewhere) : std::numeric_limits<uint32_t>::max(),
                    std::nullopt,
                    sparen->mutable_select());
                this->levels.clear();
                this->allow_in_expression_alias = true;
                matchQueryAliases(v, sparen->release_select(), sparen->mutable_select());
            }
            else if (comment_view && nopt < (alter_refresh + alter_query + comment_view + 1))
            {
                ati->set_comment(nextComment(rg));
            }
            else
            {
                UNREACHABLE();
            }
        }
    }
    else if (alter_table && nopt2 < (alter_view + alter_table + 1))
    {
        SQLTable & t = const_cast<SQLTable &>(rg.pickRandomly(filterCollection<SQLTable>(attached_tables)).get());
        const String dname = t.getDatabaseName();
        const String tname = t.getTableName();
        const bool table_has_partitions = t.isMergeTreeFamily() && fc.tableHasPartitions(false, dname, tname);

        this->allow_not_deterministic = !t.is_deterministic;
        this->enforce_final = t.is_deterministic;
        cluster = t.getCluster();
        at->set_is_temp(t.is_temp);
        at->set_sobject(SQLObject::TABLE);
        t.setName(sot->mutable_est(), false);
        for (uint32_t i = 0; i < nalters; i++)
        {
            const uint32_t alter_order_by = 3;
            const uint32_t heavy_delete = 30;
            const uint32_t heavy_update = 40;
            const uint32_t add_column = 2 * static_cast<uint32_t>(!t.hasDatabasePeer() && t.cols.size() < fc.max_columns);
            const uint32_t materialize_column = 2;
            const uint32_t drop_column = 2 * static_cast<uint32_t>(!t.hasDatabasePeer() && t.cols.size() > 1);
            const uint32_t rename_column = 2 * static_cast<uint32_t>(!t.hasDatabasePeer());
            const uint32_t clear_column = 2;
            const uint32_t modify_column = 2 * static_cast<uint32_t>(!t.hasDatabasePeer());
            const uint32_t comment_column = 2;
            const uint32_t add_stats = 3;
            const uint32_t mod_stats = 3;
            const uint32_t drop_stats = 3;
            const uint32_t clear_stats = 3;
            const uint32_t mat_stats = 3;
            const uint32_t delete_mask = 8;
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
            const uint32_t modify_ttl = 5 * static_cast<uint32_t>(!t.is_deterministic);
            const uint32_t remove_ttl = 2 * static_cast<uint32_t>(!t.is_deterministic);
            const uint32_t attach_partition_from = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t replace_partition_from = 5 * static_cast<uint32_t>(t.isMergeTreeFamily());
            const uint32_t comment_table = 2;
            const uint32_t rewrite_parts = 8;
            const uint32_t prob_space = alter_order_by + heavy_delete + heavy_update + add_column + materialize_column + drop_column
                + rename_column + clear_column + modify_column + comment_column + delete_mask + add_stats + mod_stats + drop_stats
                + clear_stats + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property
                + column_modify_setting + column_remove_setting + table_modify_setting + table_remove_setting + add_projection
                + remove_projection + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition
                + drop_partition + drop_detached_partition + forget_partition + attach_partition + move_partition_to
                + clear_column_partition + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl
                + remove_ttl + attach_partition_from + replace_partition_from + comment_table + rewrite_parts;
            AlterItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            ati->set_paren(rg.nextSmallNumber() < 9);
            if (alter_order_by && nopt < (alter_order_by + 1))
            {
                TableKey * tkey = ati->mutable_order();

                if (rg.nextSmallNumber() < 6)
                {
                    flatTableColumnPath(
                        flat_tuple | flat_nested | flat_json | skip_nested_node, t.cols, [](const SQLColumn &) { return true; });
                    generateTableKey(rg, createTableRelation(rg, true, "", t), t, true, tkey);
                    this->entries.clear();
                }
            }
            else if (heavy_delete && nopt < (heavy_delete + alter_order_by + 1))
            {
                generateNextDelete(rg, t, ati->mutable_del());
            }
            else if (add_column && nopt < (heavy_delete + alter_order_by + add_column + 1))
            {
                const uint32_t next_option = rg.nextSmallNumber();
                const uint32_t ncname = t.col_counter++;
                AddColumn * add_col = ati->mutable_add_column();
                ColumnDef * def = add_col->mutable_new_col();
                const uint64_t type_mask_backup = this->next_type_mask;
                std::vector<uint32_t> nested_ids;

                if (next_option < 4)
                {
                    flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                    columnPathRef(rg.pickRandomly(this->entries), add_col->mutable_add_where()->mutable_col());
                    this->entries.clear();
                }
                else if (next_option < 8)
                {
                    add_col->mutable_add_where()->set_first(true);
                }

                /// Add small chance to add to a nested column
                if (rg.nextSmallNumber() < 4)
                {
                    for (const auto & [key, val] : t.cols)
                    {
                        if (val.tp->getTypeClass() == SQLTypeClass::NESTED)
                        {
                            nested_ids.emplace_back(key);
                        }
                    }
                    this->next_type_mask = fc.type_mask & ~(allow_nested);
                }

                addTableColumn(rg, t, ncname, true, false, rg.nextMediumNumber() < 6, ColumnSpecial::NONE, def);
                this->next_type_mask = type_mask_backup;

                if (!nested_ids.empty())
                {
                    std::unordered_map<uint32_t, SQLColumn> nested_cols;
                    SQLColumn ncol = std::move(t.staged_cols[ncname]);
                    SQLColumn & nested_col = t.cols.at(rg.pickRandomly(nested_ids));
                    NestedType * ntp = dynamic_cast<NestedType *>(nested_col.tp);

                    chassert(nested_col.tp && ncol.tp);
                    ntp->subtypes.emplace_back(NestedSubType(ncname, ncol.tp));
                    ncol.tp = nullptr;
                    nested_cols[nested_col.cname] = nested_col;
                    flatTableColumnPath(flat_nested, nested_cols, [](const SQLColumn &) { return true; });
                    columnPathRef(this->entries.back(), def->mutable_col());
                    this->entries.clear();
                    t.staged_cols.erase(ncname);
                }
            }
            else if (materialize_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + 1))
            {
                ColInPartition * mcol = ati->mutable_materialize_column();

                flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomly(this->entries), mcol->mutable_col());
                this->entries.clear();
                if (rg.nextBool())
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
                if (rg.nextBool())
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
                ColumnDef * def = add_col->mutable_new_col();
                const uint64_t type_mask_backup = this->next_type_mask;
                std::vector<uint32_t> nested_ids;

                if (next_option < 4)
                {
                    flatTableColumnPath(flat_nested, t.cols, [](const SQLColumn &) { return true; });
                    columnPathRef(rg.pickRandomly(this->entries), add_col->mutable_add_where()->mutable_col());
                    this->entries.clear();
                }
                else if (next_option < 8)
                {
                    add_col->mutable_add_where()->set_first(true);
                }

                /// Add small chance to modify a nested column
                if (rg.nextSmallNumber() < 4)
                {
                    for (const auto & [key, val] : t.cols)
                    {
                        if (val.tp->getTypeClass() == SQLTypeClass::NESTED)
                        {
                            nested_ids.emplace_back(key);
                        }
                    }
                    this->next_type_mask = fc.type_mask & ~(allow_nested);
                }

                const uint32_t ncol = nested_ids.empty() ? rg.pickRandomly(t.cols) : t.col_counter++;
                addTableColumn(rg, t, ncol, true, true, rg.nextMediumNumber() < 6, ColumnSpecial::NONE, def);
                this->next_type_mask = type_mask_backup;

                if (!nested_ids.empty())
                {
                    std::unordered_map<uint32_t, SQLColumn> nested_cols;
                    const SQLColumn & nested_col = t.cols.at(rg.pickRandomly(nested_ids));

                    nested_cols[nested_col.cname] = nested_col;
                    flatTableColumnPath(flat_nested, nested_cols, [](const SQLColumn &) { return true; });
                    const auto & entry = rg.pickRandomly(this->entries);
                    columnPathRef(entry, def->mutable_col());
                    const uint32_t refcol = getIdentifierFromString(entry.getBottomName());
                    this->entries.clear();
                    t.staged_cols[refcol] = std::move(t.staged_cols[ncol]);
                    t.staged_cols.erase(ncol);
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
                OptionalPartitionExpr * ope = ati->mutable_delete_mask();

                if (rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, ope->mutable_single_partition()->mutable_partition());
                }
            }
            else if (
                heavy_update
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + 1))
            {
                generateNextUpdate(rg, t, ati->mutable_update());
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
                MaterializeStatistics * ms = ati->mutable_mat_stats();

                if (rg.nextSmallNumber() < 4)
                {
                    ms->set_all(true);
                }
                else
                {
                    pickUpNextCols(rg, t, ms->mutable_cols());
                }
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
                if (rg.nextBool())
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
                if (rg.nextBool())
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
                if (t.isMergeTreeFamily() && !fc.hot_table_settings.empty() && rg.nextBool())
                {
                    generateHotTableSettingsValues(rg, false, svs);
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
                if (t.isMergeTreeFamily() && !fc.hot_table_settings.empty() && rg.nextBool())
                {
                    generateHotTableSettingList(rg, sl);
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
                if (rg.nextBool())
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
                if (rg.nextBool())
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
                    pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
                }
                else if (table_has_partitions && nopt3 < 9)
                {
                    pexpr->set_part(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, false, dname, tname));
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
                    pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
                }
                else if (table_has_partitions && nopt3 < 9)
                {
                    pexpr->set_part(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, false, dname, tname));
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
                    pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), true, true, dname, tname));
                }
                else if (table_has_detached_partitions && nopt3 < 9)
                {
                    pexpr->set_part(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), true, false, dname, tname));
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

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
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
                    pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), true, true, dname, tname));
                }
                else if (table_has_detached_partitions && nopt3 < 9)
                {
                    pexpr->set_part(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), true, false, dname, tname));
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

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
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

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
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
                        fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
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

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
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

                pexpr->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
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
                flatTableColumnPath(flat_tuple | flat_nested, t.cols, [](const SQLColumn &) { return true; });
                generateNextTTL(rg, std::make_optional<SQLTable>(t), nullptr, ati->mutable_modify_ttl());
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
                const String dname2 = t2.getDatabaseName();
                const String tname2 = t2.getTableName();
                const bool table_has_partitions2 = t2.isMergeTreeFamily() && fc.tableHasPartitions(false, dname2, tname2);

                pexpr->set_partition_id(
                    table_has_partitions2 ? fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname2, tname2) : "0");
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
                const String dname2 = t2.getDatabaseName();
                const String tname2 = t2.getTableName();
                const bool table_has_partitions2 = t2.isMergeTreeFamily() && fc.tableHasPartitions(false, dname2, tname2);

                pexpr->set_partition_id(
                    table_has_partitions2 ? fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname2, tname2) : "0");
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
            else if (
                rewrite_parts
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl + remove_ttl
                       + attach_partition_from + replace_partition_from + comment_table + rewrite_parts + 1))
            {
                OptionalPartitionExpr * ope = ati->mutable_rewrite_parts();

                if (rg.nextBool())
                {
                    generateNextTablePartition(rg, false, t, ope->mutable_single_partition()->mutable_partition());
                }
            }
            else
            {
                UNREACHABLE();
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

            ati->set_paren(rg.nextSmallNumber() < 9);
            ati->set_comment(nextComment(rg));
        }
    }
    else
    {
        UNREACHABLE();
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
        UNREACHABLE();
    }
    if (rg.nextSmallNumber() < 4)
    {
        att->set_uuid(rg.nextUUID());
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
        UNREACHABLE();
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
    const uint32_t has_table = static_cast<uint32_t>(allow_table_statements && collectionHas<SQLTable>(attached_tables));

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
    const uint32_t load_pks = 3;
    const uint32_t load_pk = 8 * has_table;
    const uint32_t unload_pks = 3;
    const uint32_t unload_pk = 8 * has_table;
    /// for refreshable views
    const uint32_t refresh_views = 0;
    const uint32_t refresh_view = 8 * has_refreshable_view;
    const uint32_t stop_views = 3;
    const uint32_t stop_view = 8 * has_refreshable_view;
    const uint32_t start_views = 3;
    const uint32_t start_view = 8 * has_refreshable_view;
    const uint32_t cancel_view = 8 * has_refreshable_view;
    const uint32_t wait_view = 8 * has_refreshable_view;
    const uint32_t prewarm_cache = 8 * has_table;
    const uint32_t prewarm_primary_index_cache = 8 * has_table;
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
    const uint32_t enable_failpoint = 15;
    const uint32_t disable_failpoint = 15;
    const uint32_t reconnect_keeper = 5;
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
        + drop_query_condition_cache + enable_failpoint + disable_failpoint + reconnect_keeper;
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
        cluster = setTableSystemStatement<SQLTable>(rg, attached_tables, sc->mutable_load_pk());
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
        cluster = setTableSystemStatement<SQLTable>(rg, attached_tables, sc->mutable_unload_pk());
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
        cluster = setTableSystemStatement<SQLTable>(rg, attached_tables, sc->mutable_prewarm_cache());
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
        cluster = setTableSystemStatement<SQLTable>(rg, attached_tables, sc->mutable_prewarm_primary_index_cache());
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
    else if (
        enable_failpoint
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
               + flush_distributed + stop_distributed_sends + start_distributed_sends + drop_query_condition_cache + enable_failpoint + 1))
    {
        std::uniform_int_distribution<uint32_t> fail_range(1, static_cast<uint32_t>(FailPoint_MAX));

        sc->set_enable_failpoint(static_cast<FailPoint>(fail_range(rg.generator)));
    }
    else if (
        disable_failpoint
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
               + flush_distributed + stop_distributed_sends + start_distributed_sends + drop_query_condition_cache + enable_failpoint
               + disable_failpoint + 1))
    {
        std::uniform_int_distribution<uint32_t> fail_range(1, static_cast<uint32_t>(FailPoint_MAX));

        sc->set_disable_failpoint(static_cast<FailPoint>(fail_range(rg.generator)));
    }
    else if (
        reconnect_keeper
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
               + flush_distributed + stop_distributed_sends + start_distributed_sends + drop_query_condition_cache + enable_failpoint
               + disable_failpoint + reconnect_keeper + 1))
    {
        sc->set_reconnect_keeper(true);
    }
    else
    {
        UNREACHABLE();
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

void StatementGenerator::generateNextShowStatement(RandomGenerator & rg, ShowStatement * st)
{
    const uint32_t show_table = 15 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t show_system_table = 8 * static_cast<uint32_t>(!systemTables.empty());
    const uint32_t show_view = 15 * static_cast<uint32_t>(collectionHas<SQLView>(attached_views));
    const uint32_t show_dictionary = 15 * static_cast<uint32_t>(collectionHas<SQLDictionary>(attached_dictionaries));
    const uint32_t show_database = 15 * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases));
    const uint32_t show_databases = 3;
    const uint32_t show_database_dictionaries = 8 * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases));
    const uint32_t show_dictionaries = 3;
    const uint32_t show_columns = 6 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t show_columns_system_table = 3 * static_cast<uint32_t>(!systemTables.empty());
    const uint32_t show_indexes = 6 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t show_indexes_system_table = 3 * static_cast<uint32_t>(!systemTables.empty());
    const uint32_t show_policies = 6 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t show_policies_system_table = 3 * static_cast<uint32_t>(!systemTables.empty());
    const uint32_t show_processlist = 3;
    const uint32_t show_grants = 3;
    const uint32_t show_users = 3;
    const uint32_t show_roles = 3;
    const uint32_t show_profiles = 3;
    const uint32_t show_quotas = 3;
    const uint32_t show_quota = 3;
    const uint32_t show_access = 3;
    const uint32_t show_cluster = 3 * static_cast<uint32_t>(!fc.clusters.empty());
    const uint32_t show_clusters = 3;
    const uint32_t show_settings = 3;
    const uint32_t show_filesystem_caches = 3;
    const uint32_t show_engines = 3;
    const uint32_t show_functions = 3;
    const uint32_t show_merges = 3;
    const uint32_t prob_space = show_table + show_system_table + show_view + show_dictionary + show_database + show_databases
        + show_database_dictionaries + show_dictionaries + show_columns + show_columns_system_table + show_indexes
        + show_indexes_system_table + show_policies + show_policies_system_table + show_processlist + show_grants + show_users + show_roles
        + show_profiles + show_quotas + show_quota + show_access + show_cluster + show_clusters + show_settings + show_filesystem_caches
        + show_engines + show_functions + show_merges;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (show_table && nopt < (show_table + 1))
    {
        ShowObject * so = st->mutable_object();
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        so->set_is_temp(t.is_temp);
        so->set_create(rg.nextBool());
        so->set_sobject(SQLObject::TABLE);
        t.setName(so->mutable_object()->mutable_est(), false);
    }
    else if (show_system_table && nopt < (show_table + show_system_table + 1))
    {
        ShowObject * so = st->mutable_object();
        const auto & ntable = rg.pickRandomly(systemTables);

        so->set_is_temp(rg.nextLargeNumber() < 4);
        so->set_create(rg.nextBool());
        so->set_sobject(SQLObject::TABLE);
        ntable.setName(so->mutable_object()->mutable_est());
    }
    else if (show_view && nopt < (show_table + show_system_table + show_view + 1))
    {
        ShowObject * so = st->mutable_object();
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

        so->set_create(rg.nextBool());
        so->set_sobject(SQLObject::VIEW);
        v.setName(so->mutable_object()->mutable_est(), false);
    }
    else if (show_dictionary && nopt < (show_table + show_system_table + show_view + show_dictionary + 1))
    {
        ShowObject * so = st->mutable_object();
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(attached_dictionaries));

        so->set_create(rg.nextBool());
        so->set_sobject(SQLObject::DICTIONARY);
        d.setName(so->mutable_object()->mutable_est(), false);
    }
    else if (show_database && nopt < (show_table + show_system_table + show_view + show_dictionary + show_database + 1))
    {
        ShowObject * so = st->mutable_object();
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        so->set_create(rg.nextBool());
        so->set_sobject(SQLObject::DATABASE);
        d->setName(so->mutable_object()->mutable_database());
    }
    else if (show_databases && nopt < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + 1))
    {
        st->set_databases(rg.nextBool());
    }
    else if (
        show_database_dictionaries
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + 1))
    {
        ShowDictionaries * sd = st->mutable_dictionaries();
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        d->setName(sd->mutable_database());
    }
    else if (
        show_dictionaries
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + 1))
    {
        auto * const u = st->mutable_dictionaries();
        UNUSED(u);
    }
    else if (
        show_columns
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + 1))
    {
        ShowColumns * sc = st->mutable_columns();
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        sc->set_extended(rg.nextBool());
        sc->set_full(rg.nextBool());
        t.setName(sc->mutable_est(), false);
    }
    else if (
        show_columns_system_table
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + 1))
    {
        ShowColumns * sc = st->mutable_columns();

        sc->set_extended(rg.nextBool());
        sc->set_full(rg.nextBool());
        rg.pickRandomly(systemTables).setName(sc->mutable_est());
    }
    else if (
        show_indexes
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + 1))
    {
        ShowIndex * si = st->mutable_indexes();
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        si->set_extended(rg.nextBool());
        si->set_key(static_cast<ShowIndex_IndexShow>((rg.nextRandomUInt32() % static_cast<uint32_t>(ShowIndex::IndexShow_MAX)) + 1));
        t.setName(si->mutable_est(), false);
    }
    else if (
        show_indexes_system_table
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + 1))
    {
        ShowIndex * si = st->mutable_indexes();

        si->set_extended(rg.nextBool());
        si->set_key(static_cast<ShowIndex_IndexShow>((rg.nextRandomUInt32() % static_cast<uint32_t>(ShowIndex::IndexShow_MAX)) + 1));
        rg.pickRandomly(systemTables).setName(si->mutable_est());
    }
    else if (
        show_policies
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + 1))
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

        t.setName(st->mutable_policies(), false);
    }
    else if (
        show_policies_system_table
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + 1))
    {
        rg.pickRandomly(systemTables).setName(st->mutable_policies());
    }
    else if (
        show_processlist
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + 1))
    {
        st->set_processlist(rg.nextBool());
    }
    else if (
        show_grants
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + 1))
    {
        st->set_grants(rg.nextBool());
    }
    else if (
        show_users
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + 1))
    {
        st->set_users(rg.nextBool());
    }
    else if (
        show_roles
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + 1))
    {
        st->set_roles(rg.nextBool());
    }
    else if (
        show_profiles
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + 1))
    {
        st->set_profiles(rg.nextBool());
    }
    else if (
        show_quotas
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas + 1))
    {
        st->set_quotas(rg.nextBool());
    }
    else if (
        show_quota
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + 1))
    {
        st->set_quota(rg.nextBool());
    }
    else if (
        show_access
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + show_access + 1))
    {
        st->set_access(rg.nextBool());
    }
    else if (
        show_cluster
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + show_access + show_cluster + 1))
    {
        st->mutable_cluster()->set_cluster(rg.pickRandomly(fc.clusters));
    }
    else if (
        show_clusters
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + show_access + show_cluster + show_clusters + 1))
    {
        st->set_clusters(rg.nextBool());
    }
    else if (
        show_settings
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + show_access + show_cluster + show_clusters + show_settings + 1))
    {
        st->set_settings(rg.nextBool());
    }
    else if (
        show_filesystem_caches
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + show_access + show_cluster + show_clusters + show_settings + show_filesystem_caches + 1))
    {
        st->set_filesystem_caches(rg.nextBool());
    }
    else if (
        show_engines
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + show_access + show_cluster + show_clusters + show_settings + show_filesystem_caches + show_engines + 1))
    {
        st->set_engines(rg.nextBool());
    }
    else if (
        show_functions
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + show_access + show_cluster + show_clusters + show_settings + show_filesystem_caches + show_engines
               + show_functions + 1))
    {
        st->set_functions(rg.nextBool());
    }
    else if (
        show_merges
        && nopt
            < (show_table + show_system_table + show_view + show_dictionary + show_database + show_databases + show_database_dictionaries
               + show_dictionaries + show_columns + show_columns_system_table + show_indexes + show_indexes_system_table + show_policies
               + show_policies_system_table + show_processlist + show_grants + show_users + show_roles + show_profiles + show_quotas
               + show_quota + show_access + show_cluster + show_clusters + show_settings + show_filesystem_caches + show_engines
               + show_functions + show_merges + 1))
    {
        st->set_merges(rg.nextBool());
    }
    else
    {
        UNREACHABLE();
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, st->mutable_setting_values());
    }
}

std::optional<String> StatementGenerator::backupOrRestoreObject(BackupRestoreObject * bro, const SQLObject obj, const SQLBase & b)
{
    bro->set_is_temp(b.is_temp);
    bro->set_sobject(obj);
    return b.getCluster();
}

static void backupOrRestoreSystemTable(BackupRestoreObject * bro, const String & nschema, const String & ntable)
{
    ExprSchemaTable * est = bro->mutable_object()->mutable_est();

    bro->set_sobject(SQLObject::TABLE);
    est->mutable_database()->set_database(nschema);
    est->mutable_table()->set_table(ntable);
}

static std::optional<String> backupOrRestoreDatabase(BackupRestoreObject * bro, const std::shared_ptr<SQLDatabase> & d)
{
    bro->set_sobject(SQLObject::DATABASE);
    d->setName(bro->mutable_object()->mutable_database());
    return d->getCluster();
}

void StatementGenerator::setBackupDestination(RandomGenerator & rg, BackupRestore * br)
{
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

    /// Set backup file
    br->set_backup_number(backup_counter++);
    backup_file += std::to_string(br->backup_number());
    if (rg.nextSmallNumber() < 8)
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
        br->mutable_params()->add_out_params()->set_svalue(rg.pickRandomly(fc.disks));
        br->mutable_params()->add_out_params()->set_svalue(std::move(backup_file));
    }
    else if (out_to_file && (nopt2 < out_to_disk + out_to_file + 1))
    {
        outf = BackupRestore_BackupOutput_File;
        br->mutable_params()->add_out_params()->set_svalue((fc.server_file_path / std::move(backup_file)).generic_string());
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
        br->mutable_params()->add_out_params()->set_svalue(std::move(backup_file));
    }
    else if (out_to_null && nopt2 < (out_to_disk + out_to_file + out_to_s3 + out_to_azure + out_to_memory + out_to_null + 1))
    {
        outf = BackupRestore_BackupOutput_Null;
    }
    else
    {
        UNREACHABLE();
    }
    br->set_out(outf);
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
        const String dname = t.getDatabaseName();
        const String tname = t.getTableName();
        const bool table_has_partitions = t.isMergeTreeFamily() && fc.tableHasPartitions(false, dname, tname);

        t.setName(bro->mutable_object()->mutable_est(), false);
        cluster = backupOrRestoreObject(bro, SQLObject::TABLE, t);
        if (table_has_partitions && rg.nextSmallNumber() < 4)
        {
            bro->add_partitions()->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextRandomUInt64(), false, true, dname, tname));
        }
    }
    else if (backup_system_table && nopt < (backup_table + backup_system_table + 1))
    {
        const auto & ntable = rg.pickRandomly(systemTables);

        backupOrRestoreSystemTable(bre->mutable_bobject(), ntable.schema_name, ntable.table_name);
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
        UNREACHABLE();
    }
    if (cluster.has_value())
    {
        br->mutable_cluster()->set_cluster(cluster.value());
    }
    setBackupDestination(rg, br);

    if (rg.nextBool())
    {
        /// Most of the times, use formats that can be read later
        br->set_outformat(
            rg.nextBool() ? rg.pickRandomly(rg.pickRandomly(outFormats))
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
        const uint32_t restore_system_table
            = 3 * static_cast<uint32_t>(backup.system_table_schema.has_value() && backup.system_table_name.has_value());
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
            backupOrRestoreSystemTable(bre->mutable_bobject(), backup.system_table_schema.value(), backup.system_table_name.value());
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
            UNREACHABLE();
        }
    }

    if (cluster.has_value())
    {
        br->mutable_cluster()->set_cluster(cluster.value());
    }
    br->set_out(backup.outf);
    br->mutable_params()->CopyFrom(backup.out_params);
    if (backup.out_format.has_value())
    {
        br->set_informat(
            outIn.contains(backup.out_format.value()) && rg.nextBool()
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
        BackupParamsToString(info, backup.out_params);
        sv->set_value(std::move(info));
    }
    if (rg.nextSmallNumber() < 4)
    {
        br->set_sync(rg.nextSmallNumber() < 7 ? BackupRestore_SyncOrAsync_ASYNC : BackupRestore_SyncOrAsync_SYNC);
    }
    if (rg.nextSmallNumber() < 4)
    {
        vals = vals ? vals : br->mutable_setting_values();
        generateSettingValues(rg, formatSettings, vals);
    }
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
        UNREACHABLE();
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

void StatementGenerator::generateNextKill(RandomGenerator & rg, Kill * kil)
{
    BinaryExpr * bexpr = kil->mutable_where()->mutable_expr()->mutable_expr()->mutable_comp_expr()->mutable_binary_expr();

    bexpr->set_op(BinaryOperator::BINOP_EQ);
    bexpr->mutable_lhs()->mutable_comp_expr()->mutable_expr_stc()->mutable_col()->mutable_path()->mutable_col()->set_column("mutation_id");
    bexpr->mutable_rhs()->mutable_lit_val()->set_string_lit(fc.getRandomMutation(rg.nextRandomUInt64()));

    kil->set_command(Kill_KillEnum_MUTATION);
    if (rg.nextSmallNumber() < 3)
    {
        std::uniform_int_distribution<uint32_t> opt_range(1, static_cast<uint32_t>(Kill::KillOption_MAX));

        kil->set_option(static_cast<Kill_KillOption>(opt_range(rg.generator)));
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, kil->mutable_setting_values());
    }
}

void StatementGenerator::generateNextQuery(RandomGenerator & rg, const bool in_parallel, SQLQueryInner * sq)
{
    const bool has_databases = collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases);
    const bool has_tables = collectionHas<SQLTable>(attached_tables);
    const bool has_views = collectionHas<SQLView>(attached_views);
    const bool has_dictionaries = collectionHas<SQLDictionary>(attached_dictionaries);

    const uint32_t create_table = 12 * static_cast<uint32_t>(static_cast<uint32_t>(tables.size()) < this->fc.max_tables);
    const uint32_t create_view = 12 * static_cast<uint32_t>(static_cast<uint32_t>(views.size()) < this->fc.max_views);
    const uint32_t drop = 1
        * static_cast<uint32_t>(
                              !in_parallel
                              && (collectionCount<SQLTable>(attached_tables) > 3 || collectionCount<SQLView>(attached_views) > 3
                                  || collectionCount<SQLDictionary>(attached_dictionaries) > 3
                                  || collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3 || functions.size() > 3));
    const uint32_t insert = 180 * static_cast<uint32_t>(has_tables);
    const uint32_t light_delete = 6 * static_cast<uint32_t>(has_tables);
    const uint32_t truncate = 2 * static_cast<uint32_t>(has_databases || has_tables);
    const uint32_t optimize_table = 2 * static_cast<uint32_t>(has_tables);
    const uint32_t check_table = 2 * static_cast<uint32_t>(has_tables);
    const uint32_t desc_table = 2;
    const uint32_t exchange = 1
        * static_cast<uint32_t>(!in_parallel
                                && (collectionCount<SQLTable>(exchange_table_lambda) > 1 || collectionCount<SQLView>(attached_views) > 1
                                    || collectionCount<SQLDictionary>(attached_dictionaries) > 1));
    const uint32_t alter = 10 * static_cast<uint32_t>(has_tables || has_views || has_databases);
    const uint32_t set_values = 10;
    const uint32_t attach = 2
        * static_cast<uint32_t>(!in_parallel
                                && (collectionHas<SQLTable>(detached_tables) || collectionHas<SQLView>(detached_views)
                                    || collectionHas<SQLDictionary>(detached_dictionaries)
                                    || collectionHas<std::shared_ptr<SQLDatabase>>(detached_databases)));
    const uint32_t detach = 1
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
    const uint32_t light_update = 6 * static_cast<uint32_t>(has_tables);
    const uint32_t select_query = 800 * static_cast<uint32_t>(!in_parallel);
    const uint32_t kill = 2;
    const uint32_t show_stmt = 1;
    const uint32_t prob_space = create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table
        + desc_table + exchange + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
        + create_dictionary + rename + light_update + kill + show_stmt + select_query;
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
        generateNextUpdateOrDelete<LightDelete>(rg, sq->mutable_del());
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
        light_update
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
               + create_dictionary + rename + light_update + 1))
    {
        generateNextUpdateOrDelete<LightUpdate>(rg, sq->mutable_upt());
    }
    else if (
        kill
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
               + create_dictionary + rename + light_update + kill + 1))
    {
        generateNextKill(rg, sq->mutable_kill());
    }
    else if (
        show_stmt
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
               + create_dictionary + rename + light_update + kill + show_stmt + 1))
    {
        generateNextShowStatement(rg, sq->mutable_show());
    }
    else if (
        select_query
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + exchange
               + alter + set_values + attach + detach + create_database + create_function + system_stmt + backup_or_restore
               + create_dictionary + rename + light_update + kill + show_stmt + select_query + 1))
    {
        generateTopSelect(rg, false, std::numeric_limits<uint32_t>::max(), sq->mutable_select());
    }
    else
    {
        UNREACHABLE();
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
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_compact, trueOrFalseInt),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_run_query_tree_passes, trueOrFalseInt),
    ExplainOptValues(
        ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_query_tree_passes,
        [](RandomGenerator & rg) { return rg.randomInt<uint32_t>(0, 32); }),
    ExplainOptValues(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_projections, trueOrFalseInt)};

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
                    this->ids.insert(this->ids.end(), {0, 1});
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_SYNTAX:
                    this->ids.insert(this->ids.end(), {2, 17, 18});
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE:
                    this->ids.insert(this->ids.end(), {3, 4, 5, 6, 7});
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PLAN:
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_ESTIMATE:
                    this->ids.insert(this->ids.end(), {1, 8, 9, 10, 11, 12, 13, 14, 15, 19});
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PIPELINE:
                    this->ids.insert(this->ids.end(), {0, 15, 16});
                    break;
                default:
                    break;
            }
        }
        else
        {
            this->ids.insert(this->ids.end(), {1, 9, 10, 11, 12, 13, 14, 15, 19});
        }
        if (!this->ids.empty())
        {
            const size_t noptions = rg.randomInt<size_t>(1, this->ids.size());
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
    const uint32_t nqueries = rg.randomInt<uint32_t>(1, fc.max_parallel_queries);
    const uint32_t start_transaction = 2 * static_cast<uint32_t>(fc.allow_transactions && nqueries == 1 && !this->in_transaction);
    const uint32_t commit = 50 * static_cast<uint32_t>(fc.allow_transactions && nqueries == 1 && this->in_transaction);
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
            UNREACHABLE();
        }
    }
}

void StatementGenerator::dropTable(const bool staged, bool drop_peer, const uint32_t tname)
{
    auto & map_to_delete = staged ? this->staged_tables : this->tables;

    if (map_to_delete.contains(tname))
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
        const uint32_t tname = getIdentifierFromString(query.create_table().est().table().table());

        if (!ssq.explain().is_explain() && success)
        {
            if (query.create_table().create_opt() == CreateReplaceOption::Replace)
            {
                dropTable(false, true, tname);
            }
            if (!this->staged_tables[tname].random_engine)
            {
                this->tables[tname] = std::move(this->staged_tables[tname]);
            }
        }
        dropTable(true, !success, tname);
    }
    else if (ssq.has_explain() && query.has_create_view())
    {
        const uint32_t tname = getIdentifierFromString(query.create_view().est().table().table());

        if (!ssq.explain().is_explain() && success)
        {
            if (query.create_view().create_opt() == CreateReplaceOption::Replace)
            {
                this->views.erase(tname);
            }
            if (!this->staged_views[tname].random_engine)
            {
                this->views[tname] = std::move(this->staged_views[tname]);
            }
        }
        this->staged_views.erase(tname);
    }
    else if (ssq.has_explain() && query.has_create_dictionary())
    {
        const uint32_t dname = getIdentifierFromString(query.create_dictionary().est().table().table());

        if (!ssq.explain().is_explain() && success)
        {
            if (query.create_view().create_opt() == CreateReplaceOption::Replace)
            {
                this->dictionaries.erase(dname);
            }
            if (!this->staged_dictionaries[dname].random_engine)
            {
                this->dictionaries[dname] = std::move(this->staged_dictionaries[dname]);
            }
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
            dropTable(false, true, getIdentifierFromString(drp.object().est().table().table()));
        }
        else if (isview)
        {
            this->views.erase(getIdentifierFromString(drp.object().est().table().table()));
        }
        else if (isdictionary)
        {
            this->dictionaries.erase(getIdentifierFromString(drp.object().est().table().table()));
        }
        else if (isdatabase)
        {
            dropDatabase(getIdentifierFromString(drp.object().database().database()));
        }
        else if (isfunction)
        {
            this->functions.erase(getIdentifierFromString(drp.object().function().function()));
        }
    }
    else if (ssq.has_explain() && !ssq.explain().is_explain() && query.has_exchange() && success)
    {
        const Exchange & ex = query.exchange();
        const SQLObjectName & obj1 = ex.object1();
        const bool istable = obj1.has_est() && obj1.est().table().table()[0] == 't';
        const bool isview = obj1.has_est() && obj1.est().table().table()[0] == 'v';
        const bool isdictionary = obj1.has_est() && obj1.est().table().table()[0] == 'd';
        const uint32_t tname1 = getIdentifierFromString(obj1.est().table().table());
        const uint32_t tname2 = getIdentifierFromString(query.exchange().object2().est().table().table());

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
        const uint32_t old_tname = getIdentifierFromString(isdatabase ? oobj.database().database() : oobj.est().table().table());
        const uint32_t new_tname = getIdentifierFromString(isdatabase ? nobj.database().database() : nobj.est().table().table());
        std::optional<uint32_t> new_db;

        if (!isdatabase && nobj.est().database().database() != "default")
        {
            new_db = getIdentifierFromString(nobj.est().database().database());
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
            SQLView & v = this->views[getIdentifierFromString(at.object().est().table().table())];

            for (int i = 0; i < at.other_alters_size() + 1; i++)
            {
                const AlterItem & ati = i == 0 ? at.alter() : at.other_alters(i - 1);

                if (success && ati.has_modify_query() && !v.has_with_cols)
                {
                    v.cols.clear();
                    for (uint32_t j = 0; j < v.staged_ncols; j++)
                    {
                        v.cols.insert(j);
                    }
                }
                v.is_refreshable |= (success && ati.has_refresh());
            }
        }
        else if (istable)
        {
            SQLTable & t = this->tables[getIdentifierFromString(at.object().est().table().table())];

            for (int i = 0; i < at.other_alters_size() + 1; i++)
            {
                const AlterItem & ati = i == 0 ? at.alter() : at.other_alters(i - 1);

                chassert(!ati.has_modify_query() && !ati.has_refresh());
                if (ati.has_add_column())
                {
                    const bool is_nested = ati.add_column().new_col().col().sub_cols_size() > 0;
                    const Column & cstr = is_nested
                        ? ati.add_column().new_col().col().sub_cols(ati.add_column().new_col().col().sub_cols_size() - 1)
                        : ati.add_column().new_col().col().col();
                    const uint32_t cname = getIdentifierFromString(cstr.column());

                    if (is_nested && !success)
                    {
                        const uint32_t top_col = getIdentifierFromString(ati.add_column().new_col().col().col().column());
                        NestedType * ntp = dynamic_cast<NestedType *>(t.cols.at(top_col).tp);

                        ntp->subtypes.pop_back();
                    }
                    else if (!is_nested)
                    {
                        if (success)
                        {
                            t.cols[cname] = std::move(t.staged_cols[cname]);
                        }
                        t.staged_cols.erase(cname);
                    }
                }
                else if (ati.has_drop_column() && success)
                {
                    const ColumnPath & path = ati.drop_column();
                    const uint32_t cname = getIdentifierFromString(path.col().column());

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
                            const uint32_t ncname = getIdentifierFromString(path.sub_cols(0).column());

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
                    const uint32_t old_cname = getIdentifierFromString(path.col().column());

                    if (path.sub_cols_size() == 0)
                    {
                        const uint32_t new_cname = getIdentifierFromString(ati.rename_column().new_name().col().column());

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
                            const uint32_t nocname = getIdentifierFromString(path.sub_cols(0).column());

                            for (auto it = ntp->subtypes.begin(), next_it = it; it != ntp->subtypes.end(); it = next_it)
                            {
                                ++next_it;
                                if (it->cname == nocname)
                                {
                                    it->cname = getIdentifierFromString(ati.rename_column().new_name().sub_cols(0).column());
                                    break;
                                }
                            }
                        }
                    }
                }
                else if (ati.has_modify_column())
                {
                    const bool is_nested = ati.modify_column().new_col().col().sub_cols_size() > 0;
                    const Column & cstr = is_nested
                        ? ati.modify_column().new_col().col().sub_cols(ati.modify_column().new_col().col().sub_cols_size() - 1)
                        : ati.modify_column().new_col().col().col();
                    const uint32_t cname = getIdentifierFromString(cstr.column());

                    if (is_nested)
                    {
                        const uint32_t top_col = getIdentifierFromString(ati.modify_column().new_col().col().col().column());

                        if (success)
                        {
                            NestedType * ntp = dynamic_cast<NestedType *>(t.cols.at(top_col).tp);

                            for (auto & entry : ntp->subtypes)
                            {
                                if (entry.cname == cname)
                                {
                                    SQLColumn & ncol = t.staged_cols.at(cname);
                                    delete entry.subtype;
                                    entry.subtype = ncol.tp;
                                    ncol.tp = nullptr;
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        if (success)
                        {
                            t.cols.erase(cname);
                            t.cols[cname] = std::move(t.staged_cols[cname]);
                        }
                    }
                    t.staged_cols.erase(cname);
                }
                else if (
                    ati.has_column_remove_property() && success
                    && ati.column_remove_property().property() < RemoveColumnProperty_ColumnProperties_CODEC)
                {
                    const ColumnPath & path = ati.column_remove_property().col();
                    const uint32_t cname = getIdentifierFromString(path.col().column());

                    if (path.sub_cols_size() == 0)
                    {
                        t.cols.at(cname).dmod = std::nullopt;
                    }
                }
                else if (ati.has_add_index())
                {
                    const uint32_t iname = getIdentifierFromString(ati.add_index().new_idx().idx().index());

                    if (success)
                    {
                        t.idxs[iname] = std::move(t.staged_idxs[iname]);
                    }
                    t.staged_idxs.erase(iname);
                }
                else if (ati.has_drop_index() && success)
                {
                    const uint32_t iname = getIdentifierFromString(ati.drop_index().index());

                    t.idxs.erase(iname);
                }
                else if (ati.has_add_projection())
                {
                    const uint32_t pname = getIdentifierFromString(ati.add_projection().proj().projection());

                    if (success)
                    {
                        t.projs.insert(pname);
                    }
                    t.staged_projs.erase(pname);
                }
                else if (ati.has_remove_projection() && success)
                {
                    const uint32_t pname = getIdentifierFromString(ati.remove_projection().projection());

                    t.projs.erase(pname);
                }
                else if (ati.has_add_constraint())
                {
                    const uint32_t pname = getIdentifierFromString(ati.add_constraint().constr().constraint());

                    if (success)
                    {
                        t.constrs.insert(pname);
                    }
                    t.staged_constrs.erase(pname);
                }
                else if (ati.has_remove_constraint() && success)
                {
                    const uint32_t pname = getIdentifierFromString(ati.remove_constraint().constraint());

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
        const uint32_t tname = getIdentifierFromString(isdatabase ? oobj.database().database() : oobj.est().table().table());
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
        const uint32_t dname = getIdentifierFromString(query.create_database().database().database());

        if (!ssq.explain().is_explain() && success && !this->staged_databases[dname]->random_engine)
        {
            this->databases[dname] = std::move(this->staged_databases[dname]);
        }
        this->staged_databases.erase(dname);
    }
    else if (ssq.has_explain() && query.has_create_function())
    {
        const uint32_t fname = getIdentifierFromString(query.create_function().function().function());

        if (!ssq.explain().is_explain() && success)
        {
            this->functions[fname] = std::move(this->staged_functions[fname]);
        }
        this->staged_functions.erase(fname);
    }
    else if (ssq.has_explain() && !ssq.explain().is_explain() && query.has_trunc() && query.trunc().has_database())
    {
        dropDatabase(getIdentifierFromString(query.trunc().database().database()));
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
            newb.out_params.CopyFrom(br.params());
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
                const ExprSchemaTable & est = bro.object().est();

                if (!est.has_database()
                    || (est.database().database() != "system" && est.database().database() != "INFORMATION_SCHEMA"
                        && est.database().database() != "information_schema"))
                {
                    const uint32_t tname = getIdentifierFromString(est.table().table());

                    newb.tables[tname] = this->tables[tname];
                    if (bro.partitions_size())
                    {
                        newb.partition_id = bro.partitions(0).partition_id();
                    }
                }
                else
                {
                    newb.system_table_schema = est.database().database();
                    newb.system_table_name = est.table().table();
                }
            }
            else if (bre.has_bobject() && bre.bobject().sobject() == SQLObject::VIEW)
            {
                const uint32_t vname = getIdentifierFromString(bre.bobject().object().est().table().table());

                newb.views[vname] = this->views[vname];
            }
            else if (bre.has_bobject() && bre.bobject().sobject() == SQLObject::DICTIONARY)
            {
                const uint32_t dname = getIdentifierFromString(bre.bobject().object().est().table().table());

                newb.dictionaries[dname] = this->dictionaries[dname];
            }
            else if (bre.has_bobject() && bre.bobject().sobject() == SQLObject::DATABASE)
            {
                const uint32_t dname = getIdentifierFromString(bre.bobject().object().database().database());

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
                    if (!val.db || this->databases.contains(val.db->dname))
                    {
                        this->tables[key] = val;
                    }
                }
                for (const auto & [key, val] : backup.views)
                {
                    if (!val.db || this->databases.contains(val.db->dname))
                    {
                        this->views[key] = val;
                    }
                }
                for (const auto & [key, val] : backup.dictionaries)
                {
                    if (!val.db || this->databases.contains(val.db->dname))
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
