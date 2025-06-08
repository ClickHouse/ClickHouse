#include <cstdio>

#include <Client/BuzzHouse/Generator/QueryOracle.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BUZZHOUSE;
}
}

namespace BuzzHouse
{

/// Correctness query oracle
/// SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED>;
/// or
/// SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE> HAVING <PRED2>;
void QueryOracle::generateCorrectnessTestFirstQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq1)
{
    TopSelect * ts = sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore * ssc = ts->mutable_sel()->mutable_select_core();
    /// TODO fix this 0 WHERE, 1 HAVING, 2 WHERE + HAVING
    const uint32_t combination = 0;

    can_test_query_success = fc.compare_success_results && rg.nextBool();
    gen.setAllowEngineUDF(!can_test_query_success);
    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.resetAliasCounter();
    gen.levels[gen.current_level] = QueryLevel(gen.current_level);
    const auto u = gen.generateFromStatement(rg, std::numeric_limits<uint32_t>::max(), ssc->mutable_from());

    UNUSED(u);
    const bool prev_allow_aggregates = gen.levels[gen.current_level].allow_aggregates;
    const bool prev_allow_window_funcs = gen.levels[gen.current_level].allow_window_funcs;
    gen.levels[gen.current_level].allow_aggregates = gen.levels[gen.current_level].allow_window_funcs = false;
    if (combination != 1)
    {
        BinaryExpr * bexpr = ssc->mutable_where()->mutable_expr()->mutable_expr()->mutable_comp_expr()->mutable_binary_expr();

        bexpr->set_op(BinaryOperator::BINOP_EQ);
        bexpr->mutable_rhs()->mutable_lit_val()->mutable_special_val()->set_val(
            SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_TRUE);
        gen.generateWherePredicate(rg, bexpr->mutable_lhs());
    }
    if (combination != 0)
    {
        gen.generateGroupBy(rg, 1, true, true, ssc->mutable_groupby());
    }
    gen.levels[gen.current_level].allow_aggregates = prev_allow_aggregates;
    gen.levels[gen.current_level].allow_window_funcs = prev_allow_window_funcs;

    ssc->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call()->mutable_func()->set_catalog_func(
        FUNCcount);
    gen.levels.erase(gen.current_level);
    gen.setAllowNotDetermistic(true);
    gen.enforceFinal(false);
    gen.setAllowEngineUDF(true);

    ts->set_format(OutFormat::OUT_CSV);
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

/// SELECT ifNull(SUM(PRED),0) FROM <FROM_CLAUSE>;
/// or
/// SELECT ifNull(SUM(PRED2),0) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE>;
void QueryOracle::generateCorrectnessTestSecondQuery(SQLQuery & sq1, SQLQuery & sq2)
{
    TopSelect * ts = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore & ssc1 = const_cast<SelectStatementCore &>(sq1.single_query().explain().inner_query().select().sel().select_core());
    SelectStatementCore * ssc2 = ts->mutable_sel()->mutable_select_core();
    SQLFuncCall * sfc1 = ssc2->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call();
    SQLFuncCall * sfc2 = sfc1->add_args()->mutable_expr()->mutable_comp_expr()->mutable_func_call();

    sfc1->mutable_func()->set_catalog_func(FUNCifNull);
    sfc1->add_args()->mutable_expr()->mutable_lit_val()->mutable_special_val()->set_val(
        SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO);
    sfc2->mutable_func()->set_catalog_func(FUNCsum);

    ssc2->set_allocated_from(ssc1.release_from());
    if (ssc1.has_groupby())
    {
        ExprComparisonHighProbability & expr = const_cast<ExprComparisonHighProbability &>(ssc1.groupby().having_expr().expr());

        sfc2->add_args()->set_allocated_expr(expr.release_expr());
        ssc2->set_allocated_groupby(ssc1.release_groupby());
        ssc2->set_allocated_where(ssc1.release_where());
    }
    else
    {
        ExprComparisonHighProbability & expr = const_cast<ExprComparisonHighProbability &>(ssc1.where().expr());

        sfc2->add_args()->set_allocated_expr(expr.release_expr());
    }
    ts->set_format(sq1.single_query().explain().inner_query().select().format());
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

void QueryOracle::addLimitOrOffset(RandomGenerator & rg, StatementGenerator & gen, const uint32_t ncols, SelectStatementCore * ssc) const
{
    const uint32_t noption = rg.nextSmallNumber();

    if (noption < 3)
    {
        gen.setAllowNotDetermistic(false);
        gen.enforceFinal(true);
        gen.setAllowEngineUDF(false);
        if (noption == 1)
        {
            gen.generateLimit(rg, ssc->has_orderby(), ncols, ssc->mutable_limit());
        }
        else
        {
            gen.generateOffset(rg, ssc->has_orderby(), ssc->mutable_offset());
        }
        gen.setAllowNotDetermistic(true);
        gen.enforceFinal(false);
        gen.setAllowEngineUDF(true);
        gen.levels.clear();
    }
}

void QueryOracle::insertOnTableOrCluster(
    RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, const bool remote, TableOrFunction * tof) const
{
    const std::optional<String> & cluster = t.getCluster();

    if (remote)
    {
        gen.setTableRemote(rg, false, true, t, tof->mutable_tfunc());
    }
    else if (cluster.has_value())
    {
        /// If the table is set on cluster, always insert to all replicas/shards
        ClusterFunc * cdf = tof->mutable_tfunc()->mutable_cluster();

        cdf->set_all_replicas(true);
        cdf->mutable_cluster()->set_cluster(cluster.has_value() ? cluster.value() : rg.pickRandomly(fc.clusters));
        t.setName(cdf->mutable_tof()->mutable_est(), true);
        if (rg.nextSmallNumber() < 4)
        {
            /// Optional sharding key
            gen.flatTableColumnPath(to_remote_entries, t.cols, [](const SQLColumn &) { return true; });
            cdf->set_sharding_key(rg.pickRandomly(gen.remote_entries).getBottomName());
            gen.remote_entries.clear();
        }
    }
    else
    {
        /// Use insert into table
        t.setName(tof->mutable_est(), false);
    }
}

/// Dump and read table oracle
void QueryOracle::dumpTableContent(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, SQLQuery & sq1)
{
    bool first = true;
    TopSelect * ts = sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore * sel = ts->mutable_sel()->mutable_select_core();
    JoinedTableOrFunction * jtf = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
    OrderByList * obs = sel->mutable_orderby()->mutable_olist();

    insertOnTableOrCluster(rg, gen, t, false, jtf->mutable_tof());
    jtf->set_final(t.supportsFinal());

    gen.flatTableColumnPath(0, t.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
    const uint32_t ncols = static_cast<uint32_t>(gen.entries.size());
    for (const auto & entry : gen.entries)
    {
        ExprOrderingTerm * eot = first ? obs->mutable_ord_term() : obs->add_extra_ord_terms();

        gen.columnPathRef(entry, sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_path());
        gen.columnPathRef(entry, eot->mutable_expr()->mutable_comp_expr()->mutable_expr_stc()->mutable_col()->mutable_path());
        if (rg.nextBool())
        {
            eot->set_asc_desc(rg.nextBool() ? AscDesc::ASC : AscDesc::DESC);
        }
        if (rg.nextBool())
        {
            eot->set_nulls_order(
                rg.nextBool() ? ExprOrderingTerm_NullsOrder::ExprOrderingTerm_NullsOrder_FIRST
                              : ExprOrderingTerm_NullsOrder::ExprOrderingTerm_NullsOrder_LAST);
        }
        first = false;
    }
    gen.entries.clear();

    addLimitOrOffset(rg, gen, ncols, sel);
    ts->set_format(rg.pickRandomly(StatementGenerator::outIn));
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

void QueryOracle::generateExportQuery(
    RandomGenerator & rg, StatementGenerator & gen, const bool test_content, const SQLTable & t, SQLQuery & sq2)
{
    std::error_code ec;
    Insert * ins = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();
    FileFunc * ff = ins->mutable_tof()->mutable_tfunc()->mutable_file();
    Expr * expr = ff->mutable_structure();
    SelectStatementCore * sel = ins->mutable_select()->mutable_select_core();
    const std::filesystem::path & cnfile = fc.client_file_path / "table.data";
    const std::filesystem::path & snfile = fc.server_file_path / "table.data";
    OutFormat outf = rg.pickRandomly(StatementGenerator::outIn);

    can_test_query_success &= test_content;
    /// Remove the file if exists
    if (!std::filesystem::remove(cnfile, ec) && ec)
    {
        LOG_ERROR(fc.log, "Could not remove file: {}", ec.message());
    }
    ff->set_path(snfile.generic_string());
    ff->set_fname(FileFunc_FName::FileFunc_FName_file);

    gen.flatTableColumnPath(skip_nested_node | flat_nested, t.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
    if (!can_test_query_success && rg.nextSmallNumber() < 3)
    {
        /// Sometimes generate a not matching structure
        gen.addRandomRelation(rg, std::nullopt, gen.entries.size(), false, expr);
    }
    else
    {
        String buf;
        bool first = true;

        for (const auto & entry : gen.entries)
        {
            buf += fmt::format(
                "{}{} {}{}{}{}",
                first ? "" : ", ",
                entry.getBottomName(),
                entry.path.size() > 1 ? "Array(" : "",
                entry.getBottomType()->typeName(false),
                entry.path.size() > 1 ? ")" : "",
                (entry.path.size() == 1 && entry.nullable.has_value()) ? (entry.nullable.value() ? " NULL" : " NOT NULL") : "");
            first = false;
        }
        expr->mutable_lit_val()->set_string_lit(std::move(buf));
    }
    for (const auto & entry : gen.entries)
    {
        gen.columnPathRef(entry, sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_path());
    }
    gen.entries.clear();
    ff->set_outformat(outf);
    if (rg.nextSmallNumber() < 4)
    {
        ff->set_fcomp(static_cast<FileCompression>((rg.nextRandomUInt32() % static_cast<uint32_t>(FileCompression_MAX)) + 1));
    }
    if (rg.nextSmallNumber() < 10)
    {
        const auto & settings = can_test_query_success ? serverSettings : formatSettings;
        gen.generateSettingValues(rg, settings, ins->mutable_setting_values());
        const SettingValues & svs = ins->setting_values();

        for (int i = 0; i < (svs.other_values_size() + 1) && can_test_query_success; i++)
        {
            const SetValue & osv = i == 0 ? svs.set_value() : svs.other_values(i - 1);
            const CHSetting & ochs = settings.at(osv.property());

            can_test_query_success &= !ochs.changes_behavior;
        }
    }
    /// Set the table on select
    JoinedTableOrFunction * jtf = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();

    insertOnTableOrCluster(rg, gen, t, false, jtf->mutable_tof());
    jtf->set_final(t.supportsFinal());
}

void QueryOracle::dumpOracleIntermediateStep(
    RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, const bool use_optimize, SQLQuery & sq3)
{
    SQLQueryInner * sq = sq3.mutable_single_query()->mutable_explain()->mutable_inner_query();

    if (use_optimize)
    {
        gen.generateNextOptimizeTableInternal(rg, t, true, sq->mutable_opt());
    }
    else
    {
        /// Truncate table, then insert everything again
        const std::optional<String> & cluster = t.getCluster();
        Truncate * trunc = sq->mutable_trunc();

        t.setName(trunc->mutable_est(), false);
        if (cluster.has_value())
        {
            trunc->mutable_cluster()->set_cluster(cluster.value());
        }
    }
}

void QueryOracle::generateImportQuery(
    RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, const SQLQuery & sq2, SQLQuery & sq4) const
{
    SettingValues * svs = nullptr;
    Insert * nins = sq4.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();
    InsertFromFile * iff = nins->mutable_insert_file();
    const Insert & oins = sq2.single_query().explain().inner_query().insert();
    const FileFunc & ff = oins.tof().tfunc().file();
    const InFormat & inf = (!can_test_query_success && rg.nextSmallNumber() < 4) ? rg.pickValueRandomlyFromMap(StatementGenerator::outIn)
                                                                                 : StatementGenerator::outIn.at(ff.outformat());

    insertOnTableOrCluster(rg, gen, t, false, nins->mutable_tof());
    gen.flatTableColumnPath(skip_nested_node | flat_nested, t.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
    for (const auto & entry : gen.entries)
    {
        gen.columnPathRef(entry, nins->add_cols());
    }
    gen.entries.clear();
    const std::string & base_filename = ff.path().substr(ff.path().find_last_of(std::filesystem::path::preferred_separator) + 1);
    const std::filesystem::path & ifile = fc.client_file_path / base_filename;
    iff->set_path(ifile.generic_string());
    iff->set_format(inf);
    if (ff.has_fcomp())
    {
        iff->set_fcomp(ff.fcomp());
    }

    if (!can_test_query_success && rg.nextSmallNumber() < 10)
    {
        /// If can't test success, swap settings sometimes
        svs = nins->mutable_setting_values();
        gen.generateSettingValues(rg, formatSettings, svs);
    }
    else if (oins.has_setting_values())
    {
        svs = nins->mutable_setting_values();
        svs->CopyFrom(oins.setting_values());
    }
    if ((can_test_query_success && inf == InFormat::IN_CSV) || inf == InFormat::IN_Parquet)
    {
        svs = svs ? svs : nins->mutable_setting_values();
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        if (inf == InFormat::IN_Parquet)
        {
            /// Use available Parquet readers
            sv->set_property("input_format_parquet_use_native_reader");
            sv->set_value(rg.nextBool() ? "1" : "0");
        }
        else
        {
            /// The oracle expects to read all the lines from the file
            sv->set_property("input_format_csv_detect_header");
            sv->set_value("0");
        }
    }
}

/// Run query with different settings oracle
bool QueryOracle::generateFirstSetting(RandomGenerator & rg, SQLQuery & sq1)
{
    const bool use_settings = rg.nextMediumNumber() < 86;

    /// Most of the times use SET command, other times SYSTEM
    if (use_settings)
    {
        const uint32_t nsets = rg.nextBool() ? 1 : ((rg.nextSmallNumber() % 3) + 1);
        SettingValues * sv = sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_setting_values();

        nsettings.clear();
        for (uint32_t i = 0; i < nsets; i++)
        {
            const auto & toPickFrom = rg.nextMediumNumber() < 5 ? hotSettings : queryOracleSettings;
            const String & setting = rg.pickRandomly(toPickFrom);
            const CHSetting & chs = queryOracleSettings.at(setting);
            SetValue * setv = i == 0 ? sv->mutable_set_value() : sv->add_other_values();

            setv->set_property(setting);
            if (chs.oracle_values.size() == 2)
            {
                if (rg.nextBool())
                {
                    setv->set_value(*chs.oracle_values.begin());
                    nsettings.push_back(*std::next(chs.oracle_values.begin(), 1));
                }
                else
                {
                    setv->set_value(*std::next(chs.oracle_values.begin(), 1));
                    nsettings.push_back(*(chs.oracle_values.begin()));
                }
            }
            else
            {
                setv->set_value(rg.pickRandomly(chs.oracle_values));
                nsettings.push_back(rg.pickRandomly(chs.oracle_values));
            }
            can_test_query_success &= !chs.changes_behavior;
        }
    }
    return use_settings;
}

void QueryOracle::generateSecondSetting(
    RandomGenerator & rg, StatementGenerator & gen, const bool use_settings, const SQLQuery & sq1, SQLQuery & sq3)
{
    SQLQueryInner * sq = sq3.mutable_single_query()->mutable_explain()->mutable_inner_query();

    if (use_settings)
    {
        const SettingValues & osv = sq1.single_query().explain().inner_query().setting_values();
        SettingValues * sv = sq->mutable_setting_values();

        for (size_t i = 0; i < nsettings.size(); i++)
        {
            const SetValue & osetv = i == 0 ? osv.set_value() : osv.other_values(static_cast<int>(i - 1));
            SetValue * setv = i == 0 ? sv->mutable_set_value() : sv->add_other_values();

            setv->set_property(osetv.property());
            setv->set_value(nsettings[i]);
        }
    }
    else
    {
        gen.generateNextSystemStatement(rg, false, sq->mutable_system_cmd());
    }
}

void QueryOracle::generateOracleSelectQuery(RandomGenerator & rg, const PeerQuery pq, StatementGenerator & gen, SQLQuery & sq2)
{
    std::error_code ec;
    bool explain = false;
    Select * sel = nullptr;
    Insert * ins = nullptr;
    const uint32_t ncols = (rg.nextMediumNumber() % 5) + UINT32_C(1);

    peer_query = pq;
    if (peer_query == PeerQuery::ClickHouseOnly && (fc.measure_performance || fc.compare_explains) && rg.nextBool())
    {
        const uint32_t next_opt = rg.nextSmallNumber();

        measure_performance = !fc.compare_explains || next_opt < 7;
        explain = !fc.measure_performance || next_opt > 6;
    }
    const bool global_aggregate = !measure_performance && !explain && rg.nextSmallNumber() < 4;

    if (measure_performance)
    {
        /// When measuring performance, don't insert into file
        sel = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select()->mutable_sel();
    }
    else
    {
        ins = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();
        FileFunc * ff = ins->mutable_tof()->mutable_tfunc()->mutable_file();
        OutFormat outf = rg.pickRandomly(StatementGenerator::outIn);

        if (!std::filesystem::remove(qcfile, ec) && ec)
        {
            LOG_ERROR(fc.log, "Could not remove file: {}", ec.message());
        }
        ff->set_path(qsfile.generic_string());
        if (peer_query == PeerQuery::ClickHouseOnly && outf == OutFormat::OUT_Parquet)
        {
            /// ClickHouse prints server version on Parquet file, making checksum incompatible between versions
            outf = OutFormat::OUT_CSV;
        }
        ff->set_outformat(outf);
        ff->set_fname(FileFunc_FName::FileFunc_FName_file);
        sel = ins->mutable_select();
    }

    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.generatingPeerQuery(pq);
    gen.setAllowEngineUDF(peer_query != PeerQuery::ClickHouseOnly);
    if (explain)
    {
        /// INSERT INTO FILE EXPLAIN SELECT is not supported, so run
        /// INSERT INTO FILE SELECT * FROM (EXPLAIN SELECT);
        ExplainQuery * eq = sel->mutable_select_core()
                                ->mutable_from()
                                ->mutable_tos()
                                ->mutable_join_clause()
                                ->mutable_tos()
                                ->mutable_joined_table()
                                ->mutable_tof()
                                ->mutable_select();

        if (rg.nextBool())
        {
            ExplainOption * eopt = eq->add_opts();

            eopt->set_opt(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_indexes);
            eopt->set_val(1);
        }
        if (rg.nextBool())
        {
            ExplainOption * eopt = eq->add_opts();

            eopt->set_opt(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_actions);
            eopt->set_val(1);
        }
        eq->set_is_explain(true);
        sel = eq->mutable_inner_query()->mutable_select()->mutable_sel();
    }
    gen.resetAliasCounter();
    gen.generateSelect(rg, true, global_aggregate, ncols, std::numeric_limits<uint32_t>::max(), sel);
    gen.setAllowNotDetermistic(true);
    gen.enforceFinal(false);
    gen.generatingPeerQuery(PeerQuery::None);
    gen.setAllowEngineUDF(true);

    if (!measure_performance && !explain && !global_aggregate)
    {
        /// If not global aggregate, use ORDER BY clause
        Select * osel = ins->release_select();
        SelectStatementCore * nsel = ins->mutable_select()->mutable_select_core();
        nsel->mutable_from()
            ->mutable_tos()
            ->mutable_join_clause()
            ->mutable_tos()
            ->mutable_joined_table()
            ->mutable_tof()
            ->mutable_select()
            ->mutable_inner_query()
            ->mutable_select()
            ->set_allocated_sel(osel);
        nsel->mutable_orderby()->set_oall(true);
        addLimitOrOffset(rg, gen, ncols, nsel);
    }
    else if (measure_performance)
    {
        /// Add tag to find query later on
        if (!sel->has_setting_values())
        {
            const auto * news = sel->mutable_setting_values();
            UNUSED(news);
        }
        SettingValues & svs = const_cast<SettingValues &>(sel->setting_values());
        SetValue * sv = svs.has_set_value() ? svs.add_other_values() : svs.mutable_set_value();

        sv->set_property("log_comment");
        sv->set_value("'measure_performance'");
    }
}

bool QueryOracle::findTablesWithPeersAndReplace(
    RandomGenerator & rg, google::protobuf::Message & mes, StatementGenerator & gen, const bool replace)
{
    checkStackSize();

    if (mes.GetTypeName() == "BuzzHouse.Select")
    {
        auto & sel = static_cast<Select &>(mes);

        if (sel.has_select_core())
        {
            const auto u = findTablesWithPeersAndReplace(rg, const_cast<SelectStatementCore &>(sel.select_core()), gen, replace);
            UNUSED(u);
        }
        else if (sel.has_set_query())
        {
            const auto u = findTablesWithPeersAndReplace(rg, const_cast<SetQuery &>(sel.set_query()), gen, replace);
            UNUSED(u);
        }
        if (sel.has_ctes())
        {
            if (sel.ctes().cte().has_cte_query())
            {
                const auto u = findTablesWithPeersAndReplace(rg, const_cast<Select &>(sel.ctes().cte().cte_query().query()), gen, replace);
                UNUSED(u);
            }
            for (int i = 0; i < sel.ctes().other_ctes_size(); i++)
            {
                if (sel.ctes().other_ctes(i).has_cte_query())
                {
                    const auto u = findTablesWithPeersAndReplace(
                        rg, const_cast<Select &>(sel.ctes().other_ctes(i).cte_query().query()), gen, replace);
                    UNUSED(u);
                }
            }
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.SetQuery")
    {
        auto & setq = static_cast<SetQuery &>(mes);

        const auto u = findTablesWithPeersAndReplace(rg, const_cast<Select &>(setq.sel1().inner_query().select().sel()), gen, replace);
        const auto w = findTablesWithPeersAndReplace(rg, const_cast<Select &>(setq.sel2().inner_query().select().sel()), gen, replace);

        UNUSED(u);
        UNUSED(w);
    }
    else if (mes.GetTypeName() == "BuzzHouse.SelectStatementCore")
    {
        auto & ssc = static_cast<SelectStatementCore &>(mes);

        if (ssc.has_from())
        {
            const auto u = findTablesWithPeersAndReplace(rg, const_cast<JoinedQuery &>(ssc.from().tos()), gen, replace);
            UNUSED(u);
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.JoinedQuery")
    {
        auto & jquery = static_cast<JoinedQuery &>(mes);

        for (int i = 0; i < jquery.tos_list_size(); i++)
        {
            const auto u = findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jquery.tos_list(i)), gen, replace);
            UNUSED(u);
        }
        const auto u = findTablesWithPeersAndReplace(rg, const_cast<JoinClause &>(jquery.join_clause()), gen, replace);
        UNUSED(u);
    }
    else if (mes.GetTypeName() == "BuzzHouse.JoinClause")
    {
        auto & jclause = static_cast<JoinClause &>(mes);

        for (int i = 0; i < jclause.clauses_size(); i++)
        {
            if (jclause.clauses(i).has_core())
            {
                const auto u
                    = findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jclause.clauses(i).core().tos()), gen, replace);
                UNUSED(u);
            }
        }
        const auto u = findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jclause.tos()), gen, replace);
        UNUSED(u);
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableOrSubquery")
    {
        auto & tos = static_cast<TableOrSubquery &>(mes);

        if (tos.has_joined_table())
        {
            auto & jtf = const_cast<JoinedTableOrFunction &>(tos.joined_table());

            const auto has_external_peer = findTablesWithPeersAndReplace(rg, const_cast<TableOrFunction &>(jtf.tof()), gen, replace);
            /// Remove final for MySQL and PostgreSQL calls
            jtf.set_final(jtf.final() && !has_external_peer);
        }
        else if (tos.has_joined_query())
        {
            return findTablesWithPeersAndReplace(rg, const_cast<JoinedQuery &>(tos.joined_query()), gen, replace);
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableFunction")
    {
        auto & tfunc = static_cast<TableFunction &>(mes);

        if (tfunc.has_loop())
        {
            return findTablesWithPeersAndReplace(rg, const_cast<TableOrFunction &>(tfunc.loop()), gen, replace);
        }
        else if (tfunc.has_remote() || tfunc.has_cluster())
        {
            return findTablesWithPeersAndReplace(
                rg, const_cast<TableOrFunction &>(tfunc.has_remote() ? tfunc.remote().tof() : tfunc.cluster().tof()), gen, replace);
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableOrFunction")
    {
        auto & torfunc = static_cast<TableOrFunction &>(mes);

        if (torfunc.has_est())
        {
            bool res = false;
            const ExprSchemaTable & est = torfunc.est();

            if ((!est.has_database() || est.database().database() != "system") && est.table().table().at(0) == 't')
            {
                const uint32_t tname = static_cast<uint32_t>(std::stoul(est.table().table().substr(1)));

                if (gen.tables.find(tname) != gen.tables.end())
                {
                    const SQLTable & t = gen.tables.at(tname);

                    if (t.hasDatabasePeer())
                    {
                        if (replace)
                        {
                            insertOnTableOrCluster(rg, gen, t, true, &torfunc);
                        }
                        found_tables.insert(tname);
                        res = !t.hasClickHousePeer();
                        can_test_query_success &= t.hasClickHousePeer();
                    }
                }
            }
            return res;
        }
        else if (torfunc.has_tfunc())
        {
            return findTablesWithPeersAndReplace(rg, const_cast<TableFunction &>(torfunc.tfunc()), gen, replace);
        }
        else if (torfunc.has_select())
        {
            return findTablesWithPeersAndReplace(rg, const_cast<Select &>(torfunc.select().inner_query().select().sel()), gen, replace);
        }
    }
    return false;
}

void QueryOracle::truncatePeerTables(const StatementGenerator & gen)
{
    for (const auto & entry : found_tables)
    {
        /// First truncate tables
        other_steps_sucess &= gen.connections.truncatePeerTableOnRemote(gen.tables.at(entry));
    }
}

void QueryOracle::optimizePeerTables(const StatementGenerator & gen)
{
    for (const auto & entry : found_tables)
    {
        /// Lastly optimize tables
        const auto & ntable = gen.tables.at(entry);

        other_steps_sucess &= gen.connections.optimizeTableForOracle(PeerTableDatabase::ClickHouse, ntable);
        if (measure_performance)
        {
            other_steps_sucess &= gen.connections.optimizeTableForOracle(PeerTableDatabase::None, ntable);
        }
    }
}

void QueryOracle::replaceQueryWithTablePeers(
    RandomGenerator & rg, const SQLQuery & sq1, StatementGenerator & gen, std::vector<SQLQuery> & peer_queries, SQLQuery & sq2)
{
    found_tables.clear();
    peer_queries.clear();

    sq2.CopyFrom(sq1);
    const SQLQueryInner & sq2inner = sq2.single_query().explain().inner_query();
    Select & nsel = const_cast<Select &>(measure_performance ? sq2inner.select().sel() : sq2inner.insert().select());
    /// Replace references
    const auto u = findTablesWithPeersAndReplace(rg, nsel, gen, peer_query != PeerQuery::ClickHouseOnly);
    UNUSED(u);
    if (peer_query == PeerQuery::ClickHouseOnly && !measure_performance)
    {
        /// Use a different file for the peer database
        std::error_code ec;
        FileFunc & ff = const_cast<FileFunc &>(sq2.single_query().explain().inner_query().insert().tof().tfunc().file());

        if (!std::filesystem::remove(qfile_peer, ec) && ec)
        {
            LOG_ERROR(fc.log, "Could not remove file: {}", ec.message());
        }
        ff.set_path(qfile_peer.generic_string());
    }
    for (const auto & entry : found_tables)
    {
        SQLQuery next;
        const SQLTable & t = gen.tables.at(entry);
        Insert * ins = next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();
        SelectStatementCore * sel = ins->mutable_select()->mutable_select_core();

        /// Then insert the data
        insertOnTableOrCluster(rg, gen, t, true, ins->mutable_tof());
        JoinedTableOrFunction * jtf = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();

        insertOnTableOrCluster(rg, gen, t, false, jtf->mutable_tof());
        jtf->set_final(t.supportsFinal());
        gen.flatTableColumnPath(skip_nested_node | flat_nested, t.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
        for (const auto & colRef : gen.entries)
        {
            gen.columnPathRef(colRef, ins->add_cols());
            gen.columnPathRef(colRef, sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_path());
        }
        gen.entries.clear();
        peer_queries.emplace_back(next);
    }
}

void QueryOracle::resetOracleValues()
{
    peer_query = PeerQuery::AllPeers;
    measure_performance = false;
    first_success = other_steps_sucess = true;
    can_test_query_success = fc.compare_success_results;
    res1 = PerformanceResult();
    res2 = PerformanceResult();
}

void QueryOracle::setIntermediateStepSuccess(const bool success)
{
    other_steps_sucess &= success;
}

void QueryOracle::processFirstOracleQueryResult(const bool success, ExternalIntegrations & ei)
{
    if (success)
    {
        if (measure_performance)
        {
            other_steps_sucess &= ei.getPerformanceMetricsForLastQuery(PeerTableDatabase::None, this->res1);
        }
        else
        {
            md5_hash1.hashFile(qcfile.generic_string(), first_digest);
        }
    }
    first_success = success;
}

void QueryOracle::processSecondOracleQueryResult(const bool success, ExternalIntegrations & ei, const String & oracle_name)
{
    if (other_steps_sucess)
    {
        if (can_test_query_success && first_success != success)
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "{}: failed with different success results", oracle_name);
        }
        if (first_success && success)
        {
            if (measure_performance)
            {
                if (ei.getPerformanceMetricsForLastQuery(PeerTableDatabase::ClickHouse, this->res2))
                {
                    fc.comparePerformanceResults(oracle_name, this->res1, this->res2);
                }
            }
            else
            {
                md5_hash2.hashFile((peer_query == PeerQuery::ClickHouseOnly ? qfile_peer : qcfile).generic_string(), second_digest);
                if (first_digest != second_digest)
                {
                    throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "{}: failed with different result sets", oracle_name);
                }
            }
        }
    }
}

}
