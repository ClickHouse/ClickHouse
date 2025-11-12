#include <cstdio>

#include <Common/checkStackSize.h>

#include <Client/BuzzHouse/Generator/QueryOracle.h>
#include <Common/ErrorCodes.h>
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

const std::vector<std::vector<OutFormat>> QueryOracle::oracleFormats
    = {{OutFormat::OUT_CSV},
       {OutFormat::OUT_JSON,
        OutFormat::OUT_JSONColumns,
        OutFormat::OUT_JSONColumnsWithMetadata,
        OutFormat::OUT_JSONCompact,
        OutFormat::OUT_JSONCompactColumns,
        OutFormat::OUT_JSONCompactEachRow,
        OutFormat::OUT_JSONCompactStringsEachRow,
        OutFormat::OUT_JSONEachRow,
        OutFormat::OUT_JSONLines,
        OutFormat::OUT_JSONObjectEachRow,
        OutFormat::OUT_JSONStringsEachRow},
       {OutFormat::OUT_TabSeparated, OutFormat::OUT_TabSeparatedRaw},
       {OutFormat::OUT_Values}};

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

    can_test_oracle_result = fc.compare_success_results && rg.nextBool();
    gen.setAllowEngineUDF(!can_test_oracle_result);
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
    gen.levels.clear();
    gen.ctes.clear();
    gen.setAllowNotDetermistic(true);
    gen.enforceFinal(false);
    gen.setAllowEngineUDF(true);

    ts->set_format(OutFormat::OUT_CSV);
    /// If the file fails to be removed due to a legitimate way, the oracle will fail anyway
    const auto err = std::filesystem::remove(qcfile);
    UNUSED(err);
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
    const auto err = std::filesystem::remove(qcfile);
    UNUSED(err);
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

void QueryOracle::addLimitOrOffset(RandomGenerator & rg, StatementGenerator & gen, SelectStatementCore * ssc) const
{
    const uint32_t noption = rg.nextSmallNumber();

    if (noption < 3)
    {
        gen.setAllowNotDetermistic(false);
        gen.enforceFinal(true);
        gen.setAllowEngineUDF(false);
        if (noption == 1)
        {
            gen.generateLimit(rg, ssc->has_orderby(), ssc->mutable_limit());
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
    RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, const bool peer, TableOrFunction * tof) const
{
    const std::optional<String> & cluster = t.getCluster();
    const bool replaceable = !peer && !cluster.has_value() && t.isEngineReplaceable() && rg.nextBool();

    if (peer || cluster.has_value() || replaceable || rg.nextMediumNumber() < 16)
    {
        const TableFunctionUsage usage = peer
            ? TableFunctionUsage::PeerTable
            : (cluster.has_value() ? TableFunctionUsage::ClusterCall
                                   : (replaceable ? TableFunctionUsage::EngineReplace : TableFunctionUsage::RemoteCall));

        gen.setAllowNotDetermistic(false);
        gen.setTableFunction(rg, usage, t, tof->mutable_tfunc());
        gen.setAllowNotDetermistic(true);
    }
    else
    {
        /// Use insert into table
        t.setName(tof->mutable_est(), false);
    }
}

/// Dump and read table oracle
void QueryOracle::dumpTableContent(
    RandomGenerator & rg, StatementGenerator & gen, const bool test_content, const SQLTable & t, SQLQuery & sq1)
{
    bool first = true;
    TopSelect * ts = sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    Select * sel = ts->mutable_sel();
    SelectStatementCore * ssc = sel->mutable_select_core();
    JoinedTableOrFunction * jtf = ssc->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
    OrderByList * obs = ssc->mutable_orderby()->mutable_olist();

    insertOnTableOrCluster(rg, gen, t, false, jtf->mutable_tof());
    jtf->set_final(t.supportsFinal());

    gen.flatTableColumnPath(0, t.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
    for (const auto & entry : gen.entries)
    {
        ExprOrderingTerm * eot = first ? obs->mutable_ord_term() : obs->add_extra_ord_terms();

        gen.columnPathRef(entry, ssc->add_result_columns()->mutable_etc()->mutable_col()->mutable_path());
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

    addLimitOrOffset(rg, gen, ssc);
    if (test_content)
    {
        /// Don't write statistics
        if (!sel->has_setting_values())
        {
            const auto * news = sel->mutable_setting_values();
            UNUSED(news);
        }
        SettingValues & svs = const_cast<SettingValues &>(sel->setting_values());
        SetValue * sv = svs.has_set_value() ? svs.add_other_values() : svs.mutable_set_value();

        sv->set_property("output_format_write_statistics");
        sv->set_value("0");
    }
    ts->set_format(rg.pickRandomly(rg.pickRandomly(QueryOracle::oracleFormats)));
    const auto err = std::filesystem::remove(qcfile);
    UNUSED(err);
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

void QueryOracle::generateExportQuery(
    RandomGenerator & rg, StatementGenerator & gen, const bool test_content, const SQLTable & t, SQLQuery & sq2)
{
    Insert * ins = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();
    FileFunc * ff = ins->mutable_tof()->mutable_tfunc()->mutable_file();
    Expr * expr = ff->mutable_structure();
    SelectParen * sparen = ins->mutable_select();
    SelectStatementCore * sel = sparen->mutable_select()->mutable_select_core();
    const std::filesystem::path & cnfile = fc.client_file_path / "table.data";
    const std::filesystem::path & snfile = fc.server_file_path / "table.data";

    can_test_oracle_result &= test_content;
    /// Remove the file if exists
    const auto err = std::filesystem::remove(cnfile);
    UNUSED(err);
    ff->set_path(snfile.generic_string());
    ff->set_fname(FileFunc_FName::FileFunc_FName_file);

    gen.flatTableColumnPath(skip_nested_node | flat_nested, t.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
    if (!can_test_oracle_result && rg.nextSmallNumber() < 3)
    {
        /// Sometimes generate a not matching structure
        gen.addRandomRelation(rg, std::nullopt, gen.entries.size(), expr);
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
                entry.columnPathRef(),
                entry.path.size() > 1 ? "Array(" : "",
                entry.getBottomType()->typeName(false, false),
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
    ff->set_outformat(
        rg.pickRandomly(rg.pickRandomly(can_test_oracle_result ? QueryOracle::oracleFormats : StatementGenerator::outFormats)));
    if (rg.nextSmallNumber() < 4)
    {
        ff->set_fcomp(rg.pickRandomly(compressionMethods));
    }
    if (rg.nextSmallNumber() < 10)
    {
        const auto & settings = can_test_oracle_result ? serverSettings : formatSettings;
        gen.generateSettingValues(rg, settings, ins->mutable_setting_values());
        const SettingValues & svs = ins->setting_values();

        for (int i = 0; i < (svs.other_values_size() + 1) && can_test_oracle_result; i++)
        {
            const SetValue & osv = i == 0 ? svs.set_value() : svs.other_values(i - 1);
            const CHSetting & ochs = settings.at(osv.property());

            can_test_oracle_result &= !ochs.changes_behavior;
        }
    }
    /// Set the table on select
    JoinedTableOrFunction * jtf = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();

    insertOnTableOrCluster(rg, gen, t, false, jtf->mutable_tof());
    jtf->set_final(t.supportsFinal());
}

void QueryOracle::dumpOracleIntermediateSteps(
    RandomGenerator & rg,
    StatementGenerator & gen,
    const SQLTable & t,
    const DumpOracleStrategy strategy,
    const bool test_content,
    std::vector<SQLQuery> & intermediate_queries)
{
    intermediate_queries.clear();
    switch (strategy)
    {
        case DumpOracleStrategy::DUMP_TABLE: {
            SQLQuery next1;
            SQLQuery next2;
            SQLQuery next3;
            const std::optional<String> & cluster = t.getCluster();
            const auto & t2
                = test_content ? t : rg.pickRandomly(gen.filterCollection<BuzzHouse::SQLTable>(gen.attached_tables_to_test_format)).get();

            /// Export data
            generateExportQuery(rg, gen, test_content, t, next1);
            /// Truncate table, then insert everything again
            Truncate * trunc = next2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_trunc();

            t.setName(trunc->mutable_est(), false);
            if (cluster.has_value())
            {
                trunc->mutable_cluster()->set_cluster(cluster.value());
            }
            /// Import data again
            generateImportQuery(rg, gen, t2, next1, next3);

            intermediate_queries.emplace_back(next1);
            intermediate_queries.emplace_back(next2);
            intermediate_queries.emplace_back(next3);
        }
        break;
        case DumpOracleStrategy::OPTIMIZE: {
            SQLQuery next;

            gen.generateNextOptimizeTableInternal(
                rg, t, true, next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_opt());
            intermediate_queries.emplace_back(next);
        }
        break;
        case DumpOracleStrategy::REATTACH: {
            SQLQuery next1;
            SQLQuery next2;
            const std::optional<String> & cluster = t.getCluster();
            Detach * det = next1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_detach();
            Attach * att = next2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_attach();

            det->set_sobject(SQLObject::TABLE);
            att->set_sobject(SQLObject::TABLE);
            t.setName(det->mutable_object()->mutable_est(), false);
            t.setName(att->mutable_object()->mutable_est(), false);

            det->set_permanently(rg.nextBool());
            det->set_sync(true);
            if (cluster.has_value())
            {
                det->mutable_cluster()->set_cluster(cluster.value());
                att->mutable_cluster()->set_cluster(cluster.value());
            }
            if (rg.nextSmallNumber() < 4)
            {
                gen.generateSettingValues(rg, serverSettings, det->mutable_setting_values());
            }
            if (rg.nextSmallNumber() < 4)
            {
                gen.generateSettingValues(rg, serverSettings, att->mutable_setting_values());
            }
            intermediate_queries.emplace_back(next1);
            intermediate_queries.emplace_back(next2);
        }
        break;
        case DumpOracleStrategy::BACKUP_RESTORE: {
            SQLQuery next1;
            SQLQuery next2;
            std::optional<String> cluster;
            BackupRestore * bac = next1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_backup_restore();
            BackupRestore * res = next2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_backup_restore();
            SettingValues * bac_vals = nullptr;
            SettingValues * res_vals = nullptr;

            bac->set_command(BackupRestore_BackupCommand_BACKUP);
            res->set_command(BackupRestore_BackupCommand_RESTORE);
            cluster = gen.backupOrRestoreObject(bac->mutable_backup_element()->mutable_bobject(), SQLObject::TABLE, t);
            cluster = gen.backupOrRestoreObject(res->mutable_backup_element()->mutable_bobject(), SQLObject::TABLE, t);
            if (cluster.has_value())
            {
                bac->mutable_cluster()->set_cluster(cluster.value());
                res->mutable_cluster()->set_cluster(cluster.value());
            }

            gen.setBackupDestination(rg, bac);
            res->set_backup_number(bac->backup_number());
            res->set_out(bac->out());
            res->mutable_params()->CopyFrom(bac->params());

            bac->set_sync(BackupRestore_SyncOrAsync_SYNC);
            res->set_sync(BackupRestore_SyncOrAsync_SYNC);
            if (rg.nextSmallNumber() < 4)
            {
                bac_vals = bac->mutable_setting_values();
                gen.generateSettingValues(rg, backupSettings, bac_vals);
            }
            if (rg.nextSmallNumber() < 4)
            {
                bac_vals = bac_vals ? bac_vals : bac->mutable_setting_values();
                gen.generateSettingValues(rg, formatSettings, bac_vals);
            }
            if (rg.nextSmallNumber() < 4)
            {
                res_vals = res->mutable_setting_values();
                gen.generateSettingValues(rg, restoreSettings, res_vals);
            }
            if (rg.nextSmallNumber() < 4)
            {
                res_vals = res_vals ? res_vals : res->mutable_setting_values();
                gen.generateSettingValues(rg, formatSettings, res_vals);
            }
            intermediate_queries.emplace_back(next1);
            intermediate_queries.emplace_back(next2);
        }
        break;
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
    const InFormat & inf = (!can_test_oracle_result && rg.nextSmallNumber() < 4) ? rg.pickValueRandomlyFromMap(StatementGenerator::outIn)
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

    if (!can_test_oracle_result && rg.nextSmallNumber() < 10)
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
    if (can_test_oracle_result && inf == InFormat::IN_CSV)
    {
        svs = svs ? svs : nins->mutable_setting_values();
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        /// The oracle expects to read all the lines from the file
        sv->set_property("input_format_csv_detect_header");
        sv->set_value("0");
    }
}

/// Run query with different settings oracle
bool QueryOracle::generateFirstSetting(RandomGenerator & rg, SQLQuery & sq1)
{
    const bool use_settings = rg.nextMediumNumber() < 86;

    /// Most of the times use SET command, other times SYSTEM
    if (use_settings)
    {
        std::uniform_int_distribution<uint32_t> settings_range(1, 10);
        const uint32_t nsets = settings_range(rg.generator);
        SettingValues * sv = sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_setting_values();

        nsettings.clear();
        for (uint32_t i = 0; i < nsets; i++)
        {
            const auto & toPickFrom = (hotSettings.empty() || rg.nextMediumNumber() < 94) ? queryOracleSettings : hotSettings;
            const String & setting = rg.pickRandomly(toPickFrom);
            const CHSetting & chs = queryOracleSettings.at(setting);
            SetValue * setv = i == 0 ? sv->mutable_set_value() : sv->add_other_values();

            setv->set_property(setting);
            if (chs.oracle_values.size() == 2)
            {
                if (setting == "enable_analyzer")
                {
                    /// For the analyzer, always run the old first, so we can minimize the usage of it
                    setv->set_value("0");
                    nsettings.push_back("1");
                }
                else if (rg.nextBool())
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
                const String & fvalue = rg.pickRandomly(chs.oracle_values);
                String svalue = rg.pickRandomly(chs.oracle_values);

                for (uint32_t j = 0; j < 4 && fvalue == svalue; j++)
                {
                    /// Pick another value until they are different
                    svalue = rg.pickRandomly(chs.oracle_values);
                }
                setv->set_value(fvalue);
                nsettings.push_back(svalue);
            }
            can_test_oracle_result &= !chs.changes_behavior;
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
    bool indexes = false;
    Select * sel = nullptr;
    Select * query = nullptr;
    SelectParen * sparen = nullptr;
    const uint32_t ncols = rg.randomInt<uint32_t>(1, 5);

    peer_query = pq;
    if (peer_query == PeerQuery::ClickHouseOnly && (fc.measure_performance || fc.compare_explains) && rg.nextBool())
    {
        const bool next_opt = rg.nextBool();

        measure_performance = fc.measure_performance && (!fc.compare_explains || next_opt);
        compare_explain = fc.compare_explains && (!fc.measure_performance || !next_opt);
    }
    const bool global_aggregate = !measure_performance && !compare_explain && rg.nextSmallNumber() < 4;

    if (measure_performance)
    {
        /// When measuring performance, don't insert into file
        sel = query = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select()->mutable_sel();
    }
    else
    {
        Insert * ins = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();
        sparen = ins->mutable_select();
        FileFunc * ff = ins->mutable_tof()->mutable_tfunc()->mutable_file();
        OutFormat outf = rg.pickRandomly(rg.pickRandomly(QueryOracle::oracleFormats));

        const auto err = std::filesystem::remove(qcfile);
        UNUSED(err);
        ff->set_path(qsfile.generic_string());
        if (peer_query == PeerQuery::ClickHouseOnly && outf == OutFormat::OUT_Parquet)
        {
            /// ClickHouse prints server version on Parquet file, making checksum incompatible between versions
            outf = OutFormat::OUT_CSV;
        }
        ff->set_outformat(outf);
        ff->set_fname(FileFunc_FName::FileFunc_FName_file);
        sel = query = sparen->mutable_select();
    }

    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.generatingPeerQuery(pq);
    gen.setAllowEngineUDF(peer_query != PeerQuery::ClickHouseOnly);
    if (compare_explain)
    {
        /// INSERT INTO FILE EXPLAIN SELECT is not supported, so run
        /// INSERT INTO FILE SELECT * FROM (EXPLAIN SELECT);
        SelectStatementCore * nsel = query->mutable_select_core();
        JoinedTableOrFunction * jtf = nsel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
        ExplainQuery * eq = jtf->mutable_tof()->mutable_select();

        if (rg.nextBool())
        {
            ExplainOption * eopt = eq->add_opts();

            eopt->set_opt(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_indexes);
            eopt->set_val(1);
            indexes = true;
        }
        if (rg.nextBool())
        {
            ExplainOption * eopt = eq->add_opts();

            eopt->set_opt(ExplainOption_ExplainOpt::ExplainOption_ExplainOpt_actions);
            eopt->set_val(1);
        }
        eq->set_is_explain(true);
        /// Remove `__set_` words
        /// regexp_replace(line, '__set_[0-9]+', '')
        ExprColAlias * eca = nsel->add_result_columns()->mutable_eca();
        SQLFuncCall * fcall = eca->mutable_expr()->mutable_comp_expr()->mutable_func_call();

        eca->mutable_col_alias()->set_column("explain");
        fcall->mutable_func()->set_catalog_func(SQLFunc::FUNCREGEXP_REPLACE);
        fcall->add_args()
            ->mutable_expr()
            ->mutable_comp_expr()
            ->mutable_expr_stc()
            ->mutable_col()
            ->mutable_path()
            ->mutable_col()
            ->set_column("explain");
        fcall->add_args()->mutable_expr()->mutable_lit_val()->set_no_quote_str("'__set_[0-9]+'");
        fcall->add_args()->mutable_expr()->mutable_lit_val()->set_no_quote_str("''");

        /// Filter out granules and parts read, because rows are being inserted all at once.
        /// WHERE NOT match(explain, '^\ *(Parts|Granules|Ranges):')
        UnaryExpr * uexpr = nsel->mutable_where()->mutable_expr()->mutable_expr()->mutable_comp_expr()->mutable_unary_expr();
        SQLFuncCall * fcall2 = uexpr->mutable_expr()->mutable_comp_expr()->mutable_func_call();

        uexpr->set_unary_op(UnaryOperator::UNOP_NOT);
        fcall2->mutable_func()->set_catalog_func(SQLFunc::FUNCmatch);
        fcall2->add_args()
            ->mutable_expr()
            ->mutable_comp_expr()
            ->mutable_expr_stc()
            ->mutable_col()
            ->mutable_path()
            ->mutable_col()
            ->set_column("explain");
        fcall2->add_args()->mutable_expr()->mutable_lit_val()->set_no_quote_str("'^\\ *(Parts|Granules|Ranges):'");
        jtf->mutable_table_alias()->set_table("ex");
        jtf->add_col_aliases()->set_column("explain");

        query = eq->mutable_inner_query()->mutable_select()->mutable_sel();
    }
    gen.resetAliasCounter();
    gen.generateSelect(rg, true, global_aggregate, ncols, std::numeric_limits<uint32_t>::max(), std::nullopt, query);
    gen.setAllowNotDetermistic(true);
    gen.enforceFinal(false);
    gen.generatingPeerQuery(PeerQuery::None);
    gen.setAllowEngineUDF(true);

    if (!measure_performance && !compare_explain && !global_aggregate)
    {
        /// If not global aggregate, use ORDER BY clause
        Select * osel = sparen->release_select();
        sel = sparen->mutable_select();
        SelectStatementCore * nsel = sel->mutable_select_core();
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
        addLimitOrOffset(rg, gen, nsel);
    }

    /// Don't write statistics
    if (!sel->has_setting_values())
    {
        const auto * news = sel->mutable_setting_values();
        UNUSED(news);
    }
    SettingValues & svs = const_cast<SettingValues &>(sel->setting_values());
    SetValue * sv = svs.has_set_value() ? svs.add_other_values() : svs.mutable_set_value();

    sv->set_property("output_format_write_statistics");
    sv->set_value("0");
    if (measure_performance)
    {
        /// Add tag to find query later on
        SetValue * sv2 = svs.add_other_values();

        sv2->set_property("log_comment");
        sv2->set_value("'measure_performance'");
    }
    else if (indexes)
    {
        /// These settings are relevant to show index information
        SetValue * sv2 = svs.add_other_values();
        SetValue * sv3 = svs.add_other_values();

        sv2->set_property("use_query_condition_cache");
        sv2->set_value("0");
        sv3->set_property("use_skip_indexes_on_data_read");
        sv3->set_value("0");
    }
}

void QueryOracle::swapQuery(RandomGenerator & rg, google::protobuf::Message & mes)
{
    checkStackSize();

    if (mes.GetTypeName() == "BuzzHouse.Select")
    {
        auto & sel = static_cast<Select &>(mes);

        if (sel.has_select_core())
        {
            swapQuery(rg, const_cast<SelectStatementCore &>(sel.select_core()));
        }
        else if (sel.has_set_query())
        {
            swapQuery(rg, const_cast<SetQuery &>(sel.set_query()));
        }
        if (sel.has_ctes())
        {
            if (sel.ctes().cte().has_cte_query())
            {
                swapQuery(rg, const_cast<Select &>(sel.ctes().cte().cte_query().query()));
            }
            for (int i = 0; i < sel.ctes().other_ctes_size(); i++)
            {
                if (sel.ctes().other_ctes(i).has_cte_query())
                {
                    swapQuery(rg, const_cast<Select &>(sel.ctes().other_ctes(i).cte_query().query()));
                }
            }
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.SetQuery")
    {
        auto & setq = static_cast<SetQuery &>(mes);

        swapQuery(rg, const_cast<Select &>(setq.sel1().inner_query().select().sel()));
        swapQuery(rg, const_cast<Select &>(setq.sel2().inner_query().select().sel()));
    }
    else if (mes.GetTypeName() == "BuzzHouse.SelectStatementCore")
    {
        auto & ssc = static_cast<SelectStatementCore &>(mes);

        if ((ssc.has_pre_where() || ssc.has_where()) && rg.nextSmallNumber() < 5)
        {
            /// Swap WHERE and PREWHERE
            auto * prewhere = ssc.release_pre_where();
            auto * where = ssc.release_where();

            ssc.set_allocated_pre_where(where);
            ssc.set_allocated_where(prewhere);
        }
        if (ssc.has_from())
        {
            swapQuery(rg, const_cast<JoinedQuery &>(ssc.from().tos()));
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.JoinedQuery")
    {
        auto & jquery = static_cast<JoinedQuery &>(mes);

        for (int i = 0; i < jquery.tos_list_size(); i++)
        {
            swapQuery(rg, const_cast<TableOrSubquery &>(jquery.tos_list(i)));
        }
        swapQuery(rg, const_cast<JoinClause &>(jquery.join_clause()));
    }
    else if (mes.GetTypeName() == "BuzzHouse.JoinClause")
    {
        auto & jclause = static_cast<JoinClause &>(mes);

        for (int i = 0; i < jclause.clauses_size(); i++)
        {
            if (jclause.clauses(i).has_core())
            {
                swapQuery(rg, const_cast<TableOrSubquery &>(jclause.clauses(i).core().tos()));
            }
        }
        swapQuery(rg, const_cast<TableOrSubquery &>(jclause.tos()));
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableOrSubquery")
    {
        auto & tos = static_cast<TableOrSubquery &>(mes);

        if (tos.has_joined_table())
        {
            auto & jtf = const_cast<JoinedTableOrFunction &>(tos.joined_table());

            swapQuery(rg, const_cast<TableOrFunction &>(jtf.tof()));
        }
        else if (tos.has_joined_query())
        {
            swapQuery(rg, const_cast<JoinedQuery &>(tos.joined_query()));
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableFunction")
    {
        auto & tfunc = static_cast<TableFunction &>(mes);

        if (tfunc.has_loop())
        {
            swapQuery(rg, const_cast<TableOrFunction &>(tfunc.loop()));
        }
        else if (tfunc.has_remote() || tfunc.has_cluster())
        {
            swapQuery(rg, const_cast<TableOrFunction &>(tfunc.has_remote() ? tfunc.remote().tof() : tfunc.cluster().tof()));
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableOrFunction")
    {
        auto & torfunc = static_cast<TableOrFunction &>(mes);

        if (torfunc.has_tfunc())
        {
            swapQuery(rg, const_cast<TableFunction &>(torfunc.tfunc()));
        }
        else if (torfunc.has_select())
        {
            swapQuery(rg, const_cast<Select &>(torfunc.select().inner_query().select().sel()));
        }
    }
}

void QueryOracle::maybeUpdateOracleSelectQuery(RandomGenerator & rg, const SQLQuery & sq1, SQLQuery & sq2)
{
    sq2.CopyFrom(sq1);
    chassert(!compare_explain);
    if (rg.nextBool())
    {
        /// Swap query parts
        const SQLQueryInner & sq2inner = sq2.single_query().explain().inner_query();
        Select & nsel = const_cast<Select &>(measure_performance ? sq2inner.select().sel() : sq2inner.insert().select().select());

        swapQuery(rg, nsel);
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

            if ((!est.has_database()
                 || (est.database().database() != "system" && est.database().database() != "INFORMATION_SCHEMA"
                     && est.database().database() != "information_schema"))
                && est.table().table().at(0) == 't')
            {
                const uint32_t tname = gen.getIdentifierFromString(est.table().table());

                if (gen.tables.contains(tname))
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
                        can_test_oracle_result &= t.hasClickHousePeer();
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
    Select & nsel = const_cast<Select &>(measure_performance ? sq2inner.select().sel() : sq2inner.insert().select().select());
    /// Replace references
    const auto u = findTablesWithPeersAndReplace(rg, nsel, gen, peer_query != PeerQuery::ClickHouseOnly);
    UNUSED(u);
    if (peer_query == PeerQuery::ClickHouseOnly && !measure_performance)
    {
        /// Use a different file for the peer database
        FileFunc & ff = const_cast<FileFunc &>(sq2.single_query().explain().inner_query().insert().tof().tfunc().file());

        const auto err = std::filesystem::remove(qfile_peer);
        UNUSED(err);
        ff.set_path(qfile_peer.generic_string());
    }
    for (const auto & entry : found_tables)
    {
        SQLQuery next2;
        const SQLTable & t = gen.tables.at(entry);
        Insert * ins = next2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();
        SelectStatementCore * sel = ins->mutable_select()->mutable_select()->mutable_select_core();

        if (t.isMergeTreeFamily())
        {
            /// Apply delete mask
            SQLQuery next;
            const std::optional<String> & cluster = t.getCluster();
            Alter * alter = next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_alter();

            alter->set_sobject(SQLObject::TABLE);
            t.setName(alter->mutable_object()->mutable_est(), false);
            if (cluster.has_value())
            {
                alter->mutable_cluster()->set_cluster(cluster.value());
            }
            alter->mutable_alter()->mutable_delete_mask();
            SetValue * sv = alter->mutable_setting_values()->mutable_set_value();
            sv->set_property("mutations_sync");
            sv->set_value("2");
            peer_queries.emplace_back(next);
        }
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
        peer_queries.emplace_back(next2);
    }
}

void QueryOracle::resetOracleValues()
{
    peer_query = PeerQuery::AllPeers;
    compare_explain = false;
    measure_performance = false;
    first_errcode = 0;
    other_steps_sucess = true;
    can_test_oracle_result = fc.compare_success_results;
    res1 = PerformanceResult();
    res2 = PerformanceResult();
}

void QueryOracle::setIntermediateStepSuccess(const bool success)
{
    other_steps_sucess &= success;
}

void QueryOracle::processFirstOracleQueryResult(const int errcode, ExternalIntegrations & ei)
{
    if (!errcode)
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
    first_errcode = errcode;
}

void QueryOracle::processSecondOracleQueryResult(const int errcode, ExternalIntegrations & ei, const String & oracle_name)
{
    if (other_steps_sucess && can_test_oracle_result)
    {
        if (((first_errcode && !errcode) || (!first_errcode && errcode))
            && (fc.oracle_ignore_error_codes.find(static_cast<uint32_t>(first_errcode ? first_errcode : errcode))
                == fc.oracle_ignore_error_codes.end()))
        {
            throw DB::Exception(
                DB::ErrorCodes::BUZZHOUSE,
                "{}: failed with different success results: {} vs {}",
                oracle_name,
                DB::ErrorCodes::getName(first_errcode),
                DB::ErrorCodes::getName(errcode));
        }
        if (!first_errcode && !errcode)
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
