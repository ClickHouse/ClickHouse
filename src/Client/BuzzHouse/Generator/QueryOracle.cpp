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
    TopSelect * ts = sq1.mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore * ssc = ts->mutable_sel()->mutable_select_core();
    /// TODO fix this 0 WHERE, 1 HAVING, 2 WHERE + HAVING
    const uint32_t combination = 0;

    can_test_query_success = fc.compare_success_results && rg.nextBool();
    gen.setAllowEngineUDF(!can_test_query_success);
    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.levels[gen.current_level] = QueryLevel(gen.current_level);
    gen.generateFromStatement(rg, std::numeric_limits<uint32_t>::max(), ssc->mutable_from());

    const bool prev_allow_aggregates = gen.levels[gen.current_level].allow_aggregates;
    const bool prev_allow_window_funcs = gen.levels[gen.current_level].allow_window_funcs;
    gen.levels[gen.current_level].allow_aggregates = gen.levels[gen.current_level].allow_window_funcs = false;
    if (combination != 1)
    {
        BinaryExpr * bexpr = ssc->mutable_where()->mutable_expr()->mutable_expr()->mutable_comp_expr()->mutable_binary_expr();

        bexpr->set_op(BinaryOperator::BINOP_EQ);
        bexpr->mutable_rhs()->mutable_lit_val()->set_special_val(SpecialVal::VAL_TRUE);
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
    sif->set_path(qfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

/// SELECT ifNull(SUM(PRED),0) FROM <FROM_CLAUSE>;
/// or
/// SELECT ifNull(SUM(PRED2),0) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY <GROUP_BY CLAUSE>;
void QueryOracle::generateCorrectnessTestSecondQuery(SQLQuery & sq1, SQLQuery & sq2)
{
    TopSelect * ts = sq2.mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore & ssc1 = const_cast<SelectStatementCore &>(sq1.explain().inner_query().select().sel().select_core());
    SelectStatementCore * ssc2 = ts->mutable_sel()->mutable_select_core();
    SQLFuncCall * sfc1 = ssc2->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call();
    SQLFuncCall * sfc2 = sfc1->add_args()->mutable_expr()->mutable_comp_expr()->mutable_func_call();

    sfc1->mutable_func()->set_catalog_func(FUNCifNull);
    sfc1->add_args()->mutable_expr()->mutable_lit_val()->set_special_val(SpecialVal::VAL_ZERO);
    sfc2->mutable_func()->set_catalog_func(FUNCsum);

    ssc2->set_allocated_from(ssc1.release_from());
    if (ssc1.has_groupby())
    {
        GroupByStatement & gbs = const_cast<GroupByStatement &>(ssc1.groupby());

        sfc2->add_args()->set_allocated_expr(gbs.release_having_expr());
        ssc2->set_allocated_groupby(ssc1.release_groupby());
        ssc2->set_allocated_where(ssc1.release_where());
    }
    else
    {
        ExprComparisonHighProbability & expr = const_cast<ExprComparisonHighProbability &>(ssc1.where().expr());

        sfc2->add_args()->set_allocated_expr(expr.release_expr());
    }
    ts->set_format(OutFormat::OUT_CSV);
    sif->set_path(qfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

/// Dump and read table oracle
void QueryOracle::dumpTableContent(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, SQLQuery & sq1)
{
    bool first = true;
    TopSelect * ts = sq1.mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    SelectStatementCore * sel = ts->mutable_sel()->mutable_select_core();
    JoinedTableOrFunction * jtf = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
    OrderByList * obs = sel->mutable_orderby()->mutable_olist();
    ExprSchemaTable * est = jtf->mutable_tof()->mutable_est();

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    jtf->set_final(t.supportsFinal());
    gen.flatTableColumnPath(0, t, [](const SQLColumn & c) { return c.canBeInserted(); });
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
    ts->set_format(OutFormat::OUT_CSV);
    sif->set_path(qfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

static const std::unordered_map<OutFormat, InFormat> out_in{
    {OutFormat::OUT_CSV, InFormat::IN_CSV},
    {OutFormat::OUT_CSVWithNames, InFormat::IN_CSVWithNames},
    {OutFormat::OUT_CSVWithNamesAndTypes, InFormat::IN_CSVWithNamesAndTypes},
    {OutFormat::OUT_Values, InFormat::IN_Values},
    {OutFormat::OUT_JSON, InFormat::IN_JSON},
    {OutFormat::OUT_JSONColumns, InFormat::IN_JSONColumns},
    {OutFormat::OUT_JSONColumnsWithMetadata, InFormat::IN_JSONColumnsWithMetadata},
    {OutFormat::OUT_JSONCompact, InFormat::IN_JSONCompact},
    {OutFormat::OUT_JSONCompactColumns, InFormat::IN_JSONCompactColumns},
    {OutFormat::OUT_JSONEachRow, InFormat::IN_JSONEachRow},
    {OutFormat::OUT_JSONStringsEachRow, InFormat::IN_JSONStringsEachRow},
    {OutFormat::OUT_JSONCompactEachRow, InFormat::IN_JSONCompactEachRow},
    {OutFormat::OUT_JSONCompactEachRowWithNames, InFormat::IN_JSONCompactEachRowWithNames},
    {OutFormat::OUT_JSONCompactEachRowWithNamesAndTypes, InFormat::IN_JSONCompactEachRowWithNamesAndTypes},
    {OutFormat::OUT_JSONCompactStringsEachRow, InFormat::IN_JSONCompactStringsEachRow},
    {OutFormat::OUT_JSONCompactStringsEachRowWithNames, InFormat::IN_JSONCompactStringsEachRowWithNames},
    {OutFormat::OUT_JSONCompactStringsEachRowWithNamesAndTypes, InFormat::IN_JSONCompactStringsEachRowWithNamesAndTypes},
    {OutFormat::OUT_JSONObjectEachRow, InFormat::IN_JSONObjectEachRow},
    {OutFormat::OUT_BSONEachRow, InFormat::IN_BSONEachRow},
    {OutFormat::OUT_TSKV, InFormat::IN_TSKV},
    {OutFormat::OUT_Protobuf, InFormat::IN_Protobuf},
    {OutFormat::OUT_ProtobufSingle, InFormat::IN_ProtobufSingle},
    {OutFormat::OUT_Avro, InFormat::IN_Avro},
    {OutFormat::OUT_Parquet, InFormat::IN_Parquet},
    {OutFormat::OUT_Arrow, InFormat::IN_Arrow},
    {OutFormat::OUT_ArrowStream, InFormat::IN_ArrowStream},
    {OutFormat::OUT_ORC, InFormat::IN_ORC},
    {OutFormat::OUT_RowBinary, InFormat::IN_RowBinary},
    {OutFormat::OUT_RowBinaryWithNames, InFormat::IN_RowBinaryWithNames},
    {OutFormat::OUT_RowBinaryWithNamesAndTypes, InFormat::IN_RowBinaryWithNamesAndTypes},
    {OutFormat::OUT_Native, InFormat::IN_Native},
    {OutFormat::OUT_MsgPack, InFormat::IN_MsgPack}};

void QueryOracle::generateExportQuery(RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, SQLQuery & sq2)
{
    String buf;
    bool first = true;
    std::error_code ec;
    Insert * ins = sq2.mutable_explain()->mutable_inner_query()->mutable_insert();
    FileFunc * ff = ins->mutable_tfunction()->mutable_file();
    SelectStatementCore * sel = ins->mutable_insert_select()->mutable_select()->mutable_select_core();
    const std::filesystem::path & nfile = fc.db_file_path / "table.data";
    OutFormat outf = rg.pickKeyRandomlyFromMap(out_in);

    /// Remove the file if exists
    if (!std::filesystem::remove(nfile, ec) && ec)
    {
        LOG_ERROR(fc.log, "Could not remove file: {}", ec.message());
    }
    ff->set_path(nfile.generic_string());

    gen.flatTableColumnPath(0, t, [](const SQLColumn & c) { return c.canBeInserted(); });
    for (const auto & entry : gen.entries)
    {
        SQLType * tp = entry.getBottomType();

        buf += fmt::format(
            "{}{} {}{}",
            first ? "" : ", ",
            entry.getBottomName(),
            tp->typeName(true),
            entry.nullable.has_value() ? (entry.nullable.value() ? " NULL" : " NOT NULL") : "");
        gen.columnPathRef(entry, sel->add_result_columns()->mutable_etc()->mutable_col()->mutable_path());
        /// ArrowStream doesn't support UUID
        if (outf == OutFormat::OUT_ArrowStream && tp->getTypeClass() == SQLTypeClass::UUID)
        {
            outf = OutFormat::OUT_CSV;
        }
        first = false;
    }
    gen.entries.clear();
    ff->set_outformat(outf);
    ff->set_structure(buf);
    if (rg.nextSmallNumber() < 4)
    {
        ff->set_fcomp(static_cast<FileCompression>((rg.nextRandomUInt32() % static_cast<uint32_t>(FileCompression_MAX)) + 1));
    }

    /// Set the table on select
    JoinedTableOrFunction * jtf = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
    ExprSchemaTable * est = jtf->mutable_tof()->mutable_est();

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    jtf->set_final(t.supportsFinal());
}

void QueryOracle::generateClearQuery(const SQLTable & t, SQLQuery & sq3)
{
    Truncate * trunc = sq3.mutable_explain()->mutable_inner_query()->mutable_trunc();
    ExprSchemaTable * est = trunc->mutable_est();

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
}

void QueryOracle::generateImportQuery(StatementGenerator & gen, const SQLTable & t, const SQLQuery & sq2, SQLQuery & sq4)
{
    Insert * ins = sq4.mutable_explain()->mutable_inner_query()->mutable_insert();
    InsertFromFile * iff = ins->mutable_insert_file();
    const FileFunc & ff = sq2.explain().inner_query().insert().tfunction().file();
    ExprSchemaTable * est = ins->mutable_est();

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    gen.flatTableColumnPath(0, t, [](const SQLColumn & c) { return c.canBeInserted(); });
    for (const auto & entry : gen.entries)
    {
        gen.columnPathRef(entry, ins->add_cols());
    }
    gen.entries.clear();
    iff->set_path(ff.path());
    iff->set_format(out_in.at(ff.outformat()));
    if (ff.has_fcomp())
    {
        iff->set_fcomp(ff.fcomp());
    }
    if (iff->format() == IN_CSV)
    {
        SettingValues * vals = iff->mutable_settings();
        SetValue * sv = vals->mutable_set_value();

        sv->set_property("input_format_csv_detect_header");
        sv->set_value("0");
    }
}

static std::unordered_map<String, CHSetting> queryOracleSettings;

void loadFuzzerOracleSettings(const FuzzConfig &)
{
    for (auto & [key, value] : serverSettings)
    {
        if (!value.oracle_values.empty())
        {
            queryOracleSettings.insert({{key, value}});
        }
    }
}

/// Run query with different settings oracle
void QueryOracle::generateFirstSetting(RandomGenerator & rg, SQLQuery & sq1)
{
    const uint32_t nsets = rg.nextBool() ? 1 : ((rg.nextSmallNumber() % 3) + 1);
    SettingValues * sv = sq1.mutable_explain()->mutable_inner_query()->mutable_setting_values();

    nsettings.clear();
    for (uint32_t i = 0; i < nsets; i++)
    {
        const String & setting = rg.pickKeyRandomlyFromMap(queryOracleSettings);
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
            setv->set_value(rg.pickRandomlyFromSet(chs.oracle_values));
            nsettings.push_back(rg.pickRandomlyFromSet(chs.oracle_values));
        }
        can_test_query_success &= !chs.changes_behavior;
    }
}

void QueryOracle::generateSecondSetting(const SQLQuery & sq1, SQLQuery & sq3)
{
    const SettingValues & osv = sq1.explain().inner_query().setting_values();
    SettingValues * sv = sq3.mutable_explain()->mutable_inner_query()->mutable_setting_values();

    for (size_t i = 0; i < nsettings.size(); i++)
    {
        const SetValue & osetv = i == 0 ? osv.set_value() : osv.other_values(static_cast<int>(i - 1));
        SetValue * setv = i == 0 ? sv->mutable_set_value() : sv->add_other_values();

        setv->set_property(osetv.property());
        setv->set_value(nsettings[i]);
    }
}

void QueryOracle::generateOracleSelectQuery(RandomGenerator & rg, const PeerQuery pq, StatementGenerator & gen, SQLQuery & sq2)
{
    std::error_code ec;
    bool explain = false;
    Select * sel = nullptr;
    InsertSelect * insel = nullptr;
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
        sel = sq2.mutable_explain()->mutable_inner_query()->mutable_select()->mutable_sel();
    }
    else
    {
        Insert * ins = sq2.mutable_explain()->mutable_inner_query()->mutable_insert();
        FileFunc * ff = ins->mutable_tfunction()->mutable_file();

        if (!std::filesystem::remove(qfile, ec) && ec)
        {
            LOG_ERROR(fc.log, "Could not remove file: {}", ec.message());
        }
        ff->set_path(qfile.generic_string());
        ff->set_outformat(OutFormat::OUT_CSV);
        insel = ins->mutable_insert_select();
        sel = insel->mutable_select();
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
    gen.generateSelect(rg, true, global_aggregate, ncols, std::numeric_limits<uint32_t>::max(), sel);
    gen.setAllowNotDetermistic(true);
    gen.enforceFinal(false);
    gen.generatingPeerQuery(PeerQuery::None);
    gen.setAllowEngineUDF(true);

    if (!measure_performance && !explain && !global_aggregate)
    {
        /// If not global aggregate, use ORDER BY clause
        Select * osel = insel->release_select();
        SelectStatementCore * nsel = insel->mutable_select()->mutable_select_core();
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
    }
    else if (measure_performance)
    {
        /// Add tag to find query later on
        if (!sel->has_setting_values())
        {
            auto * news = sel->mutable_setting_values();
            UNUSED(news);
        }
        SettingValues & svs = const_cast<SettingValues &>(sel->setting_values());
        SetValue * sv = svs.has_set_value() ? svs.add_other_values() : svs.mutable_set_value();

        sv->set_property("log_comment");
        sv->set_value("'measure_performance'");
    }
}

void QueryOracle::findTablesWithPeersAndReplace(
    RandomGenerator & rg, google::protobuf::Message & mes, StatementGenerator & gen, const bool replace)
{
    checkStackSize();

    if (mes.GetTypeName() == "BuzzHouse.Select")
    {
        auto & sel = static_cast<Select &>(mes);

        if (sel.has_select_core())
        {
            findTablesWithPeersAndReplace(rg, const_cast<SelectStatementCore &>(sel.select_core()), gen, replace);
        }
        else if (sel.has_set_query())
        {
            findTablesWithPeersAndReplace(rg, const_cast<SetQuery &>(sel.set_query()), gen, replace);
        }
        if (sel.has_ctes())
        {
            findTablesWithPeersAndReplace(rg, const_cast<Select &>(sel.ctes().cte().query()), gen, replace);
            for (int i = 0; i < sel.ctes().other_ctes_size(); i++)
            {
                findTablesWithPeersAndReplace(rg, const_cast<Select &>(sel.ctes().other_ctes(i).query()), gen, replace);
            }
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.SetQuery")
    {
        auto & setq = static_cast<SetQuery &>(mes);

        findTablesWithPeersAndReplace(rg, const_cast<Select &>(setq.sel1().inner_query().select().sel()), gen, replace);
        findTablesWithPeersAndReplace(rg, const_cast<Select &>(setq.sel2().inner_query().select().sel()), gen, replace);
    }
    else if (mes.GetTypeName() == "BuzzHouse.SelectStatementCore")
    {
        auto & ssc = static_cast<SelectStatementCore &>(mes);

        if (ssc.has_from())
        {
            findTablesWithPeersAndReplace(rg, const_cast<JoinedQuery &>(ssc.from().tos()), gen, replace);
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.JoinedQuery")
    {
        auto & jquery = static_cast<JoinedQuery &>(mes);

        for (int i = 0; i < jquery.tos_list_size(); i++)
        {
            findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jquery.tos_list(i)), gen, replace);
        }
        findTablesWithPeersAndReplace(rg, const_cast<JoinClause &>(jquery.join_clause()), gen, replace);
    }
    else if (mes.GetTypeName() == "BuzzHouse.JoinClause")
    {
        auto & jclause = static_cast<JoinClause &>(mes);

        for (int i = 0; i < jclause.clauses_size(); i++)
        {
            if (jclause.clauses(i).has_core())
            {
                findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jclause.clauses(i).core().tos()), gen, replace);
            }
        }
        findTablesWithPeersAndReplace(rg, const_cast<TableOrSubquery &>(jclause.tos()), gen, replace);
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableOrSubquery")
    {
        auto & tos = static_cast<TableOrSubquery &>(mes);

        if (tos.has_joined_table())
        {
            findTablesWithPeersAndReplace(rg, const_cast<TableOrFunction &>(tos.joined_table().tof()), gen, replace);
        }
        else if (tos.has_joined_query())
        {
            findTablesWithPeersAndReplace(rg, const_cast<JoinedQuery &>(tos.joined_query()), gen, replace);
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableFunction")
    {
        auto & tfunc = static_cast<TableFunction &>(mes);

        if (tfunc.has_remote() || tfunc.has_cluster())
        {
            findTablesWithPeersAndReplace(
                rg, const_cast<TableOrFunction &>(tfunc.has_remote() ? tfunc.remote().tof() : tfunc.cluster().tof()), gen, replace);
        }
    }
    else if (mes.GetTypeName() == "BuzzHouse.TableOrFunction")
    {
        auto & torfunc = static_cast<TableOrFunction &>(mes);

        if (torfunc.has_est())
        {
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
                            gen.setTableRemote(rg, false, t, torfunc.mutable_tfunc());
                        }
                        found_tables.insert(tname);
                        can_test_query_success &= t.hasClickHousePeer();
                    }
                }
            }
        }
        else if (torfunc.has_tfunc())
        {
            findTablesWithPeersAndReplace(rg, const_cast<TableFunction &>(torfunc.tfunc()), gen, replace);
        }
        else if (torfunc.has_select())
        {
            findTablesWithPeersAndReplace(rg, const_cast<Select &>(torfunc.select().inner_query().select().sel()), gen, replace);
        }
    }
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
    Select & nsel = const_cast<Select &>(
        measure_performance ? sq2.explain().inner_query().select().sel() : sq2.explain().inner_query().insert().insert_select().select());
    /// Replace references
    findTablesWithPeersAndReplace(rg, nsel, gen, peer_query != PeerQuery::ClickHouseOnly);
    if (peer_query == PeerQuery::ClickHouseOnly && !measure_performance)
    {
        /// Use a different file for the peer database
        std::error_code ec;
        FileFunc & ff = const_cast<FileFunc &>(sq2.explain().inner_query().insert().tfunction().file());

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
        Insert * ins = next.mutable_explain()->mutable_inner_query()->mutable_insert();
        SelectStatementCore * sel = ins->mutable_insert_select()->mutable_select()->mutable_select_core();

        // Then insert the data
        gen.setTableRemote(rg, false, t, ins->mutable_tfunction());
        JoinedTableOrFunction * jtf = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
        ExprSchemaTable * est = jtf->mutable_tof()->mutable_est();
        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
        jtf->set_final(t.supportsFinal());
        gen.flatTableColumnPath(0, t, [](const SQLColumn & c) { return c.canBeInserted(); });
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
    query_duration_ms1 = memory_usage1 = query_duration_ms2 = memory_usage2 = 0;
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
            other_steps_sucess
                &= ei.getPerformanceMetricsForLastQuery(PeerTableDatabase::None, this->query_duration_ms1, this->memory_usage1);
        }
        else
        {
            md5_hash1.hashFile(qfile.generic_string(), first_digest);
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
                if (ei.getPerformanceMetricsForLastQuery(PeerTableDatabase::ClickHouse, this->query_duration_ms2, this->memory_usage2))
                {
                    if (this->fc.query_time_minimum<this->query_duration_ms1 && this->query_duration_ms1> static_cast<uint64_t>(
                            this->query_duration_ms2 * (1 + (static_cast<double>(fc.query_time_threshold) / 100.0f))))
                    {
                        throw DB::Exception(
                            DB::ErrorCodes::BUZZHOUSE,
                            "{}: ClickHouse peer server query was faster than the target server: {} vs {}",
                            oracle_name,
                            formatReadableTime(static_cast<double>(this->query_duration_ms1 * 1000000)),
                            formatReadableTime(static_cast<double>(this->query_duration_ms2 * 1000000)));
                    }
                    if (this->fc.query_memory_minimum<this->memory_usage1 && this->memory_usage1> static_cast<uint64_t>(
                            this->memory_usage2 * (1 + (static_cast<double>(fc.query_memory_threshold) / 100.0f))))
                    {
                        throw DB::Exception(
                            DB::ErrorCodes::BUZZHOUSE,
                            "{}: ClickHouse peer server query used less memory than the target server: {} vs {}",
                            oracle_name,
                            formatReadableSizeWithBinarySuffix(static_cast<double>(this->memory_usage1)),
                            formatReadableSizeWithBinarySuffix(static_cast<double>(this->memory_usage2)));
                    }
                }
            }
            else
            {
                md5_hash2.hashFile((peer_query == PeerQuery::ClickHouseOnly ? qfile_peer : qfile).generic_string(), second_digest);
                if (first_digest != second_digest)
                {
                    throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "{}: failed with different result sets", oracle_name);
                }
            }
        }
    }
}

}
