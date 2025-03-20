#include <Client/BuzzHouse/Generator/RandomSettings.h>
#include <Client/BuzzHouse/Generator/SQLCatalog.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>

namespace BuzzHouse
{

int StatementGenerator::generateStorage(RandomGenerator & rg, Storage * store) const
{
    store->set_storage(static_cast<Storage_DataStorage>((rg.nextRandomUInt32() % static_cast<uint32_t>(Storage::DataStorage_MAX)) + 1));
    store->set_storage_name(rg.pickRandomlyFromVector(fc.disks));
    return 0;
}

int StatementGenerator::generateSettingValues(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    const size_t nvalues,
    SettingValues * vals)
{
    for (size_t i = 0; i < nvalues; i++)
    {
        SetValue * sv = i == 0 ? vals->mutable_set_value() : vals->add_other_values();

        setRandomSetting(rg, settings, this->buf, sv);
    }
    return 0;
}

int StatementGenerator::generateSettingValues(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    SettingValues * vals)
{
    return generateSettingValues(
        rg, settings, std::min<size_t>(settings.size(), static_cast<size_t>((rg.nextRandomUInt32() % 10) + 1)), vals);
}

int StatementGenerator::generateSettingList(
    RandomGenerator & rg, const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings, SettingList * sl)
{
    const size_t nvalues = std::min<size_t>(settings.size(), static_cast<size_t>((rg.nextRandomUInt32() % 4) + 1));

    for (size_t i = 0; i < nvalues; i++)
    {
        const std::string & next = rg.pickKeyRandomlyFromMap(settings);

        if (i == 0)
        {
            sl->set_setting(next);
        }
        else
        {
            sl->add_other_settings(next);
        }
    }
    return 0;
}

DatabaseEngineValues StatementGenerator::getNextDatabaseEngine(RandomGenerator & rg)
{
    assert(this->ids.empty());
    this->ids.push_back(DAtomic);
    this->ids.push_back(DMemory);
    if (replica_setup)
    {
        this->ids.push_back(DReplicated);
    }
    if (supports_cloud_features)
    {
        this->ids.push_back(DShared);
    }
    const auto res = static_cast<DatabaseEngineValues>(rg.pickRandomlyFromVector(this->ids));
    this->ids.clear();
    return res;
}

int StatementGenerator::generateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd)
{
    SQLDatabase next;
    const uint32_t dname = this->database_counter++;
    DatabaseEngine * deng = cd->mutable_dengine();

    next.deng = this->getNextDatabaseEngine(rg);
    deng->set_engine(next.deng);
    if (next.deng == DatabaseEngineValues::DReplicated)
    {
        deng->set_zoo_path(this->zoo_path_counter++);
    }
    next.dname = dname;
    cd->mutable_database()->set_database("d" + std::to_string(dname));
    if (rg.nextSmallNumber() < 3)
    {
        buf.resize(0);
        rg.nextString(buf, "'", true, rg.nextRandomUInt32() % 1009);
        cd->set_comment(buf);
    }
    this->staged_databases[dname] = std::make_shared<SQLDatabase>(std::move(next));
    return 0;
}

int StatementGenerator::generateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf)
{
    SQLFunction next;
    const uint32_t fname = this->function_counter++;

    next.fname = fname;
    next.nargs = std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % (rg.nextBool() ? 4 : 10)));
    if ((next.is_deterministic = rg.nextBool()))
    {
        //if this function is later called by an oracle, then don't call it
        this->setAllowNotDetermistic(false);
        this->enforceFinal(true);
    }
    generateLambdaCall(rg, next.nargs, cf->mutable_lexpr());
    this->levels.clear();
    if (next.is_deterministic)
    {
        this->setAllowNotDetermistic(true);
        this->enforceFinal(false);
    }
    cf->mutable_function()->set_function("f" + std::to_string(fname));
    this->staged_functions[fname] = std::move(next);
    return 0;
}

static void SetViewInterval(RandomGenerator & rg, RefreshInterval * ri)
{
    ri->set_interval(rg.nextSmallNumber() - 1);
    ri->set_unit(RefreshInterval_RefreshUnit::RefreshInterval_RefreshUnit_SECOND);
}

int StatementGenerator::generateNextRefreshableView(RandomGenerator & rg, RefreshableView * cv)
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
    return 0;
}

int StatementGenerator::generateNextCreateView(RandomGenerator & rg, CreateView * cv)
{
    SQLView next;
    uint32_t tname = 0;
    ExprSchemaTable * est = cv->mutable_est();
    const bool replace = collectionCount<SQLView>(attached_views) > 3 && rg.nextMediumNumber() < 16;

    if (replace)
    {
        const SQLView & v = rg.pickRandomlyFromVector(filterCollection<SQLView>(attached_views));

        next.db = v.db;
        tname = next.tname = v.tname;
    }
    else
    {
        if (collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && rg.nextSmallNumber() < 9)
        {
            next.db = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    cv->set_replace(replace);
    next.is_materialized = rg.nextBool();
    cv->set_materialized(next.is_materialized);
    next.ncols = (rg.nextMediumNumber() % 5) + 1;
    if (next.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(next.db->dname));
    }
    est->mutable_table()->set_table("v" + std::to_string(next.tname));
    if (next.is_materialized)
    {
        TableEngine * te = cv->mutable_engine();
        const bool has_with_cols
            = collectionHas<SQLTable>([&next](const SQLTable & t) { return t.numberOfInsertableColumns() == next.ncols; });
        const bool has_tables = has_with_cols || !tables.empty();

        next.teng = getNextTableEngine(rg, false);
        te->set_engine(next.teng);

        assert(this->entries.empty());
        for (uint32_t i = 0; i < next.ncols; i++)
        {
            std::vector<ColumnPathChainEntry> path = {ColumnPathChainEntry("c" + std::to_string(i), nullptr)};
            entries.push_back(ColumnPathChain(std::nullopt, ColumnSpecial::NONE, std::nullopt, std::move(path)));
        }
        generateEngineDetails(rg, next, true, te);
        if (next.isMergeTreeFamily() && rg.nextMediumNumber() < 16)
        {
            generateNextTTL(rg, std::nullopt, te, te->mutable_ttl_expr());
        }
        this->entries.clear();

        if ((has_with_cols || has_tables) && rg.nextSmallNumber() < (has_with_cols ? 9 : 6))
        {
            const SQLTable & t = has_with_cols
                ? rg.pickRandomlyFromVector(
                        filterCollection<SQLTable>([&next](const SQLTable & tt) { return tt.numberOfInsertableColumns() == next.ncols; }))
                      .get()
                : rg.pickValueRandomlyFromMap(this->tables);

            cv->mutable_to_est()->mutable_table()->set_table("t" + std::to_string(t.tname));
        }
        if ((next.is_refreshable = rg.nextBool()))
        {
            generateNextRefreshableView(rg, cv->mutable_refresh());
            cv->set_empty(rg.nextBool());
        }
        else
        {
            cv->set_populate(rg.nextSmallNumber() < 4);
        }
    }
    if ((next.is_deterministic = rg.nextBool()))
    {
        this->setAllowNotDetermistic(false);
        this->enforceFinal(true);
    }
    this->levels[this->current_level] = QueryLevel(this->current_level);
    this->allow_in_expression_alias = rg.nextSmallNumber() < 3;
    generateSelect(
        rg,
        false,
        false,
        next.ncols,
        next.is_materialized ? (~allow_prewhere) : std::numeric_limits<uint32_t>::max(),
        cv->mutable_select());
    this->allow_in_expression_alias = true;
    if (next.is_deterministic)
    {
        this->setAllowNotDetermistic(true);
        this->enforceFinal(false);
    }
    if (rg.nextSmallNumber() < 3)
    {
        buf.resize(0);
        rg.nextString(buf, "'", true, rg.nextRandomUInt32() % 1009);
        cv->set_comment(buf);
    }
    this->staged_views[tname] = next;
    return 0;
}

int StatementGenerator::generateNextDrop(RandomGenerator & rg, Drop * dp)
{
    SQLObjectName * sot = dp->mutable_object();
    const uint32_t drop_table = 10 * static_cast<uint32_t>(collectionCount<SQLTable>(attached_tables) > 3);
    const uint32_t drop_view = 10 * static_cast<uint32_t>(collectionCount<SQLView>(attached_views) > 3);
    const uint32_t drop_database = 2 * static_cast<uint32_t>(collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3);
    const uint32_t drop_function = 1 * static_cast<uint32_t>(functions.size() > 3);
    const uint32_t prob_space = drop_table + drop_view + drop_database + drop_function;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (drop_table && nopt < (drop_table + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

        dp->set_is_temp(t.is_temp);
        dp->set_sobject(SQLObject::TABLE);
        dp->set_if_empty(rg.nextSmallNumber() < 4);
        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
    }
    else if (drop_view && nopt < (drop_table + drop_view + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLView & v = rg.pickRandomlyFromVector(filterCollection<SQLView>(attached_views));

        dp->set_sobject(SQLObject::VIEW);
        if (v.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(v.db->dname));
        }
        est->mutable_table()->set_table("v" + std::to_string(v.tname));
    }
    else if (drop_database && nopt < (drop_table + drop_view + drop_database + 1))
    {
        const std::shared_ptr<SQLDatabase> & d
            = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        dp->set_sobject(SQLObject::DATABASE);
        sot->mutable_database()->set_database("d" + std::to_string(d->dname));
    }
    else if (drop_function)
    {
        const uint32_t & fname = rg.pickKeyRandomlyFromMap(this->functions);

        dp->set_sobject(SQLObject::FUNCTION);
        sot->mutable_function()->set_function("f" + std::to_string(fname));
    }
    else
    {
        assert(0);
    }
    if (dp->sobject() != SQLObject::FUNCTION)
    {
        dp->set_sync(rg.nextSmallNumber() < 3);
        if (rg.nextSmallNumber() < 3)
        {
            generateSettingValues(rg, serverSettings, dp->mutable_setting_values());
        }
    }
    return 0;
}

int StatementGenerator::generateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * ot)
{
    ExprSchemaTable * est = ot->mutable_est();
    const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(
        [](const SQLTable & st)
        {
            return (!st.db || st.db->attached == DetachStatus::ATTACHED) && st.attached == DetachStatus::ATTACHED && st.isMergeTreeFamily();
        }));

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    if (t.isMergeTreeFamily())
    {
        if (rg.nextBool())
        {
            generateNextTablePartition<false>(rg, t, ot->mutable_partition());
        }
        ot->set_cleanup(rg.nextSmallNumber() < 3);
    }
    if (rg.nextSmallNumber() < 4)
    {
        const uint32_t noption = rg.nextMediumNumber();
        DeduplicateExpr * dde = ot->mutable_dedup();

        if (noption < 51)
        {
            ExprColumnList * ecl = noption < 26 ? dde->mutable_col_list() : dde->mutable_ded_star_except();
            flatTableColumnPath(flat_tuple | flat_nested | skip_nested_node, t, [](const SQLColumn &) { return true; });
            const uint32_t ocols
                = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(4))) + 1;
            std::shuffle(entries.begin(), entries.end(), rg.generator);
            for (uint32_t i = 0; i < ocols; i++)
            {
                ExprColumn * col = i == 0 ? ecl->mutable_col() : ecl->add_extra_cols();

                columnPathRef(this->entries[i], col->mutable_path());
            }
            entries.clear();
        }
        else if (noption < 76)
        {
            dde->set_ded_star(true);
        }
    }
    ot->set_final((t.supportsFinal() || t.isMergeTreeFamily()) && rg.nextSmallNumber() < 3);
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, ot->mutable_setting_values());
    }
    return 0;
}

int StatementGenerator::generateNextCheckTable(RandomGenerator & rg, CheckTable * ct)
{
    ExprSchemaTable * est = ct->mutable_est();
    const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    if (t.isMergeTreeFamily() && rg.nextBool())
    {
        generateNextTablePartition<true>(rg, t, ct->mutable_partition());
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
    return 0;
}

int StatementGenerator::generateNextDescTable(RandomGenerator & rg, DescTable * dt)
{
    ExprSchemaTable * est = dt->mutable_est();
    const bool has_tables = collectionHas<SQLTable>(attached_tables);
    const bool has_views = collectionHas<SQLView>(attached_views);

    if (has_views && (!has_tables || rg.nextBool()))
    {
        const SQLView & v = rg.pickRandomlyFromVector(filterCollection<SQLView>(attached_views));

        if (v.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(v.db->dname));
        }
        est->mutable_table()->set_table("v" + std::to_string(v.tname));
    }
    else if (has_tables)
    {
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
    }
    else
    {
        assert(0);
    }
    dt->set_sub_cols(rg.nextSmallNumber() < 4);
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
    return 0;
}

int StatementGenerator::generateNextInsert(RandomGenerator & rg, Insert * ins)
{
    const uint32_t noption = rg.nextMediumNumber();
    ExprSchemaTable * est = ins->mutable_est();
    const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));

    flatTableColumnPath(skip_nested_node | flat_nested, t, [](const SQLColumn & c) { return c.canBeInserted(); });
    std::shuffle(this->entries.begin(), this->entries.end(), rg.generator);
    for (const auto & entry : this->entries)
    {
        columnPathRef(entry, ins->add_cols());
    }

    if (noption < 901)
    {
        std::uniform_int_distribution<uint64_t> rows_dist(1, fc.max_insert_rows);
        std::uniform_int_distribution<uint64_t> nested_rows_dist(0, fc.max_nested_rows);
        const uint64_t nrows = rows_dist(rg.generator);
        InsertStringQuery * iquery = ins->mutable_query();

        buf.resize(0);
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
                    || rg.nextLargeNumber() < 2)
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
                    //make sure all nested entries have the same number of rows
                    strAppendArray(rg, buf, entry.getBottomType(), next_nested_rows);
                }
                else
                {
                    strAppendAnyValue(rg, buf, entry.getBottomType());
                }
                j++;
            }
            buf += ")";
        }
        iquery->set_query(buf);
        if (rg.nextSmallNumber() < 3)
        {
            generateSettingValues(rg, serverSettings, iquery->mutable_setting_values());
        }
    }
    else if (noption < 951)
    {
        InsertSelect * isel = ins->mutable_insert_select();

        this->levels[this->current_level] = QueryLevel(this->current_level);
        if (rg.nextMediumNumber() < 13)
        {
            this->addCTEs(rg, std::numeric_limits<uint32_t>::max(), ins->mutable_ctes());
        }
        generateSelect(
            rg, true, false, static_cast<uint32_t>(this->entries.size()), std::numeric_limits<uint32_t>::max(), isel->mutable_select());
        if (rg.nextSmallNumber() < 3)
        {
            generateSettingValues(rg, serverSettings, isel->mutable_setting_values());
        }
    }
    else
    {
        const uint32_t nrows = (rg.nextSmallNumber() % 3) + 1;
        ValuesStatement * vs = ins->mutable_values();

        this->levels[this->current_level] = QueryLevel(this->current_level);
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
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
        if (rg.nextSmallNumber() < 3)
        {
            generateSettingValues(rg, serverSettings, vs->mutable_setting_values());
        }
    }
    this->entries.clear();
    return 0;
}

int StatementGenerator::generateUptDelWhere(RandomGenerator & rg, const SQLTable & t, Expr * expr)
{
    if (rg.nextSmallNumber() < 10)
    {
        addTableRelation(rg, true, "", t);
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
        generateWherePredicate(rg, expr);
        this->levels.clear();
    }
    else
    {
        expr->mutable_lit_val()->set_special_val(SpecialVal::VAL_TRUE);
    }
    return 0;
}

int StatementGenerator::generateNextDelete(RandomGenerator & rg, LightDelete * del)
{
    ExprSchemaTable * est = del->mutable_est();
    const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    if (t.isMergeTreeFamily() && rg.nextBool())
    {
        generateNextTablePartition<false>(rg, t, del->mutable_partition());
    }
    generateUptDelWhere(rg, t, del->mutable_where()->mutable_expr()->mutable_expr());
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, del->mutable_setting_values());
    }
    return 0;
}

int StatementGenerator::generateNextTruncate(RandomGenerator & rg, Truncate * trunc)
{
    const bool trunc_database = collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases);
    const uint32_t trunc_table = 980 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t trunc_db_tables = 15 * static_cast<uint32_t>(trunc_database);
    const uint32_t trunc_db = 5 * static_cast<uint32_t>(trunc_database);
    const uint32_t prob_space = trunc_table + trunc_db_tables + trunc_db;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (trunc_table && nopt < (trunc_table + 1))
    {
        ExprSchemaTable * est = trunc->mutable_est();
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
    }
    else if (trunc_db_tables && nopt < (trunc_table + trunc_db_tables + 1))
    {
        const std::shared_ptr<SQLDatabase> & d
            = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        trunc->mutable_all_tables()->set_database("d" + std::to_string(d->dname));
    }
    else if (trunc_db && nopt < (trunc_table + trunc_db_tables + trunc_db + 1))
    {
        const std::shared_ptr<SQLDatabase> & d
            = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        trunc->mutable_database()->set_database("d" + std::to_string(d->dname));
    }
    else
    {
        assert(0);
    }
    trunc->set_sync(rg.nextSmallNumber() < 4);
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, trunc->mutable_setting_values());
    }
    return 0;
}

int StatementGenerator::generateNextExchangeTables(RandomGenerator & rg, ExchangeTables * et)
{
    ExprSchemaTable * est1 = et->mutable_est1();
    ExprSchemaTable * est2 = et->mutable_est2();
    const auto & input = filterCollection<SQLTable>(
        [](const SQLTable & t)
        { return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && !t.hasDatabasePeer(); });

    for (const auto & entry : input)
    {
        this->ids.push_back(entry.get().tname);
    }
    std::shuffle(this->ids.begin(), this->ids.end(), rg.generator);
    const SQLTable & t1 = this->tables[this->ids[0]];
    const SQLTable & t2 = this->tables[this->ids[1]];

    if (t1.db)
    {
        est1->mutable_database()->set_database("d" + std::to_string(t1.db->dname));
    }
    est1->mutable_table()->set_table("t" + std::to_string(t1.tname));
    if (t2.db)
    {
        est2->mutable_database()->set_database("d" + std::to_string(t2.db->dname));
    }
    est2->mutable_table()->set_table("t" + std::to_string(t2.tname));
    this->ids.clear();
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, et->mutable_setting_values());
    }
    return 0;
}

int StatementGenerator::generateAlterTable(RandomGenerator & rg, AlterTable * at)
{
    ExprSchemaTable * est = at->mutable_est();
    const uint32_t nalters = rg.nextBool() ? 1 : ((rg.nextMediumNumber() % 4) + 1);
    const bool has_tables = collectionHas<SQLTable>(
        [](const SQLTable & tt)
        { return (!tt.db || tt.db->attached == DetachStatus::ATTACHED) && tt.attached == DetachStatus::ATTACHED && !tt.isFileEngine(); });
    const bool has_views = collectionHas<SQLView>(attached_views);

    if (has_views && (!has_tables || rg.nextBool()))
    {
        SQLView & v = const_cast<SQLView &>(rg.pickRandomlyFromVector(filterCollection<SQLView>(attached_views)).get());

        if (v.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(v.db->dname));
        }
        est->mutable_table()->set_table("v" + std::to_string(v.tname));
        for (uint32_t i = 0; i < nalters; i++)
        {
            const uint32_t alter_refresh = 1 * static_cast<uint32_t>(v.is_refreshable);
            const uint32_t alter_query = 3;
            const uint32_t prob_space = alter_refresh + alter_query;
            AlterTableItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            if (alter_refresh && nopt < (alter_refresh + 1))
            {
                generateNextRefreshableView(rg, ati->mutable_refresh());
            }
            else
            {
                v.staged_ncols = (rg.nextMediumNumber() % 5) + 1;
                if (v.is_deterministic)
                {
                    this->setAllowNotDetermistic(false);
                    this->enforceFinal(true);
                }
                this->levels[this->current_level] = QueryLevel(this->current_level);
                this->allow_in_expression_alias = rg.nextSmallNumber() < 3;
                generateSelect(
                    rg,
                    false,
                    false,
                    v.staged_ncols,
                    v.is_materialized ? (~allow_prewhere) : std::numeric_limits<uint32_t>::max(),
                    ati->mutable_modify_query());
                this->allow_in_expression_alias = true;
                if (v.is_deterministic)
                {
                    this->setAllowNotDetermistic(true);
                    this->enforceFinal(false);
                }
            }
        }
    }
    else if (has_tables)
    {
        SQLTable & t = const_cast<SQLTable &>(rg.pickRandomlyFromVector(filterCollection<SQLTable>(
                                                                            [](const SQLTable & tt)
                                                                            {
                                                                                return (!tt.db || tt.db->attached == DetachStatus::ATTACHED)
                                                                                    && tt.attached == DetachStatus::ATTACHED
                                                                                    && !tt.isFileEngine();
                                                                            }))
                                                  .get());
        const std::string dname = t.db ? ("d" + std::to_string(t.db->dname)) : "";
        const std::string tname = "t" + std::to_string(t.tname);
        const bool table_has_partitions = t.isMergeTreeFamily() && fc.tableHasPartitions<false>(dname, tname);

        at->set_is_temp(t.is_temp);
        if (t.db)
        {
            est->mutable_database()->set_database(dname);
        }
        est->mutable_table()->set_table(tname);
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
            const uint32_t table_modify_setting = 2 * static_cast<uint32_t>(!allTableSettings.at(t.teng).empty());
            const uint32_t table_remove_setting = 2 * static_cast<uint32_t>(!allTableSettings.at(t.teng).empty());
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
            const uint32_t modify_ttl = 5 * static_cast<uint32_t>(t.isMergeTreeFamily() && !t.hasDatabasePeer());
            const uint32_t remove_ttl = 2 * static_cast<uint32_t>(t.isMergeTreeFamily() && !t.hasDatabasePeer());
            const uint32_t comment_table = 2;
            const uint32_t prob_space = alter_order_by + heavy_delete + heavy_update + add_column + materialize_column + drop_column
                + rename_column + clear_column + modify_column + comment_column + delete_mask + add_stats + mod_stats + drop_stats
                + clear_stats + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property
                + column_modify_setting + column_remove_setting + table_modify_setting + table_remove_setting + add_projection
                + remove_projection + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition
                + drop_partition + drop_detached_partition + forget_partition + attach_partition + move_partition_to
                + clear_column_partition + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl
                + remove_ttl + comment_table;
            AlterTableItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            if (alter_order_by && nopt < (alter_order_by + 1))
            {
                TableKey * tkey = ati->mutable_order();

                if (rg.nextSmallNumber() < 6)
                {
                    flatTableColumnPath(flat_tuple | flat_nested | flat_json | skip_nested_node, t, [](const SQLColumn &) { return true; });
                    generateTableKey(rg, t.teng, true, tkey);
                    this->entries.clear();
                }
            }
            else if (heavy_delete && nopt < (heavy_delete + alter_order_by + 1))
            {
                HeavyDelete * hdel = ati->mutable_del();

                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition<false>(rg, t, hdel->mutable_partition());
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
                    flatTableColumnPath(flat_tuple | flat_nested, t, [](const SQLColumn &) { return true; });
                    columnPathRef(rg.pickRandomlyFromVector(this->entries), add_col->mutable_add_where()->mutable_col());
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

                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), mcol->mutable_col());
                this->entries.clear();
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition<false>(rg, t, mcol->mutable_partition());
                }
            }
            else if (drop_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + 1))
            {
                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), ati->mutable_drop_column());
                this->entries.clear();
            }
            else if (
                rename_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + 1))
            {
                const uint32_t ncname = t.col_counter++;
                RenameCol * rcol = ati->mutable_rename_column();

                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), rcol->mutable_old_name());
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

                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), ccol->mutable_col());
                this->entries.clear();
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition<false>(rg, t, ccol->mutable_partition());
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
                    rg,
                    t,
                    rg.pickKeyRandomlyFromMap(t.cols),
                    true,
                    true,
                    rg.nextMediumNumber() < 6,
                    ColumnSpecial::NONE,
                    add_col->mutable_new_col());
                if (next_option < 4)
                {
                    flatTableColumnPath(flat_tuple | flat_nested, t, [](const SQLColumn &) { return true; });
                    columnPathRef(rg.pickRandomlyFromVector(this->entries), add_col->mutable_add_where()->mutable_col());
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

                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), ccol->mutable_col());
                this->entries.clear();
                buf.resize(0);
                rg.nextString(buf, "'", true, rg.nextRandomUInt32() % 1009);
                ccol->set_comment(buf);
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
                    generateNextTablePartition<false>(rg, t, adm->mutable_partition());
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
                    generateNextTablePartition<false>(rg, t, upt->mutable_partition());
                }
                flatTableColumnPath(0, t, [](const SQLColumn & c) { return !dynamic_cast<NestedType *>(c.tp); });
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
                    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
                    for (uint32_t j = 0; j < nupdates; j++)
                    {
                        const ColumnPathChain & entry = this->entries[j];
                        UpdateSet & uset = const_cast<UpdateSet &>(j == 0 ? upt->update() : upt->other_updates(j - 1));
                        Expr * expr = uset.mutable_expr();

                        if (rg.nextSmallNumber() < 9)
                        {
                            //set constant value
                            LiteralValue * lv = expr->mutable_lit_val();

                            buf.resize(0);
                            if ((entry.dmod.has_value() && entry.dmod.value() == DModifier::DEF_DEFAULT && rg.nextMediumNumber() < 6)
                                || rg.nextLargeNumber() < 2)
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
                            else
                            {
                                strAppendAnyValue(rg, buf, entry.getBottomType());
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
                        add_index->mutable_add_where()->mutable_idx()->set_index("i" + std::to_string(rg.pickKeyRandomlyFromMap(t.idxs)));
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

                iip->mutable_idx()->set_index("i" + std::to_string(rg.pickKeyRandomlyFromMap(t.idxs)));
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition<false>(rg, t, iip->mutable_partition());
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

                iip->mutable_idx()->set_index("i" + std::to_string(rg.pickKeyRandomlyFromMap(t.idxs)));
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition<false>(rg, t, iip->mutable_partition());
                }
            }
            else if (
                drop_idx
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + 1))
            {
                ati->mutable_drop_index()->set_index("i" + std::to_string(rg.pickKeyRandomlyFromMap(t.idxs)));
            }
            else if (
                column_remove_property
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + 1))
            {
                RemoveColumnProperty * rcs = ati->mutable_column_remove_property();

                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), rcs->mutable_col());
                this->entries.clear();
                rcs->set_property(static_cast<RemoveColumnProperty_ColumnProperties>(
                    (rg.nextRandomUInt32() % static_cast<uint32_t>(RemoveColumnProperty::ColumnProperties_MAX)) + 1));
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

                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), mcp->mutable_col());
                this->entries.clear();
                generateSettingValues(rg, csettings, mcp->mutable_settings());
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

                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), rcp->mutable_col());
                this->entries.clear();
                generateSettingList(rg, csettings, rcp->mutable_settings());
            }
            else if (
                table_modify_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + 1))
            {
                const auto & tsettings = allTableSettings.at(t.teng);

                generateSettingValues(rg, tsettings, ati->mutable_table_modify_setting());
            }
            else if (
                table_remove_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + 1))
            {
                const auto & tsettings = allTableSettings.at(t.teng);

                generateSettingList(rg, tsettings, ati->mutable_table_remove_setting());
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
                ati->mutable_remove_projection()->set_projection("p" + std::to_string(rg.pickRandomlyFromSet(t.projs)));
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

                pip->mutable_proj()->set_projection("p" + std::to_string(rg.pickRandomlyFromSet(t.projs)));
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition<false>(rg, t, pip->mutable_partition());
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

                pip->mutable_proj()->set_projection("p" + std::to_string(rg.pickRandomlyFromSet(t.projs)));
                if (t.isMergeTreeFamily() && rg.nextBool())
                {
                    generateNextTablePartition<false>(rg, t, pip->mutable_partition());
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
                ati->mutable_remove_constraint()->set_constraint("c" + std::to_string(rg.pickRandomlyFromSet(t.constrs)));
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
                const uint32_t nopt2 = rg.nextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_detach_partition();

                if (table_has_partitions && nopt2 < 5)
                {
                    fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                else if (table_has_partitions && nopt2 < 9)
                {
                    fc.tableGetRandomPartitionOrPart<false, false>(dname, tname, buf);
                    pexpr->set_part(buf);
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
                const uint32_t nopt2 = rg.nextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_drop_partition();

                if (table_has_partitions && nopt2 < 5)
                {
                    fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                else if (table_has_partitions && nopt2 < 9)
                {
                    fc.tableGetRandomPartitionOrPart<false, false>(dname, tname, buf);
                    pexpr->set_part(buf);
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
                const uint32_t nopt2 = rg.nextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_drop_detached_partition();
                const bool table_has_detached_partitions = fc.tableHasPartitions<true>(dname, tname);

                if (table_has_detached_partitions && nopt2 < 5)
                {
                    fc.tableGetRandomPartitionOrPart<true, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                else if (table_has_detached_partitions && nopt2 < 9)
                {
                    fc.tableGetRandomPartitionOrPart<true, false>(dname, tname, buf);
                    pexpr->set_part(buf);
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
                PartitionExpr * pexpr = ati->mutable_forget_partition();

                fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                pexpr->set_partition_id(buf);
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
                const uint32_t nopt2 = rg.nextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_attach_partition();
                const bool table_has_detached_partitions = fc.tableHasPartitions<true>(dname, tname);

                if (table_has_detached_partitions && nopt2 < 5)
                {
                    fc.tableGetRandomPartitionOrPart<true, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                else if (table_has_detached_partitions && nopt2 < 9)
                {
                    fc.tableGetRandomPartitionOrPart<true, false>(dname, tname, buf);
                    pexpr->set_part(buf);
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
                PartitionExpr * pexpr = apf->mutable_partition();
                ExprSchemaTable * est2 = apf->mutable_est();
                const SQLTable & t2 = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

                fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                pexpr->set_partition_id(buf);
                if (t2.db)
                {
                    est2->mutable_database()->set_database("d" + std::to_string(t2.db->dname));
                }
                est2->mutable_table()->set_table("t" + std::to_string(t2.tname));
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
                PartitionExpr * pexpr = ccip->mutable_partition();

                fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                pexpr->set_partition_id(buf);
                flatTableColumnPath(flat_nested, t, [](const SQLColumn &) { return true; });
                columnPathRef(rg.pickRandomlyFromVector(this->entries), ccip->mutable_col());
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
                    fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                    fp->mutable_partition()->set_partition_id(buf);
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
                const uint32_t fname = rg.pickKeyRandomlyFromMap(t.frozen_partitions);
                const std::string & partition_id = t.frozen_partitions[fname];

                if (!partition_id.empty())
                {
                    fp->mutable_partition()->set_partition_id(partition_id);
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
                PartitionExpr * pexpr = ccip->mutable_partition();

                fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                pexpr->set_partition_id(buf);
                ccip->mutable_idx()->set_index("i" + std::to_string(rg.pickKeyRandomlyFromMap(t.idxs)));
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
                PartitionExpr * pexpr = mp->mutable_partition();

                fc.tableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                pexpr->set_partition_id(buf);
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
                flatTableColumnPath(0, t, [](const SQLColumn & c) { return !dynamic_cast<NestedType *>(c.tp); });
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
                comment_table
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + comment_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats
                       + mat_stats + add_idx + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting
                       + column_remove_setting + table_modify_setting + table_remove_setting + add_projection + remove_projection
                       + materialize_projection + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition
                       + drop_detached_partition + forget_partition + attach_partition + move_partition_to + clear_column_partition
                       + freeze_partition + unfreeze_partition + clear_index_partition + move_partition + modify_ttl + remove_ttl
                       + comment_table + 1))
            {
                buf.resize(0);
                rg.nextString(buf, "'", true, rg.nextRandomUInt32() % 1009);
                ati->set_comment(buf);
            }
            else
            {
                assert(0);
            }
        }
    }
    else
    {
        assert(0);
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, at->mutable_setting_values());
    }
    return 0;
}

int StatementGenerator::generateAttach(RandomGenerator & rg, Attach * att)
{
    SQLObjectName * sot = att->mutable_object();
    const uint32_t attach_table = 10 * static_cast<uint32_t>(collectionHas<SQLTable>(detached_tables));
    const uint32_t attach_view = 10 * static_cast<uint32_t>(collectionHas<SQLView>(detached_views));
    const uint32_t attach_database = 2 * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(detached_databases));
    const uint32_t prob_space = attach_table + attach_view + attach_database;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (attach_table && nopt < (attach_table + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(detached_tables));

        att->set_sobject(SQLObject::TABLE);
        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
    }
    else if (attach_view && nopt < (attach_table + attach_view + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLView & v = rg.pickRandomlyFromVector(filterCollection<SQLView>(detached_views));

        att->set_sobject(SQLObject::TABLE);
        if (v.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(v.db->dname));
        }
        est->mutable_table()->set_table("v" + std::to_string(v.tname));
    }
    else if (attach_database)
    {
        const std::shared_ptr<SQLDatabase> & d
            = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(detached_databases));

        att->set_sobject(SQLObject::DATABASE);
        sot->mutable_database()->set_database("d" + std::to_string(d->dname));
    }
    else
    {
        assert(0);
    }
    if (rg.nextSmallNumber() < 3)
    {
        att->set_as_replicated(rg.nextBool());
    }
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, att->mutable_setting_values());
    }
    return 0;
}

int StatementGenerator::generateDetach(RandomGenerator & rg, Detach * det)
{
    SQLObjectName * sot = det->mutable_object();
    const uint32_t detach_table = 10 * static_cast<uint32_t>(collectionCount<SQLTable>(attached_tables) > 3);
    const uint32_t detach_view = 10 * static_cast<uint32_t>(collectionCount<SQLView>(attached_views) > 3);
    const uint32_t detach_database = 2 * static_cast<uint32_t>(collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3);
    const uint32_t prob_space = detach_table + detach_view + detach_database;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (detach_table && nopt < (detach_table + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

        det->set_sobject(SQLObject::TABLE);
        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
    }
    else if (detach_view && nopt < (detach_table + detach_view + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLView & v = rg.pickRandomlyFromVector(filterCollection<SQLView>(attached_views));

        det->set_sobject(SQLObject::TABLE);
        if (v.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(v.db->dname));
        }
        est->mutable_table()->set_table("v" + std::to_string(v.tname));
    }
    else if (detach_database)
    {
        const std::shared_ptr<SQLDatabase> & d
            = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        det->set_sobject(SQLObject::DATABASE);
        sot->mutable_database()->set_database("d" + std::to_string(d->dname));
    }
    else
    {
        assert(0);
    }
    det->set_permanently(!detach_database && rg.nextSmallNumber() < 4);
    det->set_sync(rg.nextSmallNumber() < 4);
    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, det->mutable_setting_values());
    }
    return 0;
}

const auto has_merge_tree_func = [](const SQLTable & t)
{ return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && t.isMergeTreeFamily(); };

const auto has_replicated_merge_tree_func = [](const SQLTable & t)
{ return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && t.isReplicatedMergeTree(); };

const auto has_refreshable_view_func = [](const SQLView & v)
{ return (!v.db || v.db->attached == DetachStatus::ATTACHED) && v.attached == DetachStatus::ATTACHED && v.is_refreshable; };

int StatementGenerator::generateNextSystemStatement(RandomGenerator & rg, SystemCommand * sc)
{
    const uint32_t has_merge_tree = static_cast<uint32_t>(collectionHas<SQLTable>(has_merge_tree_func));
    const uint32_t has_replicated_merge_tree
        = has_merge_tree * static_cast<uint32_t>(collectionHas<SQLTable>(has_replicated_merge_tree_func));
    const uint32_t has_replicated_database = static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(
        [](const std::shared_ptr<SQLDatabase> & d) { return d->attached == DetachStatus::ATTACHED && d->isReplicatedDatabase(); }));
    const uint32_t has_refreshable_view = static_cast<uint32_t>(collectionHas<SQLView>(has_refreshable_view_func));
    const uint32_t reload_embedded_dictionaries = 1;
    const uint32_t reload_dictionaries = 3;
    const uint32_t reload_models = 3;
    const uint32_t reload_functions = 3;
    const uint32_t reload_function = 8 * static_cast<uint32_t>(!functions.empty());
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
    //for merge trees
    const uint32_t stop_merges = 0 * has_merge_tree;
    const uint32_t start_merges = 0 * has_merge_tree;
    const uint32_t stop_ttl_merges = 8 * has_merge_tree;
    const uint32_t start_ttl_merges = 8 * has_merge_tree;
    const uint32_t stop_moves = 8 * has_merge_tree;
    const uint32_t start_moves = 8 * has_merge_tree;
    const uint32_t wait_loading_parts = 8 * has_merge_tree;
    //for replicated merge trees
    const uint32_t stop_fetches = 8 * has_replicated_merge_tree;
    const uint32_t start_fetches = 8 * has_replicated_merge_tree;
    const uint32_t stop_replicated_sends = 8 * has_replicated_merge_tree;
    const uint32_t start_replicated_sends = 8 * has_replicated_merge_tree;
    const uint32_t stop_replication_queues = 0 * has_replicated_merge_tree;
    const uint32_t start_replication_queues = 0 * has_replicated_merge_tree;
    const uint32_t stop_pulling_replication_log = 0 * has_replicated_merge_tree;
    const uint32_t start_pulling_replication_log = 0 * has_replicated_merge_tree;
    const uint32_t sync_replica = 8 * has_replicated_merge_tree;
    const uint32_t sync_replicated_database = 8 * has_replicated_database;
    const uint32_t restart_replica = 8 * has_replicated_merge_tree;
    const uint32_t restore_replica = 8 * has_replicated_merge_tree;
    const uint32_t restart_replicas = 3;
    const uint32_t drop_filesystem_cache = 3;
    const uint32_t sync_file_cache = 1;
    //for merge trees
    const uint32_t load_pks = 3;
    const uint32_t load_pk = 8 * has_merge_tree;
    const uint32_t unload_pks = 3;
    const uint32_t unload_pk = 8 * has_merge_tree;
    //for refreshable views
    const uint32_t refresh_views = 3;
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
    const uint32_t drop_cache = 3;
    const uint32_t prob_space = reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
        + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
        + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
        + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches
        + stop_replicated_sends + start_replicated_sends + stop_replication_queues + start_replication_queues + stop_pulling_replication_log
        + start_pulling_replication_log + sync_replica + sync_replicated_database + restart_replica + restore_replica + restart_replicas
        + drop_filesystem_cache + sync_file_cache + load_pks + load_pk + unload_pks + unload_pk + refresh_views + refresh_view + stop_views
        + stop_view + start_views + start_view + cancel_view + wait_view + prewarm_cache + prewarm_primary_index_cache
        + drop_connections_cache + drop_primary_index_cache + drop_index_mark_cache + drop_index_uncompressed_cache + drop_mmap_cache
        + drop_page_cache + drop_schema_cache + drop_s3_client_cache + flush_async_insert_queue + sync_filesystem_cache + drop_cache;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

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
        const uint32_t & fname = rg.pickKeyRandomlyFromMap(this->functions);

        sc->mutable_reload_function()->set_function("f" + std::to_string(fname));
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
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_merges());
    }
    else if (
        start_merges
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges + 1))
    {
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_merges());
    }
    else if (
        stop_ttl_merges
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + 1))
    {
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_ttl_merges());
    }
    else if (
        start_ttl_merges
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + 1))
    {
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_ttl_merges());
    }
    else if (
        stop_moves
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + 1))
    {
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_stop_moves());
    }
    else if (
        start_moves
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + 1))
    {
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_start_moves());
    }
    else if (
        wait_loading_parts
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + 1))
    {
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_wait_loading_parts());
    }
    else if (
        stop_fetches
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + 1))
    {
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_stop_fetches());
    }
    else if (
        start_fetches
        && nopt
            < (reload_embedded_dictionaries + reload_dictionaries + reload_models + reload_functions + reload_function
               + reload_asynchronous_metrics + drop_dns_cache + drop_mark_cache + drop_uncompressed_cache + drop_compiled_expression_cache
               + drop_query_cache + drop_format_schema_cache + flush_logs + reload_config + reload_users + stop_merges + start_merges
               + stop_ttl_merges + start_ttl_merges + stop_moves + start_moves + wait_loading_parts + stop_fetches + start_fetches + 1))
    {
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_start_fetches());
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
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_stop_replicated_sends());
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
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_start_replicated_sends());
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
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_stop_replication_queues());
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
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_start_replication_queues());
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
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_stop_pulling_replication_log());
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
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_start_pulling_replication_log());
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

        srep->set_policy(
            static_cast<SyncReplica_SyncPolicy>((rg.nextRandomUInt32() % static_cast<uint32_t>(SyncReplica::SyncPolicy_MAX)) + 1));
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, srep->mutable_est());
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
        const std::shared_ptr<SQLDatabase> & d = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(
            [](const std::shared_ptr<SQLDatabase> & dd) { return dd->attached == DetachStatus::ATTACHED && dd->isReplicatedDatabase(); }));

        sc->mutable_sync_replicated_database()->set_database("d" + std::to_string(d->dname));
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
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_restart_replica());
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
        setTableSystemStatement<SQLTable>(rg, has_replicated_merge_tree_func, sc->mutable_restore_replica());
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
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_load_pk());
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
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_unload_pk());
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
        setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_refresh_view());
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
        setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_stop_view());
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
        setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_start_view());
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
        setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_cancel_view());
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
        setTableSystemStatement<SQLView>(rg, has_refreshable_view_func, sc->mutable_wait_view());
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
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_prewarm_cache());
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
        setTableSystemStatement<SQLTable>(rg, has_merge_tree_func, sc->mutable_prewarm_primary_index_cache());
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
        drop_cache
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
               + flush_async_insert_queue + sync_filesystem_cache + drop_cache + 1))
    {
        sc->set_drop_cache(true);
    }
    else
    {
        assert(0);
    }
    return 0;
}

int StatementGenerator::generateNextQuery(RandomGenerator & rg, SQLQueryInner * sq)
{
    const uint32_t create_table = 6
        * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases)
                                && static_cast<uint32_t>(tables.size()) < this->fc.max_tables);
    const uint32_t create_view = 10
        * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases)
                                && static_cast<uint32_t>(views.size()) < this->fc.max_views);
    const uint32_t drop = 2
        * static_cast<uint32_t>(
                              collectionCount<SQLTable>(attached_tables) > 3 || collectionCount<SQLView>(attached_views) > 3
                              || collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3 || functions.size() > 3);
    const uint32_t insert = 180 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t light_delete = 6 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t truncate = 2
        * static_cast<uint32_t>(collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases)
                                || collectionHas<SQLTable>(attached_tables));
    const uint32_t optimize_table = 2
        * static_cast<uint32_t>(collectionHas<SQLTable>(
            [](const SQLTable & t)
            {
                return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && t.isMergeTreeFamily();
            }));
    const uint32_t check_table = 2 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables));
    const uint32_t desc_table
        = 2 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables) || collectionHas<SQLView>(attached_views));
    const uint32_t exchange_tables = 1
        * static_cast<uint32_t>(collectionCount<SQLTable>(
                                    [](const SQLTable & t)
                                    {
                                        return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED
                                            && !t.hasDatabasePeer();
                                    })
                                > 1);
    const uint32_t alter_table = 6
        * static_cast<uint32_t>(collectionHas<SQLTable>(
                                    [](const SQLTable & t)
                                    {
                                        return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED
                                            && !t.isFileEngine();
                                    })
                                || collectionHas<SQLView>(attached_views));
    const uint32_t set_values = 5;
    const uint32_t attach = 2
        * static_cast<uint32_t>(collectionHas<SQLTable>(detached_tables) || collectionHas<SQLView>(detached_views)
                                || collectionHas<std::shared_ptr<SQLDatabase>>(detached_databases));
    const uint32_t detach = 2
        * static_cast<uint32_t>(collectionCount<SQLTable>(attached_tables) > 3 || collectionCount<SQLView>(attached_views) > 3
                                || collectionCount<std::shared_ptr<SQLDatabase>>(attached_databases) > 3);
    const uint32_t create_database = 2 * static_cast<uint32_t>(static_cast<uint32_t>(databases.size()) < this->fc.max_databases);
    const uint32_t create_function = 5 * static_cast<uint32_t>(static_cast<uint32_t>(functions.size()) < this->fc.max_functions);
    const uint32_t system_stmt = 1;
    const uint32_t select_query = 800;
    const uint32_t prob_space = create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table
        + desc_table + exchange_tables + alter_table + set_values + attach + detach + create_database + create_function + system_stmt
        + select_query;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    assert(this->ids.empty());
    if (create_table && nopt < (create_table + 1))
    {
        return generateNextCreateTable(rg, sq->mutable_create_table());
    }
    else if (create_view && nopt < (create_table + create_view + 1))
    {
        return generateNextCreateView(rg, sq->mutable_create_view());
    }
    else if (drop && nopt < (create_table + create_view + drop + 1))
    {
        return generateNextDrop(rg, sq->mutable_drop());
    }
    else if (insert && nopt < (create_table + create_view + drop + insert + 1))
    {
        return generateNextInsert(rg, sq->mutable_insert());
    }
    else if (light_delete && nopt < (create_table + create_view + drop + insert + light_delete + 1))
    {
        return generateNextDelete(rg, sq->mutable_del());
    }
    else if (truncate && nopt < (create_table + create_view + drop + insert + light_delete + truncate + 1))
    {
        return generateNextTruncate(rg, sq->mutable_trunc());
    }
    else if (optimize_table && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + 1))
    {
        return generateNextOptimizeTable(rg, sq->mutable_opt());
    }
    else if (
        check_table && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + 1))
    {
        return generateNextCheckTable(rg, sq->mutable_check());
    }
    else if (
        desc_table
        && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + 1))
    {
        return generateNextDescTable(rg, sq->mutable_desc());
    }
    else if (
        exchange_tables
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + 1))
    {
        return generateNextExchangeTables(rg, sq->mutable_exchange());
    }
    else if (
        alter_table
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + 1))
    {
        return generateAlterTable(rg, sq->mutable_alter_table());
    }
    else if (
        set_values
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + 1))
    {
        return generateSettingValues(rg, serverSettings, sq->mutable_setting_values());
    }
    else if (
        attach
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + 1))
    {
        return generateAttach(rg, sq->mutable_attach());
    }
    else if (
        detach
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + detach + 1))
    {
        return generateDetach(rg, sq->mutable_detach());
    }
    else if (
        create_database
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + detach + create_database + 1))
    {
        return generateNextCreateDatabase(rg, sq->mutable_create_database());
    }
    else if (
        create_function
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + detach + create_database + create_function + 1))
    {
        return generateNextCreateFunction(rg, sq->mutable_create_function());
    }
    else if (
        system_stmt
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + detach + create_database + create_function + system_stmt + 1))
    {
        return generateNextSystemStatement(rg, sq->mutable_system_cmd());
    }
    return generateTopSelect(rg, false, std::numeric_limits<uint32_t>::max(), sq->mutable_select());
}

static const std::vector<TestSetting> explain_settings{//QUERY TREE
                                                       TestSetting("run_passes", {"0", "1"}),
                                                       TestSetting("dump_passes", {"0", "1"}),
                                                       TestSetting("passes", {"-1", "0", "1", "2", "3", "4"}),
                                                       //PLAN
                                                       TestSetting("header", {"0", "1"}),
                                                       TestSetting("description", {"0", "1"}),
                                                       TestSetting("indexes", {"0", "1"}),
                                                       TestSetting("actions", {"0", "1"}),
                                                       TestSetting("json", {"0", "1"}),
                                                       //PIPELINE
                                                       TestSetting("header", {"0", "1"}),
                                                       TestSetting("graph", {"0", "1"}),
                                                       TestSetting("compact", {"0", "1"})};

int StatementGenerator::generateNextExplain(RandomGenerator & rg, ExplainQuery * eq)
{
    if (rg.nextSmallNumber() < 10)
    {
        ExplainQuery_ExplainValues val
            = static_cast<ExplainQuery_ExplainValues>((rg.nextRandomUInt32() % static_cast<uint32_t>(ExplainQuery::ExplainValues_MAX)) + 1);

        if (rg.nextBool())
        {
            uint32_t offset = 0;

            assert(this->ids.empty());
            switch (val)
            {
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE:
                    this->ids.push_back(0);
                    this->ids.push_back(1);
                    this->ids.push_back(2);
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PLAN:
                    offset = 3;
                    this->ids.push_back(3);
                    this->ids.push_back(4);
                    this->ids.push_back(5);
                    this->ids.push_back(6);
                    this->ids.push_back(7);
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PIPELINE:
                    offset = 8;
                    this->ids.push_back(8);
                    this->ids.push_back(9);
                    this->ids.push_back(10);
                    break;
                default:
                    break;
            }
            if (!this->ids.empty())
            {
                const size_t noptions = (static_cast<size_t>(rg.nextMediumNumber()) % this->ids.size()) + 1;
                std::shuffle(ids.begin(), ids.end(), rg.generator);

                for (size_t i = 0; i < noptions; i++)
                {
                    const uint32_t nopt = this->ids[i];
                    ExplainOption * eopt = eq->add_opts();

                    eopt->set_opt(nopt - offset);
                    eopt->set_val(std::stoi(rg.pickRandomlyFromSet(explain_settings[nopt].options)));
                }
                this->ids.clear();
            }
        }
        eq->set_expl(val);
    }
    return generateNextQuery(rg, eq->mutable_inner_query());
}

int StatementGenerator::generateNextStatement(RandomGenerator & rg, SQLQuery & sq)
{
    const uint32_t start_transaction = 2 * static_cast<uint32_t>(!this->in_transaction);
    const uint32_t commit = 50 * static_cast<uint32_t>(this->in_transaction);
    const uint32_t explain_query = 10;
    const uint32_t run_query = 120;
    const uint32_t prob_space = start_transaction + commit + explain_query + run_query;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (start_transaction && nopt < (start_transaction + 1))
    {
        sq.set_start_trans(true);
        return 0;
    }
    else if (commit && nopt < (start_transaction + commit + 1))
    {
        if (rg.nextSmallNumber() < 7)
        {
            sq.set_commit_trans(true);
        }
        else
        {
            sq.set_rollback_trans(true);
        }
        return 0;
    }
    else if (explain_query && nopt < (start_transaction + commit + explain_query + 1))
    {
        return generateNextExplain(rg, sq.mutable_explain());
    }
    else if (run_query)
    {
        return generateNextQuery(rg, sq.mutable_inner_query());
    }
    else
    {
        assert(0);
        return 0;
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
    this->databases.erase(dname);
}

void StatementGenerator::updateGenerator(const SQLQuery & sq, ExternalIntegrations & ei, bool success)
{
    const SQLQueryInner & query = sq.has_inner_query() ? sq.inner_query() : sq.explain().inner_query();

    success &= (!ei.getRequiresExternalCallCheck() || ei.getNextExternalCallSucceeded());

    if ((sq.has_explain() || sq.has_inner_query()) && query.has_create_table())
    {
        const uint32_t tname = static_cast<uint32_t>(std::stoul(query.create_table().est().table().table().substr(1)));

        if (sq.has_inner_query() && success)
        {
            if (query.create_table().replace())
            {
                dropTable(false, true, tname);
            }
            this->tables[tname] = std::move(this->staged_tables[tname]);
        }
        dropTable(true, !success, tname);
    }
    else if ((sq.has_explain() || sq.has_inner_query()) && query.has_create_view())
    {
        const uint32_t tname = static_cast<uint32_t>(std::stoul(query.create_view().est().table().table().substr(1)));

        if (sq.has_inner_query() && success)
        {
            if (query.create_view().replace())
            {
                this->views.erase(tname);
            }
            this->views[tname] = std::move(this->staged_views[tname]);
        }
        this->staged_views.erase(tname);
    }
    else if (sq.has_inner_query() && query.has_drop() && success)
    {
        const Drop & drp = query.drop();

        if (drp.sobject() == SQLObject::TABLE)
        {
            dropTable(false, true, static_cast<uint32_t>(std::stoul(drp.object().est().table().table().substr(1))));
        }
        else if (drp.sobject() == SQLObject::VIEW)
        {
            this->views.erase(static_cast<uint32_t>(std::stoul(drp.object().est().table().table().substr(1))));
        }
        else if (drp.sobject() == SQLObject::DATABASE)
        {
            dropDatabase(static_cast<uint32_t>(std::stoul(drp.object().database().database().substr(1))));
        }
        else if (drp.sobject() == SQLObject::FUNCTION)
        {
            this->functions.erase(static_cast<uint32_t>(std::stoul(drp.object().function().function().substr(1))));
        }
    }
    else if (sq.has_inner_query() && query.has_exchange() && success)
    {
        const uint32_t tname1 = static_cast<uint32_t>(std::stoul(query.exchange().est1().table().table().substr(1)));
        const uint32_t tname2 = static_cast<uint32_t>(std::stoul(query.exchange().est2().table().table().substr(1)));
        SQLTable tx = std::move(this->tables[tname1]);
        SQLTable ty = std::move(this->tables[tname2]);
        auto db_tmp = tx.db;

        tx.tname = tname2;
        tx.db = ty.db;
        ty.tname = tname1;
        ty.db = db_tmp;
        this->tables[tname2] = std::move(tx);
        this->tables[tname1] = std::move(ty);
    }
    else if (sq.has_inner_query() && query.has_alter_table())
    {
        const AlterTable & at = sq.inner_query().alter_table();
        const bool isview = at.est().table().table()[0] == 'v';
        const uint32_t tname = static_cast<uint32_t>(std::stoul(at.est().table().table().substr(1)));

        if (isview)
        {
            SQLView & v = this->views[tname];

            for (int i = 0; i < at.other_alters_size() + 1; i++)
            {
                const AlterTableItem & ati = i == 0 ? at.alter() : at.other_alters(i - 1);

                if (success && ati.has_add_column())
                {
                    v.ncols = v.staged_ncols;
                }
            }
        }
        else
        {
            SQLTable & t = this->tables[tname];

            for (int i = 0; i < at.other_alters_size() + 1; i++)
            {
                const AlterTableItem & ati = i == 0 ? at.alter() : at.other_alters(i - 1);

                assert(!ati.has_modify_query() && !ati.has_refresh());
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

                        assert(path.sub_cols_size() == 1);
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

                        assert(path.sub_cols_size() == 1);
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

                    t.frozen_partitions[fp.fname()] = fp.has_partition() ? fp.partition().partition_id() : "";
                }
                else if (ati.has_unfreeze_partition() && success)
                {
                    t.frozen_partitions.erase(ati.unfreeze_partition().fname());
                }
            }
        }
    }
    else if (sq.has_inner_query() && query.has_attach() && success)
    {
        const Attach & att = sq.inner_query().attach();
        const bool istable = att.object().has_est() && att.object().est().table().table()[0] == 't';
        const bool isview = att.object().has_est() && att.object().est().table().table()[0] == 'v';
        const bool isdatabase = att.object().has_database();

        if (isview)
        {
            this->views[static_cast<uint32_t>(std::stoul(att.object().est().table().table().substr(1)))].attached = DetachStatus::ATTACHED;
        }
        else if (istable)
        {
            this->tables[static_cast<uint32_t>(std::stoul(att.object().est().table().table().substr(1)))].attached = DetachStatus::ATTACHED;
        }
        else if (isdatabase)
        {
            const uint32_t dname = static_cast<uint32_t>(std::stoul(att.object().database().database().substr(1)));

            this->databases[dname]->attached = DetachStatus::ATTACHED;
            for (auto & [_, table] : this->tables)
            {
                if (table.db && table.db->dname == dname)
                {
                    table.attached = std::max(table.attached, DetachStatus::DETACHED);
                }
            }
        }
    }
    else if (sq.has_inner_query() && query.has_detach() && success)
    {
        const Detach & det = sq.inner_query().detach();
        const bool istable = det.object().has_est() && det.object().est().table().table()[0] == 't';
        const bool isview = det.object().has_est() && det.object().est().table().table()[0] == 'v';
        const bool isdatabase = det.object().has_database();
        const bool is_permanent = det.permanently();

        if (isview)
        {
            this->views[static_cast<uint32_t>(std::stoul(det.object().est().table().table().substr(1)))].attached
                = is_permanent ? DetachStatus::PERM_DETACHED : DetachStatus::DETACHED;
        }
        else if (istable)
        {
            this->tables[static_cast<uint32_t>(std::stoul(det.object().est().table().table().substr(1)))].attached
                = is_permanent ? DetachStatus::PERM_DETACHED : DetachStatus::DETACHED;
        }
        else if (isdatabase)
        {
            const uint32_t dname = static_cast<uint32_t>(std::stoul(det.object().database().database().substr(1)));

            this->databases[dname]->attached = DetachStatus::DETACHED;
            for (auto & [_, table] : this->tables)
            {
                if (table.db && table.db->dname == dname)
                {
                    table.attached = std::max(table.attached, DetachStatus::DETACHED);
                }
            }
        }
    }
    else if ((sq.has_explain() || sq.has_inner_query()) && query.has_create_database())
    {
        const uint32_t dname = static_cast<uint32_t>(std::stoul(query.create_database().database().database().substr(1)));

        if (sq.has_inner_query() && success)
        {
            this->databases[dname] = std::move(this->staged_databases[dname]);
        }
        this->staged_databases.erase(dname);
    }
    else if ((sq.has_explain() || sq.has_inner_query()) && query.has_create_function())
    {
        const uint32_t fname = static_cast<uint32_t>(std::stoul(query.create_function().function().function().substr(1)));

        if (sq.has_inner_query() && success)
        {
            this->functions[fname] = std::move(this->staged_functions[fname]);
        }
        this->staged_functions.erase(fname);
    }
    else if (sq.has_inner_query() && query.has_trunc() && query.trunc().has_database())
    {
        dropDatabase(static_cast<uint32_t>(std::stoul(query.trunc().database().database().substr(1))));
    }
    else if (sq.has_start_trans() && success)
    {
        this->in_transaction = true;
    }
    else if ((sq.has_commit_trans() || sq.has_rollback_trans()) && success)
    {
        this->in_transaction = false;
    }

    ei.resetExternalStatus();
}

}
