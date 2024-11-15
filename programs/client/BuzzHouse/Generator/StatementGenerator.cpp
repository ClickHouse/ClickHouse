#include "StatementGenerator.h"
#include "RandomSettings.h"
#include "SQLCatalog.h"
#include "SQLTypes.h"

#include <algorithm>
#include <optional>

namespace BuzzHouse
{

int StatementGenerator::GenerateSettingValues(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    const size_t nvalues,
    SettingValues * vals)
{
    for (size_t i = 0; i < nvalues; i++)
    {
        SetValue * sv = i == 0 ? vals->mutable_set_value() : vals->add_other_values();

        SetRandomSetting(rg, settings, this->buf, sv);
    }
    return 0;
}

int StatementGenerator::GenerateSettingValues(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    SettingValues * vals)
{
    return GenerateSettingValues(
        rg, settings, std::min<size_t>(settings.size(), static_cast<size_t>((rg.NextRandomUInt32() % 10) + 1)), vals);
}

int StatementGenerator::GenerateSettingList(
    RandomGenerator & rg, const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings, SettingList * sl)
{
    const size_t nvalues = std::min<size_t>(settings.size(), static_cast<size_t>((rg.NextRandomUInt32() % 4) + 1));

    for (size_t i = 0; i < nvalues; i++)
    {
        const std::string & next = rg.PickKeyRandomlyFromMap(settings);

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

int StatementGenerator::GenerateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd)
{
    SQLDatabase next;
    const uint32_t dname = this->database_counter++;
    DatabaseEngine * deng = cd->mutable_dengine();
    DatabaseEngineValues val = rg.NextBool() ? DatabaseEngineValues::DAtomic : DatabaseEngineValues::DReplicated;

    next.deng = val;
    deng->set_engine(val);
    if (val == DatabaseEngineValues::DReplicated)
    {
        deng->set_zoo_path(this->zoo_path_counter++);
    }
    next.dname = dname;
    cd->mutable_database()->set_database("d" + std::to_string(dname));
    this->staged_databases[dname] = std::make_shared<SQLDatabase>(std::move(next));
    return 0;
}

int StatementGenerator::GenerateNextCreateFunction(RandomGenerator & rg, CreateFunction * cf)
{
    SQLFunction next;
    const uint32_t fname = this->function_counter++;

    next.fname = fname;
    next.nargs = std::min(this->fc.max_width - this->width, (rg.NextMediumNumber() % (rg.NextBool() ? 4 : 10)));
    if ((next.is_deterministic = rg.NextBool()))
    {
        //if this function is later called by an oracle, then don't call it
        this->SetAllowNotDetermistic(false);
        this->EnforceFinal(true);
    }
    GenerateLambdaCall(rg, next.nargs, cf->mutable_lexpr());
    this->levels.clear();
    if (next.is_deterministic)
    {
        this->SetAllowNotDetermistic(true);
        this->EnforceFinal(false);
    }
    cf->mutable_function()->set_function("f" + std::to_string(fname));
    this->staged_functions[fname] = std::move(next);
    return 0;
}

static void SetViewInterval(RandomGenerator & rg, RefreshInterval * ri)
{
    ri->set_interval(rg.NextSmallNumber() - 1);
    ri->set_unit(RefreshInterval_RefreshUnit::RefreshInterval_RefreshUnit_SECOND);
}

int StatementGenerator::GenerateNextRefreshableView(RandomGenerator & rg, RefreshableView * cv)
{
    const RefreshableView_RefreshPolicy pol = rg.NextBool() ? RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_EVERY
                                                            : RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_AFTER;

    cv->set_policy(pol);
    SetViewInterval(rg, cv->mutable_interval());
    if (pol == RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_EVERY && rg.NextBool())
    {
        SetViewInterval(rg, cv->mutable_offset());
    }
    SetViewInterval(rg, cv->mutable_randomize());
    cv->set_append(rg.NextBool());
    return 0;
}

int StatementGenerator::GenerateNextCreateView(RandomGenerator & rg, CreateView * cv)
{
    SQLView next;
    uint32_t tname = 0;
    ExprSchemaTable * est = cv->mutable_est();
    const bool replace = CollectionCount<SQLView>(attached_views) > 3 && rg.NextMediumNumber() < 16;

    if (replace)
    {
        const SQLView & v = rg.PickRandomlyFromVector(FilterCollection<SQLView>(attached_views));

        next.db = v.db;
        tname = next.tname = v.tname;
    }
    else
    {
        if (rg.NextSmallNumber() < 9)
        {
            next.db = rg.PickRandomlyFromVector(FilterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    cv->set_replace(replace);
    next.is_materialized = rg.NextBool();
    cv->set_materialized(next.is_materialized);
    next.ncols = (rg.NextMediumNumber() % (rg.NextBool() ? 5 : 30)) + 1;
    if (next.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(next.db->dname));
    }
    est->mutable_table()->set_table("v" + std::to_string(next.tname));
    if (next.is_materialized)
    {
        TableEngine * te = cv->mutable_engine();

        next.teng = GetNextTableEngine(rg, false);
        te->set_engine(next.teng);

        assert(this->entries.empty());
        for (uint32_t i = 0; i < next.ncols; i++)
        {
            entries.push_back(InsertEntry(true, ColumnSpecial::NONE, i, std::nullopt, nullptr, std::nullopt));
        }
        GenerateEngineDetails(rg, next, true, te);
        this->entries.clear();

        if (CollectionHas<SQLTable>(attached_tables) && rg.NextSmallNumber() < 5)
        {
            const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

            cv->mutable_to_est()->mutable_table()->set_table("t" + std::to_string(t.tname));
        }
        if ((next.is_refreshable = rg.NextBool()))
        {
            GenerateNextRefreshableView(rg, cv->mutable_refresh());
            cv->set_empty(rg.NextBool());
        }
        else
        {
            cv->set_populate(rg.NextSmallNumber() < 4);
        }
    }
    if ((next.is_deterministic = rg.NextBool()))
    {
        this->SetAllowNotDetermistic(false);
        this->EnforceFinal(true);
    }
    this->levels[this->current_level] = QueryLevel(this->current_level);
    GenerateSelect(
        rg, false, next.ncols, next.is_materialized ? (~allow_prewhere) : std::numeric_limits<uint32_t>::max(), cv->mutable_select());
    if (next.is_deterministic)
    {
        this->SetAllowNotDetermistic(true);
        this->EnforceFinal(false);
    }
    assert(!next.toption.has_value() || next.IsMergeTreeFamily());
    this->staged_views[tname] = std::move(next);
    return 0;
}

int StatementGenerator::GenerateNextDrop(RandomGenerator & rg, Drop * dp)
{
    SQLObjectName * sot = dp->mutable_object();
    const uint32_t drop_table = 10 * static_cast<uint32_t>(CollectionHas<SQLTable>(attached_tables)),
                   drop_view = 10 * static_cast<uint32_t>(CollectionHas<SQLView>(attached_views)),
                   drop_database = 2 * static_cast<uint32_t>(CollectionHas<std::shared_ptr<SQLDatabase>>(attached_databases)),
                   drop_function = 1 * static_cast<uint32_t>(!functions.empty()),
                   prob_space = drop_table + drop_view + drop_database + drop_function;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.gen);

    if (drop_table && nopt < (drop_table + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

        dp->set_is_temp(t.is_temp);
        dp->set_sobject(SQLObject::TABLE);
        dp->set_if_empty(rg.NextSmallNumber() < 4);
        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
    }
    else if (drop_view && nopt < (drop_table + drop_view + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLView & v = rg.PickRandomlyFromVector(FilterCollection<SQLView>(attached_views));

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
            = rg.PickRandomlyFromVector(FilterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        dp->set_sobject(SQLObject::DATABASE);
        sot->mutable_database()->set_database("d" + std::to_string(d->dname));
    }
    else if (drop_function)
    {
        const uint32_t & fname = rg.PickKeyRandomlyFromMap(this->functions);

        dp->set_sobject(SQLObject::FUNCTION);
        sot->mutable_function()->set_function("f" + std::to_string(fname));
    }
    else
    {
        assert(0);
    }
    dp->set_sync(rg.NextSmallNumber() < 3);
    return 0;
}

int StatementGenerator::GenerateNextOptimizeTable(RandomGenerator & rg, OptimizeTable * ot)
{
    ExprSchemaTable * est = ot->mutable_est();
    const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(
        [](const SQLTable & st)
        {
            return (!st.db || st.db->attached == DetachStatus::ATTACHED) && st.attached == DetachStatus::ATTACHED && st.IsMergeTreeFamily();
        }));

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    if (t.IsMergeTreeFamily())
    {
        if (rg.NextBool())
        {
            GenerateNextTablePartition<false>(rg, t, ot->mutable_partition());
        }
        ot->set_cleanup(rg.NextSmallNumber() < 3);
    }
    if (rg.NextSmallNumber() < 4)
    {
        const uint32_t noption = rg.NextMediumNumber();
        DeduplicateExpr * dde = ot->mutable_dedup();

        if (noption < 51)
        {
            const NestedType * ntp = nullptr;
            ExprColumnList * ecl = noption < 26 ? dde->mutable_col_list() : dde->mutable_ded_star_except();
            const uint32_t ocols
                = (rg.NextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(t.RealNumberOfColumns()), UINT32_C(4))) + 1;

            assert(entries.empty());
            for (const auto & entry : t.cols)
            {
                if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
                {
                    for (const auto & entry2 : ntp->subtypes)
                    {
                        entries.push_back(InsertEntry(
                            std::nullopt,
                            ColumnSpecial::NONE,
                            entry.second.cname,
                            std::optional<uint32_t>(entry2.cname),
                            entry2.array_subtype,
                            entry.second.dmod));
                    }
                }
                else
                {
                    entries.push_back(InsertEntry(
                        entry.second.nullable, entry.second.special, entry.second.cname, std::nullopt, entry.second.tp, entry.second.dmod));
                }
            }
            std::shuffle(entries.begin(), entries.end(), rg.gen);
            for (uint32_t i = 0; i < ocols; i++)
            {
                const InsertEntry & entry = this->entries[i];
                ExprColumn * ec = i == 0 ? ecl->mutable_col() : ecl->add_extra_cols();

                ec->mutable_col()->set_column("c" + std::to_string(entry.cname1));
                if (entry.cname2.has_value())
                {
                    ec->mutable_subcol()->set_column("c" + std::to_string(entry.cname2.value()));
                }
            }
            entries.clear();
        }
        else if (noption < 76)
        {
            dde->set_ded_star(true);
        }
    }
    ot->set_final((t.SupportsFinal() || t.IsMergeTreeFamily()) && rg.NextSmallNumber() < 3);
    return 0;
}

int StatementGenerator::GenerateNextCheckTable(RandomGenerator & rg, CheckTable * ct)
{
    ExprSchemaTable * est = ct->mutable_est();
    const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    if (t.IsMergeTreeFamily() && rg.NextBool())
    {
        GenerateNextTablePartition<true>(rg, t, ct->mutable_partition());
    }
    ct->set_single_result(rg.NextSmallNumber() < 4);
    return 0;
}

int StatementGenerator::GenerateNextDescTable(RandomGenerator & rg, DescTable * dt)
{
    ExprSchemaTable * est = dt->mutable_est();
    const bool has_tables = CollectionHas<SQLTable>(attached_tables), has_views = CollectionHas<SQLView>(attached_views);

    if (has_views && (!has_tables || rg.NextBool()))
    {
        const SQLView & v = rg.PickRandomlyFromVector(FilterCollection<SQLView>(attached_views));

        if (v.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(v.db->dname));
        }
        est->mutable_table()->set_table("v" + std::to_string(v.tname));
    }
    else if (has_tables)
    {
        const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

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
    dt->set_sub_cols(rg.NextSmallNumber() < 4);
    return 0;
}

int StatementGenerator::GenerateNextInsert(RandomGenerator & rg, Insert * ins)
{
    const NestedType * ntp = nullptr;
    const uint32_t noption = rg.NextMediumNumber();
    InsertIntoTable * iit = ins->mutable_itable();
    ExprSchemaTable * est = iit->mutable_est();
    const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    assert(this->entries.empty());
    for (const auto & entry : t.cols)
    {
        if (entry.second.CanBeInserted())
        {
            if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
            {
                for (const auto & entry2 : ntp->subtypes)
                {
                    this->entries.push_back(InsertEntry(
                        std::nullopt,
                        ColumnSpecial::NONE,
                        entry.second.cname,
                        std::optional<uint32_t>(entry2.cname),
                        entry2.array_subtype,
                        entry.second.dmod));
                }
            }
            else
            {
                this->entries.push_back(InsertEntry(
                    entry.second.nullable, entry.second.special, entry.second.cname, std::nullopt, entry.second.tp, entry.second.dmod));
            }
        }
    }
    std::shuffle(this->entries.begin(), this->entries.end(), rg.gen);

    for (const auto & entry : this->entries)
    {
        InsertEntryRefCP(entry, iit->add_cols());
    }

    if (noption < 901)
    {
        const uint32_t nrows = rg.NextMediumNumber();

        buf.resize(0);
        for (uint32_t i = 0; i < nrows; i++)
        {
            uint32_t j = 0;
            const uint32_t next_nested_rows = rg.NextLargeNumber() % 100;

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
                if ((entry.dmod.has_value() && entry.dmod.value() == DModifier::DEF_DEFAULT && rg.NextMediumNumber() < 6)
                    || rg.NextLargeNumber() < 2)
                {
                    buf += "DEFAULT";
                }
                else if (entry.special == ColumnSpecial::SIGN)
                {
                    buf += rg.NextBool() ? "1" : "-1";
                }
                else if (entry.special == ColumnSpecial::IS_DELETED)
                {
                    buf += rg.NextBool() ? "1" : "0";
                }
                else if (entry.cname2.has_value())
                {
                    //make sure all nested entries have the same number of rows
                    StrAppendArray(rg, buf, dynamic_cast<const ArrayType *>(entry.tp), next_nested_rows);
                }
                else
                {
                    StrAppendAnyValue(rg, buf, entry.tp);
                }
                j++;
            }
            buf += ")";
        }
        ins->set_query(buf);
    }
    else if (noption < 951)
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);
        if (rg.NextMediumNumber() < 13)
        {
            this->AddCTEs(rg, std::numeric_limits<uint32_t>::max(), ins->mutable_ctes());
        }
        GenerateSelect(rg, true, static_cast<uint32_t>(this->entries.size()), std::numeric_limits<uint32_t>::max(), ins->mutable_select());
    }
    else
    {
        const uint32_t nrows = (rg.NextSmallNumber() % 3) + 1;
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
                    expr->mutable_lit_val()->mutable_int_lit()->set_int_lit(rg.NextBool() ? 1 : -1);
                }
                else if (entry.special == ColumnSpecial::IS_DELETED)
                {
                    expr->mutable_lit_val()->mutable_int_lit()->set_int_lit(rg.NextBool() ? 1 : 0);
                }
                else
                {
                    GenerateExpression(rg, expr);
                }
                first = false;
            }
        }
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;
        this->levels.clear();
    }
    this->entries.clear();
    return 0;
}

int StatementGenerator::GenerateUptDelWhere(RandomGenerator & rg, const SQLTable & t, Expr * expr)
{
    if (rg.NextSmallNumber() < 10)
    {
        AddTableRelation(rg, true, "", t);
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
        GenerateWherePredicate(rg, expr);
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;
        this->levels.clear();
    }
    else
    {
        expr->mutable_lit_val()->set_special_val(SpecialVal::VAL_TRUE);
    }
    return 0;
}

int StatementGenerator::GenerateNextDelete(RandomGenerator & rg, LightDelete * del)
{
    ExprSchemaTable * est = del->mutable_est();
    const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

    if (t.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(t.tname));
    if (t.IsMergeTreeFamily() && rg.NextBool())
    {
        GenerateNextTablePartition<false>(rg, t, del->mutable_partition());
    }
    GenerateUptDelWhere(rg, t, del->mutable_where()->mutable_expr()->mutable_expr());
    return 0;
}

int StatementGenerator::GenerateNextTruncate(RandomGenerator & rg, Truncate * trunc)
{
    const bool trunc_database = CollectionHas<std::shared_ptr<SQLDatabase>>(attached_databases);
    const uint32_t trunc_table = 980 * static_cast<uint32_t>(CollectionHas<SQLTable>(attached_tables)),
                   trunc_db_tables = 15 * static_cast<uint32_t>(trunc_database), trunc_db = 5 * static_cast<uint32_t>(trunc_database),
                   prob_space = trunc_table + trunc_db_tables + trunc_db;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.gen);

    if (trunc_table && nopt < (trunc_table + 1))
    {
        ExprSchemaTable * est = trunc->mutable_est();
        const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
    }
    else if (trunc_db_tables && nopt < (trunc_table + trunc_db_tables + 1))
    {
        const std::shared_ptr<SQLDatabase> & d
            = rg.PickRandomlyFromVector(FilterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        trunc->mutable_all_tables()->set_database("d" + std::to_string(d->dname));
    }
    else if (trunc_db && nopt < (trunc_table + trunc_db_tables + trunc_db + 1))
    {
        const std::shared_ptr<SQLDatabase> & d
            = rg.PickRandomlyFromVector(FilterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        trunc->mutable_database()->set_database("d" + std::to_string(d->dname));
    }
    else
    {
        assert(0);
    }
    trunc->set_sync(rg.NextSmallNumber() < 4);
    return 0;
}

int StatementGenerator::GenerateNextExchangeTables(RandomGenerator & rg, ExchangeTables * et)
{
    ExprSchemaTable *est1 = et->mutable_est1(), *est2 = et->mutable_est2();
    const auto & input = FilterCollection<SQLTable>(attached_tables);

    for (const auto & entry : input)
    {
        this->ids.push_back(entry.get().tname);
    }
    std::shuffle(this->ids.begin(), this->ids.end(), rg.gen);
    const SQLTable &t1 = this->tables[this->ids[0]], &t2 = this->tables[this->ids[1]];

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
    return 0;
}

int StatementGenerator::GenerateAlterTable(RandomGenerator & rg, AlterTable * at)
{
    ExprSchemaTable * est = at->mutable_est();
    const uint32_t nalters = rg.NextBool() ? 1 : ((rg.NextMediumNumber() % 4) + 1);
    const bool has_tables
        = CollectionHas<SQLTable>(
            [](const SQLTable & tt)
            {
                return (!tt.db || tt.db->attached == DetachStatus::ATTACHED) && tt.attached == DetachStatus::ATTACHED && !tt.IsFileEngine();
            }),
        has_views = CollectionHas<SQLView>(attached_views);

    if (has_views && (!has_tables || rg.NextBool()))
    {
        SQLView & v = const_cast<SQLView &>(rg.PickRandomlyFromVector(FilterCollection<SQLView>(attached_views)).get());

        if (v.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(v.db->dname));
        }
        est->mutable_table()->set_table("v" + std::to_string(v.tname));
        for (uint32_t i = 0; i < nalters; i++)
        {
            const uint32_t alter_refresh = 1 * static_cast<uint32_t>(v.is_refreshable), alter_query = 3,
                           prob_space = alter_refresh + alter_query;
            AlterTableItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.gen);

            if (alter_refresh && nopt < (alter_refresh + 1))
            {
                GenerateNextRefreshableView(rg, ati->mutable_refresh());
            }
            else
            {
                v.staged_ncols = (rg.NextMediumNumber() % (rg.NextBool() ? 5 : 30)) + 1;
                if (v.is_deterministic)
                {
                    this->SetAllowNotDetermistic(false);
                    this->EnforceFinal(true);
                }
                this->levels[this->current_level] = QueryLevel(this->current_level);
                GenerateSelect(
                    rg,
                    false,
                    v.staged_ncols,
                    v.is_materialized ? (~allow_prewhere) : std::numeric_limits<uint32_t>::max(),
                    ati->mutable_modify_query());
                if (v.is_deterministic)
                {
                    this->SetAllowNotDetermistic(true);
                    this->EnforceFinal(false);
                }
            }
        }
    }
    else if (has_tables)
    {
        SQLTable & t = const_cast<SQLTable &>(rg.PickRandomlyFromVector(FilterCollection<SQLTable>(
                                                                            [](const SQLTable & tt)
                                                                            {
                                                                                return (!tt.db || tt.db->attached == DetachStatus::ATTACHED)
                                                                                    && tt.attached == DetachStatus::ATTACHED
                                                                                    && !tt.IsFileEngine();
                                                                            }))
                                                  .get());
        const std::string dname = t.db ? ("d" + std::to_string(t.db->dname)) : "", tname = "t" + std::to_string(t.tname);
        const bool table_has_partitions = t.IsMergeTreeFamily() && fc.TableHasPartitions<false>(dname, tname);

        at->set_is_temp(t.is_temp);
        if (t.db)
        {
            est->mutable_database()->set_database(dname);
        }
        est->mutable_table()->set_table(tname);
        for (uint32_t i = 0; i < nalters; i++)
        {
            const uint32_t alter_order_by
                = 3 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                heavy_delete = 30, heavy_update = 40, add_column = 2 * static_cast<uint32_t>(t.cols.size() < 10), materialize_column = 2,
                drop_column = 2 * static_cast<uint32_t>(t.cols.size() > 1), rename_column = 2, clear_column = 2, modify_column = 2,
                add_stats = 3 * static_cast<uint32_t>(t.IsMergeTreeFamily()), mod_stats = 3 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                drop_stats = 3 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                clear_stats = 3 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                mat_stats = 3 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                delete_mask = 8 * static_cast<uint32_t>(t.IsMergeTreeFamily()), add_idx = 2 * static_cast<uint32_t>(t.idxs.size() < 3),
                materialize_idx = 2 * static_cast<uint32_t>(!t.idxs.empty()), clear_idx = 2 * static_cast<uint32_t>(!t.idxs.empty()),
                drop_idx = 2 * static_cast<uint32_t>(!t.idxs.empty()), column_remove_property = 2,
                column_modify_setting = 2 * static_cast<uint32_t>(!AllColumnSettings.at(t.teng).empty()),
                column_remove_setting = 2 * static_cast<uint32_t>(!AllColumnSettings.at(t.teng).empty()),
                table_modify_setting = 2 * static_cast<uint32_t>(!AllTableSettings.at(t.teng).empty()),
                table_remove_setting = 2 * static_cast<uint32_t>(!AllTableSettings.at(t.teng).empty()),
                add_projection = 2 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                remove_projection = 2 * static_cast<uint32_t>(t.IsMergeTreeFamily() && !t.projs.empty()),
                materialize_projection = 2 * static_cast<uint32_t>(t.IsMergeTreeFamily() && !t.projs.empty()),
                clear_projection = 2 * static_cast<uint32_t>(t.IsMergeTreeFamily() && !t.projs.empty()),
                add_constraint = 2 * static_cast<uint32_t>(t.constrs.size() < 4),
                remove_constraint = 2 * static_cast<uint32_t>(!t.constrs.empty()),
                detach_partition = 5 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                drop_partition = 5 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                drop_detached_partition = 5 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                forget_partition = 5 * static_cast<uint32_t>(table_has_partitions),
                attach_partition = 5 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                move_partition_to = 5 * static_cast<uint32_t>(table_has_partitions),
                clear_column_partition = 5 * static_cast<uint32_t>(table_has_partitions),
                freeze_partition = 5 * static_cast<uint32_t>(t.IsMergeTreeFamily()),
                unfreeze_partition = 7 * static_cast<uint32_t>(!t.frozen_partitions.empty()),
                clear_index_partition = 5 * static_cast<uint32_t>(table_has_partitions && !t.idxs.empty()),
                prob_space = alter_order_by + heavy_delete + heavy_update + add_column + materialize_column + drop_column + rename_column
                + clear_column + modify_column + delete_mask + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + drop_detached_partition
                + forget_partition + attach_partition + move_partition_to + clear_column_partition + freeze_partition + unfreeze_partition
                + clear_index_partition;
            AlterTableItem * ati = i == 0 ? at->mutable_alter() : at->add_other_alters();
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.gen);

            if (alter_order_by && nopt < (alter_order_by + 1))
            {
                TableKey * tkey = ati->mutable_order();

                if (rg.NextSmallNumber() < 6)
                {
                    const NestedType * ntp = nullptr;

                    assert(this->entries.empty());
                    for (const auto & entry : t.cols)
                    {
                        if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
                        {
                            for (const auto & entry2 : ntp->subtypes)
                            {
                                if (!dynamic_cast<const JSONType *>(entry2.subtype))
                                {
                                    entries.push_back(InsertEntry(
                                        std::nullopt,
                                        ColumnSpecial::NONE,
                                        entry.second.cname,
                                        std::optional<uint32_t>(entry2.cname),
                                        entry2.array_subtype,
                                        entry.second.dmod));
                                }
                            }
                        }
                        else if (!dynamic_cast<const JSONType *>(entry.second.tp))
                        {
                            entries.push_back(InsertEntry(
                                entry.second.nullable,
                                entry.second.special,
                                entry.second.cname,
                                std::nullopt,
                                entry.second.tp,
                                entry.second.dmod));
                        }
                    }
                    GenerateTableKey(rg, tkey);
                    this->entries.clear();
                }
            }
            else if (heavy_delete && nopt < (heavy_delete + alter_order_by + 1))
            {
                HeavyDelete * hdel = ati->mutable_del();

                if (t.IsMergeTreeFamily() && rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, hdel->mutable_partition());
                }
                GenerateUptDelWhere(rg, t, hdel->mutable_del()->mutable_expr()->mutable_expr());
            }
            else if (add_column && nopt < (heavy_delete + alter_order_by + add_column + 1))
            {
                const uint32_t next_option = rg.NextSmallNumber();
                AddColumn * add_col = ati->mutable_add_column();

                AddTableColumn(
                    rg, t, t.col_counter++, true, false, rg.NextMediumNumber() < 6, ColumnSpecial::NONE, add_col->mutable_new_col());
                if (next_option < 4)
                {
                    add_col->mutable_add_where()->mutable_col()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
                }
                else if (next_option < 8)
                {
                    add_col->mutable_add_where()->set_first(true);
                }
            }
            else if (materialize_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + 1))
            {
                ColInPartition * mcol = ati->mutable_materialize_column();

                mcol->mutable_col()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
                if (t.IsMergeTreeFamily() && rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, mcol->mutable_partition());
                }
            }
            else if (drop_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + 1))
            {
                ati->mutable_drop_column()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
            }
            else if (
                rename_column && nopt < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + 1))
            {
                const uint32_t ncname = t.col_counter++;
                RenameCol * rcol = ati->mutable_rename_column();

                rcol->mutable_old_name()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
                rcol->mutable_new_name()->set_column("c" + std::to_string(ncname));
            }
            else if (
                clear_column
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column + 1))
            {
                ColInPartition * ccol = ati->mutable_clear_column();

                ccol->mutable_col()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
                if (t.IsMergeTreeFamily() && rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, ccol->mutable_partition());
                }
            }
            else if (
                modify_column
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + 1))
            {
                const uint32_t next_option = rg.NextSmallNumber();
                AddColumn * add_col = ati->mutable_modify_column();

                AddTableColumn(
                    rg,
                    t,
                    rg.PickKeyRandomlyFromMap(t.cols),
                    true,
                    true,
                    rg.NextMediumNumber() < 6,
                    ColumnSpecial::NONE,
                    add_col->mutable_new_col());
                if (next_option < 4)
                {
                    add_col->mutable_add_where()->mutable_col()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
                }
                else if (next_option < 8)
                {
                    add_col->mutable_add_where()->set_first(true);
                }
            }
            else if (
                delete_mask
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + 1))
            {
                ApplyDeleteMask * adm = ati->mutable_delete_mask();

                if (rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, adm->mutable_partition());
                }
            }
            else if (
                heavy_update
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + 1))
            {
                Update * upt = ati->mutable_update();

                if (t.IsMergeTreeFamily() && rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, upt->mutable_partition());
                }
                assert(this->entries.empty());
                for (const auto & entry : t.cols)
                {
                    if (!dynamic_cast<const NestedType *>(entry.second.tp))
                    {
                        this->entries.push_back(InsertEntry(
                            entry.second.nullable,
                            entry.second.special,
                            entry.second.cname,
                            std::nullopt,
                            entry.second.tp,
                            entry.second.dmod));
                    }
                }
                if (this->entries.empty())
                {
                    UpdateSet * upset = upt->mutable_update();

                    upset->mutable_col()->mutable_col()->set_column("c0");
                    upset->mutable_expr()->mutable_lit_val()->mutable_int_lit()->set_int_lit(0);
                }
                else
                {
                    const uint32_t nupdates
                        = (rg.NextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(4))) + 1;

                    std::shuffle(this->entries.begin(), this->entries.end(), rg.gen);
                    for (uint32_t j = 0; j < nupdates; j++)
                    {
                        InsertEntryRefCP(
                            this->entries[j], j == 0 ? upt->mutable_update()->mutable_col() : upt->add_other_updates()->mutable_col());
                    }
                    AddTableRelation(rg, true, "", t);
                    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
                    for (uint32_t j = 0; j < nupdates; j++)
                    {
                        const InsertEntry & entry = this->entries[j];
                        UpdateSet & uset = const_cast<UpdateSet &>(j == 0 ? upt->update() : upt->other_updates(j - 1));
                        Expr * expr = uset.mutable_expr();

                        if (rg.NextSmallNumber() < 9)
                        {
                            //set constant value
                            LiteralValue * lv = expr->mutable_lit_val();

                            buf.resize(0);
                            if (entry.special == ColumnSpecial::SIGN)
                            {
                                buf += rg.NextBool() ? "1" : "-1";
                            }
                            else if (entry.special == ColumnSpecial::IS_DELETED)
                            {
                                buf += rg.NextBool() ? "1" : "0";
                            }
                            else
                            {
                                StrAppendAnyValue(rg, buf, entry.tp);
                            }
                            lv->set_no_quote_str(buf);
                        }
                        else
                        {
                            GenerateExpression(rg, expr);
                        }
                    }
                    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;
                    this->levels.clear();
                    this->entries.clear();
                }

                GenerateUptDelWhere(rg, t, upt->mutable_where()->mutable_expr()->mutable_expr());
            }
            else if (
                add_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + 1))
            {
                AddStatistics * ads = ati->mutable_add_stats();

                PickUpNextCols(rg, t, ads->mutable_cols());
                GenerateNextStatistics(rg, ads->mutable_stats());
            }
            else if (
                mod_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + 1))
            {
                AddStatistics * ads = ati->mutable_mod_stats();

                PickUpNextCols(rg, t, ads->mutable_cols());
                GenerateNextStatistics(rg, ads->mutable_stats());
            }
            else if (
                drop_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + 1))
            {
                PickUpNextCols(rg, t, ati->mutable_drop_stats());
            }
            else if (
                clear_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + 1))
            {
                PickUpNextCols(rg, t, ati->mutable_clear_stats());
            }
            else if (
                mat_stats
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + 1))
            {
                PickUpNextCols(rg, t, ati->mutable_mat_stats());
            }
            else if (
                add_idx
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + 1))
            {
                AddIndex * add_index = ati->mutable_add_index();

                AddTableIndex(rg, t, true, add_index->mutable_new_idx());
                if (!t.idxs.empty())
                {
                    const uint32_t next_option = rg.NextSmallNumber();

                    if (next_option < 4)
                    {
                        add_index->mutable_add_where()->mutable_idx()->set_index("i" + std::to_string(rg.PickKeyRandomlyFromMap(t.idxs)));
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
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + 1))
            {
                IdxInPartition * iip = ati->mutable_materialize_index();

                iip->mutable_idx()->set_index("i" + std::to_string(rg.PickKeyRandomlyFromMap(t.idxs)));
                if (t.IsMergeTreeFamily() && rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, iip->mutable_partition());
                }
            }
            else if (
                clear_idx
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + 1))
            {
                IdxInPartition * iip = ati->mutable_clear_index();

                iip->mutable_idx()->set_index("i" + std::to_string(rg.PickKeyRandomlyFromMap(t.idxs)));
                if (t.IsMergeTreeFamily() && rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, iip->mutable_partition());
                }
            }
            else if (
                drop_idx
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + 1))
            {
                ati->mutable_drop_index()->set_index("i" + std::to_string(rg.PickKeyRandomlyFromMap(t.idxs)));
            }
            else if (
                column_remove_property
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + 1))
            {
                RemoveColumnProperty * rcs = ati->mutable_column_remove_property();

                rcs->mutable_col()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
                rcs->set_property(static_cast<RemoveColumnProperty_ColumnProperties>(
                    (rg.NextRandomUInt32() % static_cast<uint32_t>(RemoveColumnProperty::ColumnProperties_MAX)) + 1));
            }
            else if (
                column_modify_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + 1))
            {
                ModifyColumnSetting * mcp = ati->mutable_column_modify_setting();
                const auto & csettings = AllColumnSettings.at(t.teng);

                mcp->mutable_col()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
                GenerateSettingValues(rg, csettings, mcp->mutable_settings());
            }
            else if (
                column_remove_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + 1))
            {
                RemoveColumnSetting * rcp = ati->mutable_column_remove_setting();
                const auto & csettings = AllColumnSettings.at(t.teng);

                rcp->mutable_col()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
                GenerateSettingList(rg, csettings, rcp->mutable_settings());
            }
            else if (
                table_modify_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + 1))
            {
                const auto & tsettings = AllTableSettings.at(t.teng);

                GenerateSettingValues(rg, tsettings, ati->mutable_table_modify_setting());
            }
            else if (
                table_remove_setting
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + 1))
            {
                const auto & tsettings = AllTableSettings.at(t.teng);

                GenerateSettingList(rg, tsettings, ati->mutable_table_remove_setting());
            }
            else if (
                add_projection
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + 1))
            {
                AddTableProjection(rg, t, true, ati->mutable_add_projection());
            }
            else if (
                remove_projection
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + 1))
            {
                ati->mutable_remove_projection()->set_projection("p" + std::to_string(rg.PickRandomlyFromSet(t.projs)));
            }
            else if (
                materialize_projection
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection + 1))
            {
                ProjectionInPartition * pip = ati->mutable_materialize_projection();

                pip->mutable_proj()->set_projection("p" + std::to_string(rg.PickRandomlyFromSet(t.projs)));
                if (t.IsMergeTreeFamily() && rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, pip->mutable_partition());
                }
            }
            else if (
                clear_projection
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + 1))
            {
                ProjectionInPartition * pip = ati->mutable_clear_projection();

                pip->mutable_proj()->set_projection("p" + std::to_string(rg.PickRandomlyFromSet(t.projs)));
                if (t.IsMergeTreeFamily() && rg.NextBool())
                {
                    GenerateNextTablePartition<false>(rg, t, pip->mutable_partition());
                }
            }
            else if (
                add_constraint
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + 1))
            {
                AddTableConstraint(rg, t, true, ati->mutable_add_constraint());
            }
            else if (
                remove_constraint
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + 1))
            {
                ati->mutable_remove_constraint()->set_constraint("c" + std::to_string(rg.PickRandomlyFromSet(t.constrs)));
            }
            else if (
                detach_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + 1))
            {
                const uint32_t nopt2 = rg.NextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_detach_partition();

                if (table_has_partitions && nopt2 < 5)
                {
                    fc.TableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                else if (table_has_partitions && nopt2 < 9)
                {
                    fc.TableGetRandomPartitionOrPart<false, false>(dname, tname, buf);
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
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + 1))
            {
                const uint32_t nopt2 = rg.NextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_drop_partition();

                if (table_has_partitions && nopt2 < 5)
                {
                    fc.TableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                else if (table_has_partitions && nopt2 < 9)
                {
                    fc.TableGetRandomPartitionOrPart<false, false>(dname, tname, buf);
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
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_detached_partition + 1))
            {
                const uint32_t nopt2 = rg.NextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_drop_detached_partition();
                const bool table_has_detached_partitions = fc.TableHasPartitions<true>(dname, tname);

                if (table_has_detached_partitions && nopt2 < 5)
                {
                    fc.TableGetRandomPartitionOrPart<true, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                else if (table_has_detached_partitions && nopt2 < 9)
                {
                    fc.TableGetRandomPartitionOrPart<true, false>(dname, tname, buf);
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
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + drop_detached_partition
                       + forget_partition + 1))
            {
                PartitionExpr * pexpr = ati->mutable_forget_partition();

                fc.TableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                pexpr->set_partition_id(buf);
            }
            else if (
                attach_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + drop_detached_partition
                       + forget_partition + attach_partition + 1))
            {
                const uint32_t nopt2 = rg.NextSmallNumber();
                PartitionExpr * pexpr = ati->mutable_attach_partition();
                const bool table_has_detached_partitions = fc.TableHasPartitions<true>(dname, tname);

                if (table_has_detached_partitions && nopt2 < 5)
                {
                    fc.TableGetRandomPartitionOrPart<true, true>(dname, tname, buf);
                    pexpr->set_partition_id(buf);
                }
                else if (table_has_detached_partitions && nopt2 < 9)
                {
                    fc.TableGetRandomPartitionOrPart<true, false>(dname, tname, buf);
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
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + drop_detached_partition
                       + forget_partition + attach_partition + move_partition_to + 1))
            {
                AttachPartitionFrom * apf = ati->mutable_move_partition_to();
                PartitionExpr * pexpr = apf->mutable_partition();
                ExprSchemaTable * est2 = apf->mutable_est();
                const SQLTable & t2 = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

                fc.TableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
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
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + drop_detached_partition
                       + forget_partition + attach_partition + move_partition_to + clear_column_partition + 1))
            {
                ClearColumnInPartition * ccip = ati->mutable_clear_column_partition();
                PartitionExpr * pexpr = ccip->mutable_partition();

                fc.TableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                pexpr->set_partition_id(buf);
                ccip->mutable_col()->set_column("c" + std::to_string(rg.PickKeyRandomlyFromMap(t.cols)));
            }
            else if (
                freeze_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + drop_detached_partition
                       + forget_partition + attach_partition + move_partition_to + clear_column_partition + freeze_partition + 1))
            {
                FreezePartition * fp = ati->mutable_freeze_partition();

                if (table_has_partitions && rg.NextSmallNumber() < 9)
                {
                    fc.TableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                    fp->mutable_partition()->set_partition_id(buf);
                }
                fp->set_fname(t.freeze_counter++);
            }
            else if (
                unfreeze_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + drop_detached_partition
                       + forget_partition + attach_partition + move_partition_to + clear_column_partition + freeze_partition
                       + unfreeze_partition + 1))
            {
                FreezePartition * fp = ati->mutable_unfreeze_partition();
                const uint32_t fname = rg.PickKeyRandomlyFromMap(t.frozen_partitions);
                const std::string & partition_id = t.frozen_partitions[fname];

                if (partition_id != "")
                {
                    fp->mutable_partition()->set_partition_id(partition_id);
                }
                fp->set_fname(fname);
            }
            else if (
                clear_index_partition
                && nopt
                    < (heavy_delete + alter_order_by + add_column + materialize_column + drop_column + rename_column + clear_column
                       + modify_column + delete_mask + heavy_update + add_stats + mod_stats + drop_stats + clear_stats + mat_stats + add_idx
                       + materialize_idx + clear_idx + drop_idx + column_remove_property + column_modify_setting + column_remove_setting
                       + table_modify_setting + table_remove_setting + add_projection + remove_projection + materialize_projection
                       + clear_projection + add_constraint + remove_constraint + detach_partition + drop_partition + drop_detached_partition
                       + forget_partition + attach_partition + move_partition_to + clear_column_partition + freeze_partition
                       + unfreeze_partition + clear_index_partition + 1))
            {
                ClearIndexInPartition * ccip = ati->mutable_clear_index_partition();
                PartitionExpr * pexpr = ccip->mutable_partition();

                fc.TableGetRandomPartitionOrPart<false, true>(dname, tname, buf);
                pexpr->set_partition_id(buf);
                ccip->mutable_idx()->set_index("i" + std::to_string(rg.PickKeyRandomlyFromMap(t.idxs)));
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
    return 0;
}

int StatementGenerator::GenerateAttach(RandomGenerator & rg, Attach * att)
{
    SQLObjectName * sot = att->mutable_object();
    const uint32_t attach_table = 10 * static_cast<uint32_t>(CollectionHas<SQLTable>(detached_tables)),
                   attach_view = 10 * static_cast<uint32_t>(CollectionHas<SQLView>(detached_views)),
                   attach_database = 2 * static_cast<uint32_t>(CollectionHas<std::shared_ptr<SQLDatabase>>(detached_databases)),
                   prob_space = attach_table + attach_view + attach_database;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.gen);

    if (attach_table && nopt < (attach_table + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(detached_tables));

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
        const SQLView & v = rg.PickRandomlyFromVector(FilterCollection<SQLView>(detached_views));

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
            = rg.PickRandomlyFromVector(FilterCollection<std::shared_ptr<SQLDatabase>>(detached_databases));

        att->set_sobject(SQLObject::DATABASE);
        sot->mutable_database()->set_database("d" + std::to_string(d->dname));
    }
    else
    {
        assert(0);
    }
    return 0;
}

int StatementGenerator::GenerateDetach(RandomGenerator & rg, Detach * det)
{
    SQLObjectName * sot = det->mutable_object();
    const uint32_t detach_table = 10 * static_cast<uint32_t>(CollectionHas<SQLTable>(attached_tables)),
                   detach_view = 10 * static_cast<uint32_t>(CollectionHas<SQLView>(attached_views)),
                   detach_database = 2 * static_cast<uint32_t>(CollectionHas<std::shared_ptr<SQLDatabase>>(attached_databases)),
                   prob_space = detach_table + detach_view + detach_database;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.gen);

    if (detach_table && nopt < (detach_table + 1))
    {
        ExprSchemaTable * est = sot->mutable_est();
        const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

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
        const SQLView & v = rg.PickRandomlyFromVector(FilterCollection<SQLView>(attached_views));

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
            = rg.PickRandomlyFromVector(FilterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

        det->set_sobject(SQLObject::DATABASE);
        sot->mutable_database()->set_database("d" + std::to_string(d->dname));
    }
    else
    {
        assert(0);
    }
    det->set_permanently(!detach_database && rg.NextSmallNumber() < 4);
    det->set_sync(rg.NextSmallNumber() < 4);
    return 0;
}

int StatementGenerator::GenerateNextQuery(RandomGenerator & rg, SQLQueryInner * sq)
{
    const uint32_t create_table = 6
        * static_cast<uint32_t>(CollectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && tables.size() < this->fc.max_tables),
                   create_view
        = 10 * static_cast<uint32_t>(CollectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && views.size() < this->fc.max_views),
                   drop = 1
        * static_cast<uint32_t>(
                              CollectionHas<SQLTable>(attached_tables) || CollectionHas<SQLView>(attached_views)
                              || CollectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) || !functions.empty()),
                   insert = 100 * static_cast<uint32_t>(CollectionHas<SQLTable>(attached_tables)),
                   light_delete = 6 * static_cast<uint32_t>(CollectionHas<SQLTable>(attached_tables)),
                   truncate = 2
        * static_cast<uint32_t>(CollectionHas<std::shared_ptr<SQLDatabase>>(attached_databases)
                                || CollectionHas<SQLTable>(attached_tables)),
                   optimize_table = 2
        * static_cast<uint32_t>(CollectionHas<SQLTable>(
            [](const SQLTable & t)
            {
                return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED && t.IsMergeTreeFamily();
            })),
                   check_table = 2 * static_cast<uint32_t>(CollectionHas<SQLTable>(attached_tables)),
                   desc_table
        = 2 * static_cast<uint32_t>(CollectionHas<SQLTable>(attached_tables) || CollectionHas<SQLView>(attached_views)),
                   exchange_tables = 1 * static_cast<uint32_t>(CollectionCount<SQLTable>(attached_tables) > 1),
                   alter_table = 6
        * static_cast<uint32_t>(CollectionHas<SQLTable>(
                                    [](const SQLTable & t)
                                    {
                                        return (!t.db || t.db->attached == DetachStatus::ATTACHED) && t.attached == DetachStatus::ATTACHED
                                            && !t.IsFileEngine();
                                    })
                                || CollectionHas<SQLView>(attached_views)),
                   set_values = 5,
                   attach = 2
        * static_cast<uint32_t>(CollectionHas<SQLTable>(detached_tables) || CollectionHas<SQLView>(detached_views)
                                || CollectionHas<std::shared_ptr<SQLDatabase>>(detached_databases)),
                   detach = 2
        * static_cast<uint32_t>(CollectionHas<SQLTable>(attached_tables) || CollectionHas<SQLView>(attached_views)
                                || CollectionHas<std::shared_ptr<SQLDatabase>>(attached_databases)),
                   create_database = 2 * static_cast<uint32_t>(databases.size() < this->fc.max_databases),
                   create_function = 5 * static_cast<uint32_t>(functions.size() < this->fc.max_functions), select_query = 350,
                   prob_space = create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table
        + desc_table + exchange_tables + alter_table + set_values + attach + detach + create_database + create_function + select_query;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.gen);

    assert(this->ids.empty());
    if (create_table && nopt < (create_table + 1))
    {
        return GenerateNextCreateTable(rg, sq->mutable_create_table());
    }
    else if (create_view && nopt < (create_table + create_view + 1))
    {
        return GenerateNextCreateView(rg, sq->mutable_create_view());
    }
    else if (drop && nopt < (create_table + create_view + drop + 1))
    {
        return GenerateNextDrop(rg, sq->mutable_drop());
    }
    else if (insert && nopt < (create_table + create_view + drop + insert + 1))
    {
        return GenerateNextInsert(rg, sq->mutable_insert());
    }
    else if (light_delete && nopt < (create_table + create_view + drop + insert + light_delete + 1))
    {
        return GenerateNextDelete(rg, sq->mutable_del());
    }
    else if (truncate && nopt < (create_table + create_view + drop + insert + light_delete + truncate + 1))
    {
        return GenerateNextTruncate(rg, sq->mutable_trunc());
    }
    else if (optimize_table && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + 1))
    {
        return GenerateNextOptimizeTable(rg, sq->mutable_opt());
    }
    else if (
        check_table && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + 1))
    {
        return GenerateNextCheckTable(rg, sq->mutable_check());
    }
    else if (
        desc_table
        && nopt < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table + 1))
    {
        return GenerateNextDescTable(rg, sq->mutable_desc());
    }
    else if (
        exchange_tables
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + 1))
    {
        return GenerateNextExchangeTables(rg, sq->mutable_exchange());
    }
    else if (
        alter_table
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + 1))
    {
        return GenerateAlterTable(rg, sq->mutable_alter_table());
    }
    else if (
        set_values
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + 1))
    {
        return GenerateSettingValues(rg, ServerSettings, sq->mutable_setting_values());
    }
    else if (
        attach
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + 1))
    {
        return GenerateAttach(rg, sq->mutable_attach());
    }
    else if (
        detach
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + detach + 1))
    {
        return GenerateDetach(rg, sq->mutable_detach());
    }
    else if (
        create_database
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + detach + create_database + 1))
    {
        return GenerateNextCreateDatabase(rg, sq->mutable_create_database());
    }
    else if (
        create_function
        && nopt
            < (create_table + create_view + drop + insert + light_delete + truncate + optimize_table + check_table + desc_table
               + exchange_tables + alter_table + set_values + attach + detach + create_database + create_function + 1))
    {
        return GenerateNextCreateFunction(rg, sq->mutable_create_function());
    }
    return GenerateTopSelect(rg, std::numeric_limits<uint32_t>::max(), sq->mutable_select());
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

int StatementGenerator::GenerateNextExplain(RandomGenerator & rg, ExplainQuery * eq)
{
    if (rg.NextSmallNumber() < 10)
    {
        ExplainQuery_ExplainValues val
            = static_cast<ExplainQuery_ExplainValues>((rg.NextRandomUInt32() % static_cast<uint32_t>(ExplainQuery::ExplainValues_MAX)) + 1);

        if (rg.NextBool())
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
                const size_t noptions = (static_cast<size_t>(rg.NextMediumNumber()) % this->ids.size()) + 1;
                std::shuffle(ids.begin(), ids.end(), rg.gen);

                for (size_t i = 0; i < noptions; i++)
                {
                    const uint32_t nopt = this->ids[i];
                    ExplainOption * eopt = eq->add_opts();

                    eopt->set_opt(nopt - offset);
                    eopt->set_val(std::stoi(rg.PickRandomlyFromSet(explain_settings[nopt].options)));
                }
                this->ids.clear();
            }
        }
        eq->set_expl(val);
    }
    return GenerateNextQuery(rg, eq->mutable_inner_query());
}

int StatementGenerator::GenerateNextStatement(RandomGenerator & rg, SQLQuery & sq)
{
    const uint32_t start_transaction = 2 * static_cast<uint32_t>(supports_cloud_features && !this->in_transaction),
                   commit = 50 * static_cast<uint32_t>(supports_cloud_features && this->in_transaction), explain_query = 10,
                   run_query = 120, prob_space = start_transaction + commit + explain_query + run_query;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.gen);

    if (start_transaction && nopt < (start_transaction + 1))
    {
        sq.set_start_trans(true);
        return 0;
    }
    else if (commit && nopt < (start_transaction + commit + 1))
    {
        if (rg.NextSmallNumber() < 7)
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
        return GenerateNextExplain(rg, sq.mutable_explain());
    }
    else if (run_query)
    {
        return GenerateNextQuery(rg, sq.mutable_inner_query());
    }
    else
    {
        assert(0);
        return 0;
    }
}

void StatementGenerator::DropDatabase(const uint32_t dname)
{
    for (auto it = this->tables.cbegin(); it != this->tables.cend();)
    {
        if (it->second.db && it->second.db->dname == dname)
        {
            this->tables.erase(it++);
        }
        else
        {
            ++it;
        }
    }
    for (auto it = this->views.cbegin(); it != this->views.cend();)
    {
        if (it->second.db && it->second.db->dname == dname)
        {
            this->views.erase(it++);
        }
        else
        {
            ++it;
        }
    }
    this->databases.erase(dname);
}

void StatementGenerator::UpdateGenerator(const SQLQuery & sq, ExternalIntegrations & ei, bool success)
{
    const SQLQueryInner & query = sq.has_inner_query() ? sq.inner_query() : sq.explain().inner_query();

    success &= (!ei.GetRequiresExternalCallCheck() || ei.GetNextExternalCallSucceeded());

    if (sq.has_inner_query() && query.has_create_table())
    {
        const uint32_t tname = static_cast<uint32_t>(std::stoul(query.create_table().est().table().table().substr(1)));

        if (success)
        {
            if (query.create_table().replace())
            {
                this->tables.erase(tname);
            }
            this->tables[tname] = std::move(this->staged_tables[tname]);
        }
        this->staged_tables.erase(tname);
    }
    else if (sq.has_inner_query() && query.has_create_view())
    {
        const uint32_t tname = static_cast<uint32_t>(std::stoul(query.create_view().est().table().table().substr(1)));

        if (success)
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
            this->tables.erase(static_cast<uint32_t>(std::stoul(drp.object().est().table().table().substr(1))));
        }
        else if (drp.sobject() == SQLObject::VIEW)
        {
            this->views.erase(static_cast<uint32_t>(std::stoul(drp.object().est().table().table().substr(1))));
        }
        else if (drp.sobject() == SQLObject::DATABASE)
        {
            DropDatabase(std::stoul(drp.object().database().database().substr(1)));
        }
        else if (drp.sobject() == SQLObject::FUNCTION)
        {
            this->functions.erase(static_cast<uint32_t>(std::stoul(drp.object().function().function().substr(1))));
        }
    }
    else if (sq.has_inner_query() && query.has_exchange() && success)
    {
        const uint32_t tname1 = static_cast<uint32_t>(std::stoul(query.exchange().est1().table().table().substr(1))),
                       tname2 = static_cast<uint32_t>(std::stoul(query.exchange().est2().table().table().substr(1)));
        SQLTable tx = std::move(this->tables[tname1]), ty = std::move(this->tables[tname2]);

        tx.tname = tname2;
        ty.tname = tname1;
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
                    const uint32_t cname = static_cast<uint32_t>(std::stoul(ati.drop_column().column().substr(1)));

                    t.cols.erase(cname);
                }
                else if (ati.has_rename_column() && success)
                {
                    const uint32_t old_cname = static_cast<uint32_t>(std::stoul(ati.rename_column().old_name().column().substr(1))),
                                   new_cname = static_cast<uint32_t>(std::stoul(ati.rename_column().new_name().column().substr(1)));

                    t.cols[new_cname] = std::move(t.cols[old_cname]);
                    t.cols[new_cname].cname = new_cname;
                    t.cols.erase(old_cname);
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
                else if (
                    success && ati.has_column_remove_property()
                    && ati.column_remove_property().property() <= RemoveColumnProperty_ColumnProperties_MATERIALIZED)
                {
                    const uint32_t cname = static_cast<uint32_t>(std::stoul(ati.column_remove_property().col().column().substr(1)));

                    t.cols[cname].dmod = std::nullopt;
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
        const bool istable = att.object().has_est() && att.object().est().table().table()[0] == 't',
                   isview = att.object().has_est() && att.object().est().table().table()[0] == 'v',
                   isdatabase = att.object().has_database();

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
            for (auto it = this->tables.begin(); it != this->tables.end(); ++it)
            {
                if (it->second.db && it->second.db->dname == dname)
                {
                    it->second.attached = std::max(it->second.attached, DetachStatus::DETACHED);
                }
            }
        }
    }
    else if (sq.has_inner_query() && query.has_detach() && success)
    {
        const Detach & det = sq.inner_query().detach();
        const bool istable = det.object().has_est() && det.object().est().table().table()[0] == 't',
                   isview = det.object().has_est() && det.object().est().table().table()[0] == 'v',
                   isdatabase = det.object().has_database(), is_permanent = det.permanently();

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
            for (auto it = this->tables.begin(); it != this->tables.end(); ++it)
            {
                if (it->second.db && it->second.db->dname == dname)
                {
                    it->second.attached = std::max(it->second.attached, DetachStatus::DETACHED);
                }
            }
        }
    }
    else if (sq.has_inner_query() && query.has_create_database())
    {
        const uint32_t dname = static_cast<uint32_t>(std::stoul(query.create_database().database().database().substr(1)));

        if (success)
        {
            this->databases[dname] = std::move(this->staged_databases[dname]);
        }
        this->staged_databases.erase(dname);
    }
    else if (sq.has_inner_query() && query.has_create_function())
    {
        const uint32_t fname = static_cast<uint32_t>(std::stoul(query.create_function().function().function().substr(1)));

        if (success)
        {
            this->functions[fname] = std::move(this->staged_functions[fname]);
        }
        this->staged_functions.erase(fname);
    }
    else if (sq.has_inner_query() && query.has_trunc() && query.trunc().has_database())
    {
        DropDatabase(std::stoul(query.trunc().database().database().substr(1)));
    }
    else if (sq.has_start_trans() && success)
    {
        this->in_transaction = true;
    }
    else if ((sq.has_commit_trans() || sq.has_rollback_trans()) && success)
    {
        this->in_transaction = false;
    }

    ei.ResetExternalStatus();
}

}
