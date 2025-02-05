#include <cstdint>

#include <Client/BuzzHouse/Generator/SQLCatalog.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>
#include <Client/BuzzHouse/Generator/SystemTables.h>

namespace BuzzHouse
{

void collectColumnPaths(
    const std::string cname, SQLType * tp, const uint32_t flags, ColumnPathChain & next, std::vector<ColumnPathChain> & paths)
{
    ArrayType * at = nullptr;
    MapType * mt = nullptr;
    TupleType * ttp = nullptr;
    NestedType * ntp = nullptr;
    JSONType * jt = nullptr;

    checkStackSize();
    // Append this node to the path
    next.path.push_back(ColumnPathChainEntry(cname, tp));
    if (((flags & skip_nested_node) == 0 || !dynamic_cast<NestedType *>(tp))
        && ((flags & skip_tuple_node) == 0 || !dynamic_cast<TupleType *>(tp)))
    {
        paths.push_back(next);
    }
    if ((flags & collect_generated) != 0 && dynamic_cast<Nullable *>(tp))
    {
        next.path.push_back(ColumnPathChainEntry("null", null_tp));
        paths.push_back(next);
        next.path.pop_back();
    }
    else if ((flags & collect_generated) != 0 && ((at = dynamic_cast<ArrayType *>(tp)) || (mt = dynamic_cast<MapType *>(tp))))
    {
        uint32_t i = 1;

        next.path.push_back(ColumnPathChainEntry("size0", size_tp));
        paths.push_back(next);
        next.path.pop_back();
        while (at && (at = dynamic_cast<ArrayType *>(at->subtype)))
        {
            next.path.push_back(ColumnPathChainEntry("size" + std::to_string(i), size_tp));
            paths.push_back(next);
            next.path.pop_back();
            i++;
        }
        if (mt)
        {
            next.path.push_back(ColumnPathChainEntry("keys", mt->key));
            paths.push_back(next);
            next.path.pop_back();
            next.path.push_back(ColumnPathChainEntry("values", mt->value));
            paths.push_back(next);
            next.path.pop_back();
        }
    }
    else if ((flags & flat_tuple) != 0 && (ttp = dynamic_cast<TupleType *>(tp)))
    {
        uint32_t i = 1;

        for (const auto & entry : ttp->subtypes)
        {
            collectColumnPaths(
                entry.cname.has_value() ? ("c" + std::to_string(entry.cname.value())) : std::to_string(i),
                entry.subtype,
                flags,
                next,
                paths);
            i++;
        }
    }
    else if ((flags & flat_nested) != 0 && (ntp = dynamic_cast<NestedType *>(tp)))
    {
        for (const auto & entry : ntp->subtypes)
        {
            collectColumnPaths("c" + std::to_string(entry.cname), entry.subtype, flags, next, paths);
        }
    }
    else if ((flags & flat_json) != 0 && (jt = dynamic_cast<JSONType *>(tp)))
    {
        for (const auto & entry : jt->subcols)
        {
            next.path.push_back(ColumnPathChainEntry(entry.cname, entry.subtype));
            paths.push_back(next);
            next.path.pop_back();
        }
    }
    // Remove the last element from the path
    next.path.pop_back();
}

void StatementGenerator::flatTableColumnPath(const uint32_t flags, const SQLTable & t, std::function<bool(const SQLColumn & c)> col_filter)
{
    auto & res = ((flags & to_table_entries) != 0) ? this->table_entries
                                                   : (((flags & to_remote_entries) != 0) ? this->remote_entries : this->entries);

    assert(res.empty());
    for (const auto & entry : t.cols)
    {
        if (col_filter(entry.second))
        {
            ColumnPathChain cpc(entry.second.nullable, entry.second.special, entry.second.dmod, {});

            collectColumnPaths("c" + std::to_string(entry.first), entry.second.tp, flags, cpc, res);
        }
    }
}

void StatementGenerator::addTableRelation(
    RandomGenerator & rg, const bool allow_internal_cols, const std::string & rel_name, const SQLTable & t)
{
    SQLRelation rel(rel_name);

    flatTableColumnPath(
        flat_tuple | flat_nested | flat_json | to_table_entries | collect_generated,
        t,
        [](const SQLColumn & c) { return !c.dmod.has_value() || c.dmod.value() != DModifier::DEF_EPHEMERAL; });
    for (const auto & entry : this->table_entries)
    {
        std::vector<std::string> names;

        names.reserve(entry.path.size());
        for (const auto & path : entry.path)
        {
            names.push_back(path.cname);
        }
        rel.cols.push_back(SQLRelationCol(rel_name, std::move(names)));
    }
    this->table_entries.clear();
    if (allow_internal_cols && rg.nextSmallNumber() < 3)
    {
        if (t.isMergeTreeFamily())
        {
            if (this->allow_not_deterministic)
            {
                rel.cols.push_back(SQLRelationCol(rel_name, {"_block_number"}));
                rel.cols.push_back(SQLRelationCol(rel_name, {"_block_offset"}));
            }
            rel.cols.push_back(SQLRelationCol(rel_name, {"_part"}));
            rel.cols.push_back(SQLRelationCol(rel_name, {"_part_data_version"}));
            rel.cols.push_back(SQLRelationCol(rel_name, {"_part_index"}));
            rel.cols.push_back(SQLRelationCol(rel_name, {"_part_offset"}));
            rel.cols.push_back(SQLRelationCol(rel_name, {"_part_uuid"}));
            rel.cols.push_back(SQLRelationCol(rel_name, {"_partition_id"}));
            rel.cols.push_back(SQLRelationCol(rel_name, {"_partition_value"}));
            rel.cols.push_back(SQLRelationCol(rel_name, {"_sample_factor"}));
        }
        else if (t.isAnyS3Engine() || t.isFileEngine())
        {
            rel.cols.push_back(SQLRelationCol(rel_name, {"_path"}));
            rel.cols.push_back(SQLRelationCol(rel_name, {"_file"}));
            if (t.isS3Engine())
            {
                rel.cols.push_back(SQLRelationCol(rel_name, {"_size"}));
                rel.cols.push_back(SQLRelationCol(rel_name, {"_time"}));
                rel.cols.push_back(SQLRelationCol(rel_name, {"_etag"}));
            }
        }
        else if (t.isMergeEngine())
        {
            rel.cols.push_back(SQLRelationCol(rel_name, {"_table"}));
        }
    }
    if (rel_name.empty())
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);
    }
    this->levels[this->current_level].rels.push_back(std::move(rel));
}

int StatementGenerator::generateNextStatistics(RandomGenerator & rg, ColumnStatistics * cstats)
{
    const size_t nstats = (rg.nextMediumNumber() % static_cast<uint32_t>(ColumnStat_MAX)) + 1;

    for (uint32_t i = 1; i <= ColumnStat_MAX; i++)
    {
        ids.push_back(i);
    }
    std::shuffle(ids.begin(), ids.end(), rg.generator);
    for (size_t i = 0; i < nstats; i++)
    {
        const ColumnStat nstat = static_cast<ColumnStat>(ids[i]);

        if (i == 0)
        {
            cstats->set_stat(nstat);
        }
        else
        {
            cstats->add_other_stats(nstat);
        }
    }
    ids.clear();
    return 0;
}

int StatementGenerator::generateNextCodecs(RandomGenerator & rg, CodecList * cl)
{
    const uint32_t ncodecs = (rg.nextMediumNumber() % UINT32_C(3)) + 1;

    for (uint32_t i = 0; i < ncodecs; i++)
    {
        CodecParam * cp = i == 0 ? cl->mutable_codec() : cl->add_other_codecs();
        const CompressionCodec cc
            = static_cast<CompressionCodec>((rg.nextRandomUInt32() % static_cast<uint32_t>(CompressionCodec_MAX)) + 1);

        cp->set_codec(cc);
        switch (cc)
        {
            case COMP_LZ4HC:
            case COMP_ZSTD_QAT:
                if (rg.nextBool())
                {
                    std::uniform_int_distribution<uint32_t> next_dist(1, 12);
                    cp->add_params(next_dist(rg.generator));
                }
                break;
            case COMP_ZSTD:
                if (rg.nextBool())
                {
                    std::uniform_int_distribution<uint32_t> next_dist(1, 22);
                    cp->add_params(next_dist(rg.generator));
                }
                break;
            case COMP_Delta:
            case COMP_DoubleDelta:
            case COMP_Gorilla:
                if (rg.nextBool())
                {
                    std::uniform_int_distribution<uint32_t> next_dist(0, 3);
                    cp->add_params(UINT32_C(1) << next_dist(rg.generator));
                }
                break;
            case COMP_FPC:
                if (rg.nextBool())
                {
                    std::uniform_int_distribution<uint32_t> next_dist1(1, 28);
                    cp->add_params(next_dist1(rg.generator));
                    cp->add_params(rg.nextBool() ? 4 : 9);
                }
                break;
            default:
                break;
        }
    }
    return 0;
}

int StatementGenerator::generateTTLExpression(RandomGenerator & rg, const std::optional<SQLTable> & t, Expr * ttl_expr)
{
    assert(filtered_entries.empty());
    for (const auto & entry : this->entries)
    {
        SQLType * tp = entry.getBottomType();

        if (!tp || (tp && (dynamic_cast<DateTimeType *>(tp) || dynamic_cast<DateType *>(tp))))
        {
            filtered_entries.push_back(std::ref<const ColumnPathChain>(entry));
        }
    }
    if (!filtered_entries.empty() && rg.nextMediumNumber() < 96)
    {
        BinaryExpr * bexpr = ttl_expr->mutable_comp_expr()->mutable_binary_expr();
        IntervalExpr * ie = bexpr->mutable_rhs()->mutable_comp_expr()->mutable_interval();
        IntLiteral * il = ie->mutable_expr()->mutable_lit_val()->mutable_int_lit();
        std::uniform_int_distribution<int64_t> next_dist(-100, 100);

        bexpr->set_op(rg.nextBool() ? BinaryOperator::BINOP_PLUS : BinaryOperator::BINOP_MINUS);
        columnPathRef(rg.pickRandomlyFromVector(filtered_entries).get(), bexpr->mutable_lhs());
        ie->set_interval(
            static_cast<IntervalExpr_Interval>((rg.nextRandomUInt32() % static_cast<uint32_t>(IntervalExpr_Interval_MINUTE)) + 1));
        il->set_int_lit(next_dist(rg.generator));
        filtered_entries.clear();
    }
    else
    {
        filtered_entries.clear();
        if (t.has_value())
        {
            addTableRelation(rg, false, "", t.value());
        }
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs
            = this->allow_in_expression_alias = this->allow_subqueries = false;
        generateExpression(rg, ttl_expr);
        this->allow_in_expression_alias = this->allow_subqueries = true;
        this->levels.clear();
    }
    return 0;
}

int StatementGenerator::generateNextTTL(RandomGenerator & rg, const std::optional<SQLTable> & t, const TableEngine * te, TTLExpr * ttl_expr)
{
    const uint32_t nttls = (rg.nextLargeNumber() % 3) + 1;

    for (uint32_t i = 0; i < nttls; i++)
    {
        const uint32_t nopt = rg.nextSmallNumber();
        TTLEntry * entry = i == 0 ? ttl_expr->mutable_ttl_expr() : ttl_expr->add_other_ttl();

        generateTTLExpression(rg, t, entry->mutable_time_expr());
        if (nopt < 5)
        {
            TTLUpdate * tupt = entry->mutable_update();
            const uint32_t nopt2 = rg.nextSmallNumber();

            if (nopt2 < 5)
            {
                generateNextCodecs(rg, tupt->mutable_codecs());
            }
            else if (!fc.disks.empty() && nopt2 < 9)
            {
                generateStorage(rg, tupt->mutable_storage());
            }
            else
            {
                TTLDelete * tdel = tupt->mutable_del();

                if (rg.nextSmallNumber() < 4)
                {
                    if (t.has_value())
                    {
                        addTableRelation(rg, false, "", t.value());
                    }
                    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
                    generateWherePredicate(rg, tdel->mutable_where()->mutable_expr()->mutable_expr());
                    this->levels.clear();
                }
            }
        }
        else if (
            nopt < 9 && te && !entries.empty()
            && ((te->has_order() && te->order().exprs_size()) || (te->has_primary_key() && te->primary_key().exprs_size())))
        {
            TTLGroupBy * gb = entry->mutable_group_by();
            ExprList * el = gb->mutable_expr_list();
            const TableKey & tk = te->has_primary_key() && te->primary_key().exprs_size() ? te->primary_key() : te->order();
            std::uniform_int_distribution<uint32_t> table_key_dist(1, tk.exprs_size());
            const uint32_t ttl_group_size = table_key_dist(rg.generator);
            const size_t nset = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(3))) + 1;

            for (uint32_t j = 0; j < ttl_group_size; j++)
            {
                const TableKeyExpr & tke = tk.exprs(j);
                Expr * expr = j == 0 ? el->mutable_expr() : el->add_extra_exprs();

                expr->CopyFrom(tke.expr());
            }

            if (t.has_value())
            {
                addTableRelation(rg, false, "", t.value());
            }
            this->levels[this->current_level].global_aggregate = true;
            this->allow_in_expression_alias = this->allow_subqueries = false;
            std::shuffle(entries.begin(), entries.end(), rg.generator);

            for (size_t j = 0; j < nset; j++)
            {
                TTLSet * tset = j == 0 ? gb->mutable_ttl_set() : gb->add_other_ttl_set();

                columnPathRef(entries[j], tset->mutable_col());
                generateExpression(rg, tset->mutable_expr());
            }
            this->levels.clear();
            this->allow_in_expression_alias = this->allow_subqueries = true;
        }
    }
    return 0;
}

int StatementGenerator::pickUpNextCols(RandomGenerator & rg, const SQLTable & t, ColumnPathList * clist)
{
    flatTableColumnPath(flat_nested | skip_nested_node, t, [](const SQLColumn &) { return true; });
    const uint32_t ocols = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(4))) + 1;
    std::shuffle(entries.begin(), entries.end(), rg.generator);
    for (uint32_t i = 0; i < ocols; i++)
    {
        columnPathRef(this->entries[i], i == 0 ? clist->mutable_col() : clist->add_other_cols());
    }
    entries.clear();
    return 0;
}

const std::vector<SQLFunc> multicol_hash
    = {SQLFunc::FUNChalfMD5,
       SQLFunc::FUNCsipHash64,
       SQLFunc::FUNCsipHash128,
       SQLFunc::FUNCsipHash128Reference,
       SQLFunc::FUNCcityHash64,
       SQLFunc::FUNCfarmFingerprint64,
       SQLFunc::FUNCfarmHash64,
       SQLFunc::FUNCmetroHash64,
       SQLFunc::FUNCmurmurHash2_32,
       SQLFunc::FUNCmurmurHash2_64,
       SQLFunc::FUNCgccMurmurHash,
       SQLFunc::FUNCkafkaMurmurHash,
       SQLFunc::FUNCmurmurHash3_32,
       SQLFunc::FUNCmurmurHash3_64,
       SQLFunc::FUNCmurmurHash3_128};

const std::vector<SQLFunc> dates_hash
    = {SQLFunc::FUNCtoYear,
       SQLFunc::FUNCtoQuarter,
       SQLFunc::FUNCtoMonth,
       SQLFunc::FUNCtoDayOfYear,
       SQLFunc::FUNCtoDayOfMonth,
       SQLFunc::FUNCtoDayOfWeek,
       SQLFunc::FUNCtoHour,
       SQLFunc::FUNCtoMinute,
       SQLFunc::FUNCtoSecond,
       SQLFunc::FUNCtoMillisecond,
       SQLFunc::FUNCtoUnixTimestamp,
       SQLFunc::FUNCtoStartOfYear,
       SQLFunc::FUNCtoStartOfISOYear,
       SQLFunc::FUNCtoStartOfQuarter,
       SQLFunc::FUNCtoStartOfMonth,
       SQLFunc::FUNCtoLastDayOfMonth,
       SQLFunc::FUNCtoMonday,
       SQLFunc::FUNCtoStartOfWeek,
       SQLFunc::FUNCtoLastDayOfWeek,
       SQLFunc::FUNCtoStartOfDay,
       SQLFunc::FUNCtoStartOfHour,
       SQLFunc::FUNCtoStartOfMinute,
       SQLFunc::FUNCtoStartOfSecond,
       SQLFunc::FUNCtoStartOfMillisecond,
       SQLFunc::FUNCtoStartOfMicrosecond,
       SQLFunc::FUNCtoStartOfNanosecond,
       SQLFunc::FUNCtoStartOfFiveMinutes,
       SQLFunc::FUNCtoStartOfTenMinutes,
       SQLFunc::FUNCtoStartOfFifteenMinutes,
       SQLFunc::FUNCtoTime,
       SQLFunc::FUNCtoRelativeYearNum,
       SQLFunc::FUNCtoRelativeQuarterNum,
       SQLFunc::FUNCtoRelativeMonthNum,
       SQLFunc::FUNCtoRelativeWeekNum,
       SQLFunc::FUNCtoRelativeDayNum,
       SQLFunc::FUNCtoRelativeHourNum,
       SQLFunc::FUNCtoRelativeMinuteNum,
       SQLFunc::FUNCtoRelativeSecondNum,
       SQLFunc::FUNCtoISOYear,
       SQLFunc::FUNCtoISOWeek,
       SQLFunc::FUNCtoWeek,
       SQLFunc::FUNCtoYearWeek,
       SQLFunc::FUNCtoDaysSinceYearZero,
       SQLFunc::FUNCtoday,
       SQLFunc::FUNCyesterday,
       SQLFunc::FUNCtimeSlot,
       SQLFunc::FUNCtoYYYYMM,
       SQLFunc::FUNCtoYYYYMMDD,
       SQLFunc::FUNCtoYYYYMMDDhhmmss,
       SQLFunc::FUNCmonthName,
       SQLFunc::FUNCtoModifiedJulianDay,
       SQLFunc::FUNCtoModifiedJulianDayOrNull,
       SQLFunc::FUNCtoUTCTimestamp};

void StatementGenerator::columnPathRef(const ColumnPathChain & entry, Expr * expr) const
{
    columnPathRef(entry, expr->mutable_comp_expr()->mutable_expr_stc()->mutable_col()->mutable_path());
}

void StatementGenerator::columnPathRef(const ColumnPathChain & entry, ColumnPath * cp) const
{
    for (size_t i = 0; i < entry.path.size(); i++)
    {
        Column * col = i == 0 ? cp->mutable_col() : cp->add_sub_cols();

        col->set_column(entry.path[i].cname);
    }
}

int StatementGenerator::generateTableKey(RandomGenerator & rg, const TableEngineValues teng, const bool allow_asc_desc, TableKey * tkey)
{
    if (!entries.empty() && rg.nextSmallNumber() < 7)
    {
        const size_t ocols = (rg.nextMediumNumber() % std::min<size_t>(entries.size(), UINT32_C(3))) + 1;

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        if (teng != TableEngineValues::SummingMergeTree && rg.nextSmallNumber() < 3)
        {
            //Use a single expression for the entire table
            //See https://github.com/ClickHouse/ClickHouse/issues/72043 for SummingMergeTree exception
            TableKeyExpr * tke = tkey->add_exprs();
            Expr * expr = tke->mutable_expr();
            SQLFuncCall * func_call = expr->mutable_comp_expr()->mutable_func_call();

            func_call->mutable_func()->set_catalog_func(rg.pickRandomlyFromVector(multicol_hash));
            for (size_t i = 0; i < ocols; i++)
            {
                columnPathRef(this->entries[i], func_call->add_args()->mutable_expr());
            }
            if (allow_asc_desc && rg.nextSmallNumber() < 3)
            {
                tke->set_asc_desc(rg.nextBool() ? AscDesc::ASC : AscDesc::DESC);
            }
        }
        else
        {
            for (size_t i = 0; i < ocols; i++)
            {
                TableKeyExpr * tke = tkey->add_exprs();
                Expr * expr = tke->mutable_expr();
                const ColumnPathChain & entry = this->entries[i];
                SQLType * tp = entry.getBottomType();

                if ((hasType<DateType, false, true, false>(tp) || hasType<DateTimeType, false, true, false>(tp)) && rg.nextBool())
                {
                    //Use date functions for partitioning/keys
                    SQLFuncCall * func_call = expr->mutable_comp_expr()->mutable_func_call();

                    func_call->mutable_func()->set_catalog_func(rg.pickRandomlyFromVector(dates_hash));
                    columnPathRef(entry, func_call->add_args()->mutable_expr());
                }
                else if (hasType<IntType, true, true, false>(tp) && rg.nextBool())
                {
                    //Use modulo function for partitioning/keys
                    BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

                    columnPathRef(entry, bexpr->mutable_lhs());
                    bexpr->set_op(BinaryOperator::BINOP_PERCENT);
                    bexpr->mutable_rhs()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(
                        rg.nextRandomUInt32() % (rg.nextBool() ? 1024 : 65536));
                }
                else if (teng != TableEngineValues::SummingMergeTree && rg.nextMediumNumber() < 6)
                {
                    //Use hash
                    SQLFuncCall * func_call = expr->mutable_comp_expr()->mutable_func_call();

                    func_call->mutable_func()->set_catalog_func(rg.pickRandomlyFromVector(multicol_hash));
                    columnPathRef(entry, func_call->add_args()->mutable_expr());
                }
                else
                {
                    columnPathRef(entry, expr);
                }
                if (allow_asc_desc && rg.nextSmallNumber() < 3)
                {
                    tke->set_asc_desc(rg.nextBool() ? AscDesc::ASC : AscDesc::DESC);
                }
            }
        }
    }
    return 0;
}

template <typename T>
void StatementGenerator::setMergeTableParamter(RandomGenerator & rg, const char initial)
{
    const uint32_t noption = rg.nextSmallNumber();

    buf.resize(0);
    if constexpr (std::is_same_v<T, std::shared_ptr<SQLDatabase>>)
    {
        if (collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && noption < 4)
        {
            const std::shared_ptr<SQLDatabase> & d
                = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

            buf += initial;
            buf += std::to_string(d->dname);
            return;
        }
    }
    else
    {
        if (collectionHas<SQLTable>(attached_tables) && noption < 4)
        {
            const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

            buf += initial;
            buf += std::to_string(t.tname);
            return;
        }
    }
    if (noption < 7)
    {
        buf += initial;
        buf += std::to_string(rg.nextSmallNumber() - 1);
        buf += ".*";
    }
    else if (noption < 10)
    {
        const uint32_t first = rg.nextSmallNumber() - 1;
        const uint32_t second = std::max(rg.nextSmallNumber() - 1, first);

        buf += initial;
        buf += "[";
        buf += std::to_string(first);
        buf += "-";
        buf += std::to_string(second);
        buf += "].*";
    }
    else
    {
        if constexpr (std::is_same_v<T, std::shared_ptr<SQLDatabase>>)
        {
            buf += "default";
        }
        else
        {
            buf += "t0";
        }
    }
}

int StatementGenerator::generateMergeTreeEngineDetails(
    RandomGenerator & rg, const TableEngineValues teng, const PeerTableDatabase peer, const bool add_pkey, TableEngine * te)
{
    if (rg.nextSmallNumber() < 6)
    {
        generateTableKey(rg, teng, peer != PeerTableDatabase::ClickHouse, te->mutable_order());
    }
    if (te->has_order() && add_pkey && rg.nextSmallNumber() < 5)
    {
        //pkey is a subset of order by
        TableKey * tkey = te->mutable_primary_key();

        if (te->order().exprs_size())
        {
            std::uniform_int_distribution<uint32_t> table_order_by(1, te->order().exprs_size());
            const uint32_t pkey_size = table_order_by(rg.generator);

            for (uint32_t i = 0; i < pkey_size; i++)
            {
                const TableKeyExpr & tke = te->order().exprs(i);

                tkey->add_exprs()->mutable_expr()->CopyFrom(tke.expr());
            }
        }
    }
    else if (!te->has_order() && add_pkey)
    {
        generateTableKey(rg, teng, false, te->mutable_primary_key());
    }
    if (rg.nextBool())
    {
        generateTableKey(rg, teng, false, te->mutable_partition_by());
    }

    const int npkey = te->primary_key().exprs_size();
    if (npkey && rg.nextSmallNumber() < 5)
    {
        //try to add sample key
        assert(this->ids.empty());
        for (const auto & entry : this->entries)
        {
            IntType * itp = nullptr;
            SQLType * tp = entry.getBottomType();

            if ((itp = dynamic_cast<IntType *>(tp)) && itp->is_unsigned)
            {
                const TableKey & tpk = te->primary_key();

                //must be in pkey
                for (int j = 0; j < npkey; j++)
                {
                    if (tpk.exprs(j).expr().has_comp_expr() && tpk.exprs(j).expr().comp_expr().has_expr_stc()
                        && (tpk.exprs(j).expr().comp_expr().expr_stc().col().path().sub_cols_size() + 1)
                            == static_cast<int>(entry.path.size()))
                    {
                        bool ok = true;
                        const ExprColumn & oecol = tpk.exprs(j).expr().comp_expr().expr_stc().col();

                        for (uint32_t i = 0; i < oecol.path().sub_cols_size() + UINT32_C(1) && ok; i++)
                        {
                            const std::string & col
                                = i == 0 ? oecol.path().col().column() : oecol.path().sub_cols(i - UINT32_C(1)).column();

                            ok &= col == entry.path[i].cname;
                        }
                        if (ok)
                        {
                            this->filtered_entries.push_back(std::ref<const ColumnPathChain>(entry));
                            break;
                        }
                    }
                }
            }
        }
        if (!this->filtered_entries.empty())
        {
            TableKey * tkey = te->mutable_sample_by();
            const size_t ncols = (rg.nextMediumNumber() % std::min<size_t>(this->filtered_entries.size(), UINT32_C(3))) + 1;

            std::shuffle(this->filtered_entries.begin(), this->filtered_entries.end(), rg.generator);
            for (size_t i = 0; i < ncols; i++)
            {
                columnPathRef(this->filtered_entries[i].get(), tkey->add_exprs()->mutable_expr());
            }
            this->filtered_entries.clear();
        }
    }
    if (teng == TableEngineValues::SummingMergeTree && rg.nextSmallNumber() < 4)
    {
        ColumnPathList * clist = te->add_params()->mutable_col_list();
        const size_t ncols = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(entries.size()), UINT32_C(4))) + 1;

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        for (size_t i = 0; i < ncols; i++)
        {
            columnPathRef(entries[i], i == 0 ? clist->mutable_col() : clist->add_other_cols());
        }
    }
    return 0;
}

const std::vector<std::string> & s3_compress = {"none", "gzip", "gz", "brotli", "br", "xz", "LZMA", "zstd", "zst"};

int StatementGenerator::generateEngineDetails(RandomGenerator & rg, SQLBase & b, const bool add_pkey, TableEngine * te)
{
    SettingValues * svs = nullptr;

    if (b.isMergeTreeFamily())
    {
        if (!b.is_temp && (supports_cloud_features || replica_setup) && rg.nextSmallNumber() < 4)
        {
            assert(this->ids.empty());
            if (replica_setup)
            {
                this->ids.push_back(TReplicated);
            }
            if (supports_cloud_features)
            {
                this->ids.push_back(TShared);
            }
            b.toption = static_cast<TableEngineOption>(rg.pickRandomlyFromVector(this->ids));
            te->set_toption(b.toption.value());
            this->ids.clear();
        }
        generateMergeTreeEngineDetails(rg, b.teng, b.peer_table, add_pkey, te);
    }
    else if (b.isFileEngine())
    {
        const uint32_t noption = rg.nextSmallNumber();
        TableEngineParam * tep = te->add_params();

        if (noption < 9)
        {
            tep->set_in_out(static_cast<InOutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(InOutFormat_MAX)) + 1));
        }
        else if (noption == 9)
        {
            tep->set_in(static_cast<InFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(InFormat_MAX)) + 1));
        }
        else
        {
            tep->set_out(static_cast<OutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(OutFormat_MAX)) + 1));
        }
    }
    else if (b.isJoinEngine())
    {
        const size_t ncols = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(entries.size()), UINT32_C(3))) + 1;
        JoinType jt = static_cast<JoinType>((rg.nextRandomUInt32() % static_cast<uint32_t>(J_FULL)) + 1);
        TableEngineParam * tep = te->add_params();

        switch (jt)
        {
            case JoinType::J_LEFT:
            case JoinType::J_INNER:
            case JoinType::J_RIGHT:
                tep->set_join_const(static_cast<JoinConst>((rg.nextRandomUInt32() % static_cast<uint32_t>(JoinConst::J_ANTI)) + 1));
                break;
            case JoinType::J_FULL:
                tep->set_join_const(JoinConst::J_ALL);
                break;
            default:
                assert(0);
                break;
        }
        te->add_params()->set_join_op(jt);

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        for (size_t i = 0; i < ncols; i++)
        {
            columnPathRef(entries[i], te->add_params()->mutable_cols());
        }
        if (supports_cloud_features && rg.nextSmallNumber() < 5)
        {
            b.toption = TableEngineOption::TShared;
            te->set_toption(b.toption.value());
        }
    }
    else if (b.isSetEngine() && supports_cloud_features && rg.nextSmallNumber() < 5)
    {
        b.toption = TableEngineOption::TShared;
        te->set_toption(b.toption.value());
    }
    else if (b.isBufferEngine())
    {
        const bool has_tables = collectionHas<SQLTable>(
            [](const SQLTable & t) { return t.db && t.db->attached == DetachStatus::ATTACHED && t.attached == DetachStatus::ATTACHED; });
        const bool has_views = collectionHas<SQLView>(
            [](const SQLView & v) { return v.db && v.db->attached == DetachStatus::ATTACHED && v.attached == DetachStatus::ATTACHED; });

        if (has_tables && (!has_views || rg.nextSmallNumber() < 8))
        {
            const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(
                [](const SQLTable & tt)
                { return tt.db && tt.db->attached == DetachStatus::ATTACHED && tt.attached == DetachStatus::ATTACHED; }));

            te->add_params()->mutable_database()->set_database("d" + std::to_string(t.db->dname));
            te->add_params()->mutable_table()->set_table("t" + std::to_string(t.tname));
        }
        else
        {
            const SQLView & v = rg.pickRandomlyFromVector(filterCollection<SQLView>(
                [](const SQLView & vv)
                { return vv.db && vv.db->attached == DetachStatus::ATTACHED && vv.attached == DetachStatus::ATTACHED; }));

            te->add_params()->mutable_database()->set_database("d" + std::to_string(v.db->dname));
            te->add_params()->mutable_table()->set_table("v" + std::to_string(v.tname));
        }
        //num_layers
        te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 101));
        //min_time, max_time, min_rows, max_rows, min_bytes, max_bytes
        for (int i = 0; i < 6; i++)
        {
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
        }
        if (rg.nextSmallNumber() < 7)
        {
            //flush_time
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 61));
        }
        if (rg.nextSmallNumber() < 7)
        {
            //flush_rows
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
        }
        if (rg.nextSmallNumber() < 7)
        {
            //flush_bytes
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
        }
    }
    else if (b.isMySQLEngine() || b.isPostgreSQLEngine() || b.isSQLiteEngine() || b.isMongoDBEngine() || b.isRedisEngine())
    {
        IntegrationCall next = IntegrationCall::MinIO;

        if (b.isMySQLEngine())
        {
            next = IntegrationCall::MySQL;
        }
        else if (b.isPostgreSQLEngine())
        {
            next = IntegrationCall::PostgreSQL;
        }
        else if (b.isSQLiteEngine())
        {
            next = IntegrationCall::SQLite;
        }
        else if (b.isMongoDBEngine())
        {
            next = IntegrationCall::MongoDB;
        }
        else if (b.isRedisEngine())
        {
            next = IntegrationCall::Redis;
        }
        else
        {
            assert(0);
        }
        connections.createExternalDatabaseTable(rg, next, b, entries, te);
    }
    else if (b.isAnyS3Engine() || b.isHudiEngine() || b.isDeltaLakeEngine() || b.isIcebergEngine())
    {
        connections.createExternalDatabaseTable(rg, IntegrationCall::MinIO, b, entries, te);
        if (b.isAnyS3Engine() || b.isIcebergEngine())
        {
            b.file_format = static_cast<InOutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(InOutFormat_MAX)) + 1);
            te->add_params()->set_in_out(b.file_format);
            if (rg.nextSmallNumber() < 4)
            {
                b.file_comp = rg.pickRandomlyFromVector(s3_compress);
                te->add_params()->set_svalue(b.file_comp);
            }
            if (b.isAnyS3Engine() && rg.nextSmallNumber() < 5)
            {
                generateTableKey(rg, b.teng, false, te->mutable_partition_by());
            }
        }
    }
    else if (b.isMergeEngine())
    {
        setMergeTableParamter<std::shared_ptr<SQLDatabase>>(rg, 'd');
        te->add_params()->set_regexp(buf);

        setMergeTableParamter<SQLTable>(rg, 't');
        te->add_params()->set_svalue(buf);
    }
    if ((b.isRocksEngine() || b.isRedisEngine()) && add_pkey && !entries.empty())
    {
        columnPathRef(rg.pickRandomlyFromVector(entries), te->mutable_primary_key()->add_exprs()->mutable_expr());
    }
    const auto & tsettings = allTableSettings.at(b.teng);
    if (!tsettings.empty() && rg.nextSmallNumber() < 5)
    {
        svs = te->mutable_settings();
        generateSettingValues(rg, tsettings, svs);
    }
    if (b.isMergeTreeFamily() || b.isAnyS3Engine() || b.toption.has_value())
    {
        if (!svs)
        {
            svs = te->mutable_settings();
        }
        if (b.isMergeTreeFamily())
        {
            SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

            sv->set_property("allow_nullable_key");
            sv->set_value("1");

            if (!b.hasClickHousePeer())
            {
                SetValue * sv2 = svs->add_other_values();

                sv2->set_property("allow_experimental_reverse_key");
                sv2->set_value("1");
            }
        }
        else if (b.isAnyS3Engine())
        {
            SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

            sv->set_property("input_format_with_names_use_header");
            sv->set_value("0");
            if (b.isS3QueueEngine())
            {
                SetValue * sv2 = svs->add_other_values();

                sv2->set_property("mode");
                sv2->set_value(rg.nextBool() ? "'ordered'" : "'unordered'");
            }
        }
        if (b.toption.has_value() && b.toption.value() == TableEngineOption::TShared)
        {
            //requires keeper storage
            bool found = false;
            const auto & ovals = svs->other_values();

            for (auto it = ovals.begin(); it != ovals.end() && !found; it++)
            {
                if (it->property() == "storage_policy")
                {
                    auto & prop = const_cast<SetValue &>(*it);
                    prop.set_value("'s3_with_keeper'");
                    found = true;
                }
            }
            if (!found)
            {
                SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

                sv->set_property("storage_policy");
                sv->set_value("'s3_with_keeper'");
            }
        }
    }
    return 0;
}

int StatementGenerator::addTableColumn(
    RandomGenerator & rg,
    SQLTable & t,
    const uint32_t cname,
    const bool staged,
    const bool modify,
    const bool is_pk,
    const ColumnSpecial special,
    ColumnDef * cd)
{
    SQLColumn col;
    SQLType * tp = nullptr;
    auto & to_add = staged ? t.staged_cols : t.cols;
    uint32_t possible_types = std::numeric_limits<uint32_t>::max();

    if (t.isMySQLEngine() || t.hasMySQLPeer())
    {
        possible_types &= ~(
            allow_int128 | allow_dynamic | allow_JSON | allow_array | allow_map | allow_tuple | allow_variant | allow_nested | allow_geo);
    }
    if (t.isPostgreSQLEngine() || t.hasPostgreSQLPeer())
    {
        possible_types &= ~(
            allow_int128 | allow_unsigned_int | allow_dynamic | allow_JSON | allow_map | allow_tuple | allow_variant | allow_nested
            | allow_geo);
        if (t.hasPostgreSQLPeer())
        {
            possible_types &= ~(set_any_datetime_precision); //datetime must have 6 digits precision
        }
    }
    if (t.isSQLiteEngine() || t.hasSQLitePeer())
    {
        possible_types &= ~(
            allow_int128 | allow_unsigned_int | allow_dynamic | allow_JSON | allow_array | allow_map | allow_tuple | allow_variant
            | allow_nested | allow_geo);
        if (t.hasSQLitePeer())
        {
            //for bool it maps to int type, then it outputs 0 as default instead of false
            // for decimal it prints as text
            possible_types &= ~(allow_bool | allow_decimals);
        }
    }
    if (t.isMongoDBEngine())
    {
        possible_types &= ~(allow_dynamic | allow_map | allow_tuple | allow_variant | allow_nested);
    }
    if (t.hasDatabasePeer())
    {
        //ClickHouse's UUID sorting order is different from other databases
        possible_types &= ~(allow_uuid);
    }

    col.cname = cname;
    cd->mutable_col()->set_column("c" + std::to_string(cname));
    if (special == ColumnSpecial::SIGN || special == ColumnSpecial::IS_DELETED)
    {
        tp = new IntType(8, special == ColumnSpecial::IS_DELETED);
        cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_integers(
            special == ColumnSpecial::IS_DELETED ? Integers::UInt8 : Integers::Int8);
    }
    else if (special == ColumnSpecial::VERSION)
    {
        if (((possible_types & (allow_dates | allow_datetimes)) == 0) || rg.nextBool())
        {
            Integers nint;

            std::tie(tp, nint) = randomIntType(rg, possible_types);
            cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_integers(nint);
        }
        else if (((possible_types & allow_datetimes) == 0) || rg.nextBool())
        {
            Dates dd;

            std::tie(tp, dd) = randomDateType(rg, possible_types);
            cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_dates(dd);
        }
        else
        {
            tp = randomDateTimeType(rg, possible_types, cd->mutable_type()->mutable_type()->mutable_non_nullable()->mutable_datetimes());
        }
    }
    else
    {
        tp = randomNextType(rg, possible_types, t.col_counter, cd->mutable_type()->mutable_type());
    }
    col.tp = tp;
    col.special = special;
    if (!modify && col.special == ColumnSpecial::NONE
        && (dynamic_cast<IntType *>(tp) || dynamic_cast<FloatType *>(tp) || dynamic_cast<DateType *>(tp) || dynamic_cast<DateTimeType *>(tp)
            || dynamic_cast<DecimalType *>(tp) || dynamic_cast<StringType *>(tp) || dynamic_cast<const BoolType *>(tp)
            || dynamic_cast<UUIDType *>(tp) || dynamic_cast<IPv4Type *>(tp) || dynamic_cast<IPv6Type *>(tp))
        && rg.nextSmallNumber() < 3)
    {
        cd->set_nullable(rg.nextBool());
        col.nullable = std::optional<bool>(cd->nullable());
    }
    if (rg.nextSmallNumber() < 2)
    {
        generateNextStatistics(rg, cd->mutable_stats());
    }
    if (col.special == ColumnSpecial::NONE && rg.nextSmallNumber() < 2)
    {
        DefaultModifier * def_value = cd->mutable_defaultv();
        DModifier dmod = static_cast<DModifier>((rg.nextRandomUInt32() % static_cast<uint32_t>(DModifier_MAX)) + 1);

        if (is_pk && dmod == DModifier::DEF_EPHEMERAL)
        {
            dmod = DModifier::DEF_DEFAULT;
        }
        def_value->set_dvalue(dmod);
        col.dmod = std::optional<DModifier>(dmod);
        if (dmod != DModifier::DEF_EPHEMERAL || rg.nextMediumNumber() < 21)
        {
            addTableRelation(rg, false, "", t);
            this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs
                = this->allow_in_expression_alias = this->allow_subqueries = false;
            generateExpression(rg, def_value->mutable_expr());
            this->allow_in_expression_alias = this->allow_subqueries = true;
            this->levels.clear();
        }
    }
    if (t.isMergeTreeFamily())
    {
        const auto & csettings = allColumnSettings.at(t.teng);

        if ((!col.dmod.has_value() || col.dmod.value() != DModifier::DEF_ALIAS) && rg.nextMediumNumber() < 16)
        {
            generateNextCodecs(rg, cd->mutable_codecs());
        }
        if ((!col.dmod.has_value() || col.dmod.value() != DModifier::DEF_EPHEMERAL) && !csettings.empty() && rg.nextMediumNumber() < 16)
        {
            generateSettingValues(rg, csettings, cd->mutable_settings());
        }
        if (!t.hasDatabasePeer() && rg.nextMediumNumber() < 16)
        {
            flatTableColumnPath(0, t, [](const SQLColumn & c) { return !dynamic_cast<NestedType *>(c.tp); });
            generateTTLExpression(rg, t, cd->mutable_ttl_expr());
            this->entries.clear();
        }
        cd->set_is_pkey(is_pk);
    }
    if (rg.nextSmallNumber() < 3)
    {
        buf.resize(0);
        rg.nextString(buf, "'", true, rg.nextRandomUInt32() % 1009);
        cd->set_comment(buf);
    }
    to_add[cname] = std::move(col);
    return 0;
}

int StatementGenerator::addTableIndex(RandomGenerator & rg, SQLTable & t, const bool staged, IndexDef * idef)
{
    SQLIndex idx;
    const uint32_t iname = t.idx_counter++;
    Expr * expr = idef->mutable_expr();
    const IndexType itpe = static_cast<IndexType>((rg.nextRandomUInt32() % static_cast<uint32_t>(IndexType_MAX)) + 1);
    auto & to_add = staged ? t.staged_idxs : t.idxs;

    idx.iname = iname;
    idef->mutable_idx()->set_index("i" + std::to_string(iname));
    idef->set_type(itpe);
    if (rg.nextSmallNumber() < 9)
    {
        flatTableColumnPath(
            flat_tuple | flat_nested | flat_json | skip_nested_node,
            t,
            [&itpe](const SQLColumn & c) { return itpe < IndexType::IDX_ngrambf_v1 || hasType<StringType, true, true, true>(c.tp); });
    }
    if (!entries.empty())
    {
        std::shuffle(entries.begin(), entries.end(), rg.generator);

        if (itpe == IndexType::IDX_hypothesis && entries.size() > 1 && rg.nextSmallNumber() < 9)
        {
            BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();
            Expr * expr1 = bexpr->mutable_lhs();
            Expr * expr2 = bexpr->mutable_rhs();
            ExprSchemaTableColumn * estc1 = expr1->mutable_comp_expr()->mutable_expr_stc();
            ExprSchemaTableColumn * estc2 = expr2->mutable_comp_expr()->mutable_expr_stc();

            bexpr->set_op(
                rg.nextSmallNumber() < 8
                    ? BinaryOperator::BINOP_EQ
                    : static_cast<BinaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(BinaryOperator::BINOP_LEGR)) + 1));
            columnPathRef(this->entries[0], estc1->mutable_col()->mutable_path());
            columnPathRef(this->entries[1], estc2->mutable_col()->mutable_path());
        }
        else
        {
            columnPathRef(this->entries[0], expr);
        }
        entries.clear();
    }
    else
    {
        addTableRelation(rg, false, "", t);
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs
            = this->allow_in_expression_alias = this->allow_subqueries = false;
        generateExpression(rg, expr);
        this->allow_in_expression_alias = this->allow_subqueries = true;
        this->levels.clear();
    }
    switch (itpe)
    {
        case IndexType::IDX_set:
            if (rg.nextSmallNumber() < 7)
            {
                idef->add_params()->set_ival(0);
            }
            else
            {
                std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
                idef->add_params()->set_ival(next_dist(rg.generator));
            }
            break;
        case IndexType::IDX_bloom_filter: {
            std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
            idef->add_params()->set_dval(static_cast<double>(next_dist(rg.generator)) / static_cast<double>(1000));
        }
        break;
        case IndexType::IDX_ngrambf_v1:
        case IndexType::IDX_tokenbf_v1: {
            std::uniform_int_distribution<uint32_t> next_dist1(1, 1000);
            std::uniform_int_distribution<uint32_t> next_dist2(1, 5);

            if (itpe == IndexType::IDX_ngrambf_v1)
            {
                idef->add_params()->set_ival(next_dist1(rg.generator));
            }
            idef->add_params()->set_ival(next_dist1(rg.generator));
            idef->add_params()->set_ival(next_dist2(rg.generator));
            idef->add_params()->set_ival(next_dist1(rg.generator));
        }
        break;
        case IndexType::IDX_full_text:
        case IndexType::IDX_inverted: {
            std::uniform_int_distribution<uint32_t> next_dist(0, 10);
            idef->add_params()->set_ival(next_dist(rg.generator));
        }
        break;
        case IndexType::IDX_minmax:
        case IndexType::IDX_hypothesis:
            break;
    }
    if (rg.nextSmallNumber() < 7)
    {
        std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
        idef->set_granularity(next_dist(rg.generator));
    }
    to_add[iname] = std::move(idx);
    return 0;
}

int StatementGenerator::addTableProjection(RandomGenerator & rg, SQLTable & t, const bool staged, ProjectionDef * pdef)
{
    const uint32_t pname = t.proj_counter++;
    const uint32_t ncols = std::max(std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % UINT32_C(3)) + 1), UINT32_C(1));
    auto & to_add = staged ? t.staged_projs : t.projs;

    pdef->mutable_proj()->set_projection("p" + std::to_string(pname));
    this->inside_projection = true;
    addTableRelation(rg, false, "", t);
    generateSelect(rg, true, false, ncols, allow_groupby | allow_orderby, pdef->mutable_select());
    this->levels.clear();
    this->inside_projection = false;
    to_add.insert(pname);
    return 0;
}

int StatementGenerator::addTableConstraint(RandomGenerator & rg, SQLTable & t, const bool staged, ConstraintDef * cdef)
{
    const uint32_t crname = t.constr_counter++;
    auto & to_add = staged ? t.staged_constrs : t.constrs;

    cdef->mutable_constr()->set_constraint("c" + std::to_string(crname));
    cdef->set_ctype(
        static_cast<ConstraintDef_ConstraintType>((rg.nextRandomUInt32() % static_cast<uint32_t>(ConstraintDef::ConstraintType_MAX)) + 1));
    addTableRelation(rg, false, "", t);
    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
    this->generateWherePredicate(rg, cdef->mutable_expr());
    this->levels.clear();
    to_add.insert(crname);
    return 0;
}

PeerTableDatabase StatementGenerator::getNextPeerTableDatabase(RandomGenerator & rg, TableEngineValues teng)
{
    assert(this->ids.empty());
    if (teng != TableEngineValues::Set)
    {
        if (teng != TableEngineValues::MySQL && connections.hasMySQLConnection())
        {
            this->ids.push_back(static_cast<uint32_t>(PeerTableDatabase::MySQL));
        }
        if (teng != TableEngineValues::PostgreSQL && connections.hasPostgreSQLConnection())
        {
            this->ids.push_back(static_cast<uint32_t>(PeerTableDatabase::PostgreSQL));
        }
        if (teng != TableEngineValues::SQLite && connections.hasSQLiteConnection())
        {
            this->ids.push_back(static_cast<uint32_t>(PeerTableDatabase::SQLite));
        }
        if (teng >= TableEngineValues::MergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree
            && connections.hasClickHouseExtraServerConnection())
        {
            this->ids.push_back(static_cast<uint32_t>(PeerTableDatabase::ClickHouse));
            this->ids.push_back(static_cast<uint32_t>(PeerTableDatabase::ClickHouse)); // give more probability
        }
    }
    const auto res = (this->ids.empty() || rg.nextBool()) ? PeerTableDatabase::None
                                                          : static_cast<PeerTableDatabase>(rg.pickRandomlyFromVector(this->ids));
    this->ids.clear();
    return res;
}

TableEngineValues StatementGenerator::getNextTableEngine(RandomGenerator & rg, const bool use_external_integrations)
{
    if (rg.nextSmallNumber() < 9)
    {
        std::uniform_int_distribution<uint32_t> table_engine(1, TableEngineValues::VersionedCollapsingMergeTree);
        return static_cast<TableEngineValues>(table_engine(rg.generator));
    }
    assert(this->ids.empty());
    this->ids.push_back(MergeTree);
    this->ids.push_back(ReplacingMergeTree);
    this->ids.push_back(SummingMergeTree);
    this->ids.push_back(AggregatingMergeTree);
    this->ids.push_back(CollapsingMergeTree);
    this->ids.push_back(VersionedCollapsingMergeTree);
    this->ids.push_back(File);
    this->ids.push_back(Null);
    this->ids.push_back(Set);
    this->ids.push_back(Join);
    this->ids.push_back(Memory);
    this->ids.push_back(StripeLog);
    this->ids.push_back(Log);
    this->ids.push_back(TinyLog);
    this->ids.push_back(EmbeddedRocksDB);
    this->ids.push_back(Merge);
    if (collectionHas<SQLTable>([](const SQLTable & t)
                                { return t.db && t.db->attached == DetachStatus::ATTACHED && t.attached == DetachStatus::ATTACHED; })
        || collectionHas<SQLView>([](const SQLView & v)
                                  { return v.db && v.db->attached == DetachStatus::ATTACHED && v.attached == DetachStatus::ATTACHED; }))
    {
        this->ids.push_back(Buffer);
    }
    if (use_external_integrations)
    {
        if (connections.hasMySQLConnection())
        {
            this->ids.push_back(MySQL);
        }
        if (connections.hasPostgreSQLConnection())
        {
            this->ids.push_back(PostgreSQL);
        }
        if (connections.hasSQLiteConnection())
        {
            this->ids.push_back(SQLite);
        }
        if (connections.hasMongoDBConnection())
        {
            this->ids.push_back(MongoDB);
        }
        if (connections.hasRedisConnection())
        {
            this->ids.push_back(Redis);
        }
        if (connections.hasMinIOConnection())
        {
            this->ids.push_back(S3);
            //this->ids.push_back(S3Queue);
            this->ids.push_back(Hudi);
            this->ids.push_back(DeltaLake);
            //this->ids.push_back(IcebergS3);
        }
    }

    const auto res = static_cast<TableEngineValues>(rg.pickRandomlyFromVector(this->ids));
    this->ids.clear();
    return res;
}

const std::vector<TableEngineValues> like_engs
    = {TableEngineValues::MergeTree,
       TableEngineValues::ReplacingMergeTree,
       TableEngineValues::SummingMergeTree,
       TableEngineValues::AggregatingMergeTree,
       TableEngineValues::File,
       TableEngineValues::Null,
       TableEngineValues::Set,
       TableEngineValues::Join,
       TableEngineValues::Memory,
       TableEngineValues::StripeLog,
       TableEngineValues::Log,
       TableEngineValues::TinyLog,
       TableEngineValues::EmbeddedRocksDB,
       TableEngineValues::Merge};

int StatementGenerator::generateNextCreateTable(RandomGenerator & rg, CreateTable * ct)
{
    SQLTable next;
    uint32_t tname = 0;
    bool added_pkey = false;
    TableEngine * te = ct->mutable_engine();
    ExprSchemaTable * est = ct->mutable_est();
    const bool replace = collectionCount<SQLTable>(
                             [](const SQLTable & tt)
                             {
                                 return (!tt.db || tt.db->attached == DetachStatus::ATTACHED) && tt.attached == DetachStatus::ATTACHED
                                     && !tt.hasDatabasePeer();
                             })
            > 3
        && rg.nextMediumNumber() < 16;

    next.is_temp = rg.nextMediumNumber() < 11;
    ct->set_is_temp(next.is_temp);
    if (replace)
    {
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(
            [](const SQLTable & tt)
            {
                return (!tt.db || tt.db->attached == DetachStatus::ATTACHED) && tt.attached == DetachStatus::ATTACHED
                    && !tt.hasDatabasePeer();
            }));

        next.db = t.db;
        tname = next.tname = t.tname;
    }
    else
    {
        if (!next.is_temp && collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && rg.nextSmallNumber() < 9)
        {
            next.db = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    ct->set_replace(replace);
    if (next.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(next.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(next.tname));
    if (!collectionHas<SQLTable>(
            [](const SQLTable & tt)
            { return (!tt.db || tt.db->attached == DetachStatus::ATTACHED) && tt.attached == DetachStatus::ATTACHED && !tt.is_temp; })
        || rg.nextSmallNumber() < 9)
    {
        //create table with definition
        TableDef * colsdef = ct->mutable_table_def();

        next.teng = getNextTableEngine(rg, true);
        te->set_engine(next.teng);
        next.peer_table = getNextPeerTableDatabase(rg, next.teng);
        added_pkey |= (!next.isMergeTreeFamily() && !next.isRocksEngine() && !next.isRedisEngine());
        const bool add_version_to_replacing = next.teng == TableEngineValues::ReplacingMergeTree && !next.hasPostgreSQLPeer()
            && !next.hasSQLitePeer() && rg.nextSmallNumber() < 4;
        uint32_t added_cols = 0;
        uint32_t added_idxs = 0;
        uint32_t added_projs = 0;
        uint32_t added_consts = 0;
        uint32_t added_sign = 0;
        uint32_t added_is_deleted = 0;
        uint32_t added_version = 0;
        const uint32_t to_addcols = (rg.nextMediumNumber() % 5) + 1;
        const uint32_t to_addidxs
            = (rg.nextMediumNumber() % 4) * static_cast<uint32_t>(next.isMergeTreeFamily() && rg.nextSmallNumber() < 4);
        const uint32_t to_addprojs
            = (rg.nextMediumNumber() % 3) * static_cast<uint32_t>(next.isMergeTreeFamily() && rg.nextSmallNumber() < 5);
        const uint32_t to_addconsts = (rg.nextMediumNumber() % 3) * static_cast<uint32_t>(rg.nextSmallNumber() < 3);
        const uint32_t to_add_sign = static_cast<uint32_t>(next.hasSignColumn());
        const uint32_t to_add_version = static_cast<uint32_t>(next.hasVersionColumn() || add_version_to_replacing);
        const uint32_t to_add_is_deleted = static_cast<uint32_t>(add_version_to_replacing && rg.nextSmallNumber() < 4);
        const uint32_t total_to_add
            = to_addcols + to_addidxs + to_addprojs + to_addconsts + to_add_sign + to_add_version + to_add_is_deleted;

        for (uint32_t i = 0; i < total_to_add; i++)
        {
            const uint32_t add_idx = 4 * static_cast<uint32_t>(!next.cols.empty() && added_idxs < to_addidxs);
            const uint32_t add_proj = 4 * static_cast<uint32_t>(!next.cols.empty() && added_projs < to_addprojs);
            const uint32_t add_const = 4 * static_cast<uint32_t>(!next.cols.empty() && added_consts < to_addconsts);
            const uint32_t add_col = 8 * static_cast<uint32_t>(added_cols < to_addcols);
            const uint32_t add_sign = 2 * static_cast<uint32_t>(added_sign < to_add_sign);
            const uint32_t add_version = 2 * static_cast<uint32_t>(added_version < to_add_version && added_sign == to_add_sign);
            const uint32_t add_is_deleted
                = 2 * static_cast<uint32_t>(added_is_deleted < to_add_is_deleted && added_version == to_add_version);
            const uint32_t prob_space = add_idx + add_proj + add_const + add_col + add_sign + add_version + add_is_deleted;
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            if (add_idx && nopt < (add_idx + 1))
            {
                addTableIndex(rg, next, false, colsdef->add_other_defs()->mutable_idx_def());
                added_idxs++;
            }
            else if (add_proj && nopt < (add_idx + add_proj + 1))
            {
                addTableProjection(rg, next, false, colsdef->add_other_defs()->mutable_proj_def());
                added_projs++;
            }
            else if (add_const && nopt < (add_idx + add_proj + add_const + 1))
            {
                addTableConstraint(rg, next, false, colsdef->add_other_defs()->mutable_const_def());
                added_consts++;
            }
            else if (add_col && nopt < (add_idx + add_proj + add_const + add_col + 1))
            {
                const bool add_pkey = !added_pkey && rg.nextMediumNumber() < 4;
                ColumnDef * cd = i == 0 ? colsdef->mutable_col_def() : colsdef->add_other_defs()->mutable_col_def();

                addTableColumn(rg, next, next.col_counter++, false, false, add_pkey, ColumnSpecial::NONE, cd);
                added_pkey |= add_pkey;
                added_cols++;
            }
            else
            {
                const uint32_t cname = next.col_counter++;
                const bool add_pkey = !added_pkey && rg.nextMediumNumber() < 4;
                const bool add_version_col = add_version && nopt < (add_idx + add_proj + add_const + add_col + add_version + 1);
                ColumnDef * cd = i == 0 ? colsdef->mutable_col_def() : colsdef->add_other_defs()->mutable_col_def();

                addTableColumn(
                    rg,
                    next,
                    cname,
                    false,
                    false,
                    add_pkey,
                    add_version_col ? ColumnSpecial::VERSION : (add_sign ? ColumnSpecial::SIGN : ColumnSpecial::IS_DELETED),
                    cd);
                added_pkey |= add_pkey;
                te->add_params()->mutable_cols()->mutable_col()->set_column("c" + std::to_string(cname));
                if (add_version_col)
                {
                    added_version++;
                }
                else if (add_sign)
                {
                    assert(!add_is_deleted);
                    added_sign++;
                }
                else
                {
                    assert(add_is_deleted);
                    added_is_deleted++;
                }
            }
        }
        if (rg.nextSmallNumber() < 2)
        {
            this->levels[this->current_level] = QueryLevel(this->current_level);
            generateSelect(
                rg,
                true,
                false,
                static_cast<uint32_t>(next.numberOfInsertableColumns()),
                std::numeric_limits<uint32_t>::max(),
                ct->mutable_as_select_stmt());
        }
    }
    else
    {
        //create table as
        CreateTableAs * cta = ct->mutable_table_as();
        ExprSchemaTable * aest = cta->mutable_est();
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(
            [](const SQLTable & tt)
            { return (!tt.db || tt.db->attached == DetachStatus::ATTACHED) && tt.attached == DetachStatus::ATTACHED && !tt.is_temp; }));
        std::uniform_int_distribution<size_t> table_engine(0, rg.nextSmallNumber() < 8 ? 3 : (like_engs.size() - 1));
        TableEngineValues val = like_engs[table_engine(rg.generator)];

        next.teng = val;
        te->set_engine(val);
        cta->set_clone(next.isMergeTreeFamily() && t.isMergeTreeFamily() && rg.nextBool());
        if (t.db)
        {
            aest->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        aest->mutable_table()->set_table("t" + std::to_string(t.tname));
        for (const auto & col : t.cols)
        {
            next.cols[col.first] = col.second;
        }
        for (const auto & idx : t.idxs)
        {
            next.idxs[idx.first] = idx.second;
        }
        next.projs.insert(t.projs.begin(), t.projs.end());
        next.constrs.insert(t.constrs.begin(), t.constrs.end());
        next.col_counter = t.col_counter;
        next.idx_counter = t.idx_counter;
        next.proj_counter = t.proj_counter;
        next.constr_counter = t.constr_counter;
        next.is_temp = t.is_temp;
    }

    flatTableColumnPath(flat_tuple | flat_nested | flat_json | skip_nested_node, next, [](const SQLColumn &) { return true; });
    generateEngineDetails(rg, next, !added_pkey, te);
    entries.clear();
    if (next.hasDatabasePeer())
    {
        flatTableColumnPath(0, next, [](const SQLColumn &) { return true; });
        connections.createPeerTable(rg, next.peer_table, next, ct, entries);
        entries.clear();
    }
    else if (next.isMergeTreeFamily())
    {
        bool has_date_cols = false;

        flatTableColumnPath(0, next, [](const SQLColumn & c) { return !dynamic_cast<NestedType *>(c.tp); });
        for (const auto & entry : entries)
        {
            SQLType * tp = entry.getBottomType();

            if (dynamic_cast<DateTimeType *>(tp) || dynamic_cast<DateType *>(tp))
            {
                has_date_cols = true;
                break;
            }
        }
        if (has_date_cols || rg.nextMediumNumber() < 6)
        {
            generateNextTTL(rg, next, te, te->mutable_ttl_expr());
        }
        entries.clear();
    }

    assert(!next.toption.has_value() || next.isMergeTreeFamily() || next.isJoinEngine() || next.isSetEngine());
    this->staged_tables[tname] = std::move(next);
    return 0;
}

}
