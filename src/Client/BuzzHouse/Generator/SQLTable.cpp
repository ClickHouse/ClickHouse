#include <cstdint>

#include <Client/BuzzHouse/Generator/SQLCatalog.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>

namespace BuzzHouse
{

String StatementGenerator::nextComment(RandomGenerator & rg) const
{
    return rg.nextSmallNumber() < 4 ? "''" : rg.nextString("'", true, rg.nextStrlen());
}

void collectColumnPaths(
    const String cname, SQLType * tp, const uint32_t flags, ColumnPathChain & next, std::vector<ColumnPathChain> & paths)
{
    ArrayType * at = nullptr;
    MapType * mt = nullptr;
    TupleType * ttp = nullptr;
    NestedType * ntp = nullptr;
    JSONType * jt = nullptr;

    checkStackSize();
    /// Append this node to the path
    next.path.emplace_back(ColumnPathChainEntry(cname, tp));
    if (((flags & skip_nested_node) == 0 || tp->getTypeClass() != SQLTypeClass::NESTED)
        && ((flags & skip_tuple_node) == 0 || tp->getTypeClass() != SQLTypeClass::TUPLE))
    {
        paths.push_back(next);
    }
    if ((flags & collect_generated) != 0 && tp->getTypeClass() == SQLTypeClass::NULLABLE)
    {
        next.path.emplace_back(ColumnPathChainEntry("null", &(*null_tp)));
        paths.push_back(next);
        next.path.pop_back();
    }
    else if ((flags & collect_generated) != 0 && ((at = dynamic_cast<ArrayType *>(tp)) || (mt = dynamic_cast<MapType *>(tp))))
    {
        uint32_t i = 1;

        next.path.emplace_back(ColumnPathChainEntry("size0", &(*size_tp)));
        paths.push_back(next);
        next.path.pop_back();
        while (at && (at = dynamic_cast<ArrayType *>(at->subtype)))
        {
            next.path.emplace_back(ColumnPathChainEntry("size" + std::to_string(i), &(*size_tp)));
            paths.push_back(next);
            next.path.pop_back();
            i++;
        }
        if (mt)
        {
            next.path.emplace_back(ColumnPathChainEntry("keys", mt->key));
            paths.push_back(next);
            next.path.pop_back();
            next.path.emplace_back(ColumnPathChainEntry("values", mt->value));
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
            const String nsub = "c" + std::to_string(entry.cname);

            collectColumnPaths(nsub, entry.subtype, flags, next, paths);
            if ((flags & collect_generated) != 0)
            {
                /// The size entry also exists for nested cols
                next.path.emplace_back(ColumnPathChainEntry(nsub, entry.subtype));
                next.path.emplace_back(ColumnPathChainEntry("size0", &(*size_tp)));
                paths.push_back(next);
                next.path.pop_back();
                next.path.pop_back();
            }
        }
    }
    else if ((flags & flat_json) != 0 && (jt = dynamic_cast<JSONType *>(tp)))
    {
        for (const auto & entry : jt->subcols)
        {
            next.path.emplace_back(ColumnPathChainEntry(entry.cname, entry.subtype));
            paths.push_back(next);
            next.path.pop_back();
        }
    }
    /// Remove the last element from the path
    next.path.pop_back();
}

void StatementGenerator::flatTableColumnPath(
    const uint32_t flags, const std::unordered_map<uint32_t, SQLColumn> & cols, std::function<bool(const SQLColumn & c)> col_filter)
{
    auto & res = ((flags & to_table_entries) != 0) ? this->table_entries
                                                   : (((flags & to_remote_entries) != 0) ? this->remote_entries : this->entries);

    chassert(res.empty());
    for (const auto & [key, val] : cols)
    {
        if (col_filter(val))
        {
            ColumnPathChain cpc(val.nullable, val.special, val.dmod, {});

            collectColumnPaths("c" + std::to_string(key), val.tp, flags, cpc, res);
        }
    }
}

void StatementGenerator::flatColumnPath(const uint32_t flags, const std::unordered_map<uint32_t, std::unique_ptr<SQLType>> & centries)
{
    auto & res = ((flags & to_table_entries) != 0) ? this->table_entries
                                                   : (((flags & to_remote_entries) != 0) ? this->remote_entries : this->entries);

    chassert(res.empty());
    for (const auto & [key, val] : centries)
    {
        ColumnPathChain cpc(hasType<Nullable>(false, false, false, val.get()), ColumnSpecial::NONE, std::nullopt, {});

        collectColumnPaths("c" + std::to_string(key), val.get(), flags, cpc, res);
    }
}

SQLRelation
StatementGenerator::createTableRelation(RandomGenerator & rg, const bool allow_internal_cols, const String & rel_name, const SQLTable & t)
{
    SQLRelation rel(rel_name);

    flatTableColumnPath(
        flat_tuple | flat_nested | flat_json | to_table_entries | collect_generated, t.cols, [](const SQLColumn &) { return true; });
    for (const auto & entry : this->table_entries)
    {
        DB::Strings names;

        names.reserve(entry.path.size());
        for (const auto & path : entry.path)
        {
            names.push_back(path.cname);
        }
        rel.cols.emplace_back(SQLRelationCol(rel_name, std::move(names)));
    }
    this->table_entries.clear();
    if (allow_internal_cols && rg.nextSmallNumber() < 3)
    {
        if (t.isMergeTreeFamily() && this->allow_not_deterministic)
        {
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_block_number"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_block_offset"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_disk_name"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_part"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_part_data_version"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_part_granule_offset"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_part_index"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_part_offset"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_part_starting_offset"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_part_uuid"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_partition_id"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_partition_value"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_row_exists"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_sample_factor"}));
        }
        else if (
            t.isAnyS3Engine() || t.isAnyAzureEngine() || t.isAnyDeltaLakeEngine() || t.isAnyIcebergEngine() || t.isFileEngine()
            || t.isURLEngine())
        {
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_path"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_file"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_size"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_time"}));
            if (t.isAnyDeltaLakeEngine() || t.isAnyIcebergEngine())
            {
                rel.cols.emplace_back(SQLRelationCol(rel_name, {"_data_lake_snapshot_version"}));
            }
            if (t.isURLEngine())
            {
                rel.cols.emplace_back(SQLRelationCol(rel_name, {"_headers"}));
            }
            else
            {
                rel.cols.emplace_back(SQLRelationCol(rel_name, {"_etag"}));
            }
        }
        else if (t.isDistributedEngine())
        {
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_shard_num"}));
        }
        else if (t.isMaterializedPostgreSQLEngine())
        {
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_version"}));
            rel.cols.emplace_back(SQLRelationCol(rel_name, {"_sign"}));
        }
        rel.cols.emplace_back(SQLRelationCol(rel_name, {"_table"}));
    }
    return rel;
}

void StatementGenerator::addTableRelation(RandomGenerator & rg, const bool allow_internal_cols, const String & rel_name, const SQLTable & t)
{
    const SQLRelation rel = createTableRelation(rg, allow_internal_cols, rel_name, t);

    if (this->levels.find(this->current_level) == this->levels.end())
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);
    }
    this->levels[this->current_level].rels.emplace_back(rel);
}

SQLRelation StatementGenerator::createViewRelation(const String & rel_name, const SQLView & v)
{
    SQLRelation rel(rel_name);

    assert(!v.cols.empty());
    for (const auto & entry : v.cols)
    {
        rel.cols.emplace_back(SQLRelationCol(rel_name, {"c" + std::to_string(entry)}));
    }
    return rel;
}

void StatementGenerator::addViewRelation(const String & rel_name, const SQLView & v)
{
    const SQLRelation rel = createViewRelation(rel_name, v);

    if (this->levels.find(this->current_level) == this->levels.end())
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);
    }
    this->levels[this->current_level].rels.emplace_back(rel);
}

void StatementGenerator::addDictionaryRelation(const String & rel_name, const SQLDictionary & d)
{
    SQLRelation rel(rel_name);

    flatTableColumnPath(
        flat_tuple | flat_nested | flat_json | to_table_entries | collect_generated, d.cols, [](const SQLColumn &) { return true; });
    for (const auto & entry : this->table_entries)
    {
        DB::Strings names;

        names.reserve(entry.path.size());
        for (const auto & path : entry.path)
        {
            names.push_back(path.cname);
        }
        rel.cols.emplace_back(SQLRelationCol(rel_name, std::move(names)));
    }
    this->table_entries.clear();
    if (this->levels.find(this->current_level) == this->levels.end())
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);
    }
    this->levels[this->current_level].rels.emplace_back(rel);
}

void StatementGenerator::generateNextStatistics(RandomGenerator & rg, ColumnStatistics * cstats)
{
    const size_t nstats = (rg.nextMediumNumber() % static_cast<uint32_t>(ColumnStat_MAX)) + 1;

    for (uint32_t i = 1; i <= ColumnStat_MAX; i++)
    {
        ids.emplace_back(i);
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
}

void StatementGenerator::generateNextCodecs(RandomGenerator & rg, CodecList * cl)
{
    const uint32_t ncodecs = (rg.nextMediumNumber() % UINT32_C(3)) + 1;
    std::uniform_int_distribution<uint32_t> codec_range(1, static_cast<uint32_t>(CompressionCodec_MAX));

    for (uint32_t i = 0; i < ncodecs; i++)
    {
        CodecParam * cp = i == 0 ? cl->mutable_codec() : cl->add_other_codecs();
        const CompressionCodec cc = static_cast<CompressionCodec>(codec_range(rg.generator));

        cp->set_codec(cc);
        switch (cc)
        {
            case COMP_LZ4HC:
            case COMP_ZSTD_QAT:
                if (rg.nextBool())
                {
                    std::uniform_int_distribution<uint32_t> next_dist(1, 12);
                    cp->add_params()->set_ival(next_dist(rg.generator));
                }
                break;
            case COMP_ZSTD:
                if (rg.nextBool())
                {
                    std::uniform_int_distribution<uint32_t> next_dist(1, 22);
                    cp->add_params()->set_ival(next_dist(rg.generator));
                }
                break;
            case COMP_Delta:
            case COMP_DoubleDelta:
            case COMP_Gorilla:
                if (rg.nextBool())
                {
                    std::uniform_int_distribution<uint32_t> next_dist(0, 3);
                    cp->add_params()->set_ival(UINT32_C(1) << next_dist(rg.generator));
                }
                break;
            case COMP_FPC:
                if (rg.nextBool())
                {
                    std::uniform_int_distribution<uint32_t> next_dist1(1, 28);
                    cp->add_params()->set_ival(next_dist1(rg.generator));
                    cp->add_params()->set_ival(rg.nextBool() ? 4 : 9);
                }
                break;
            default:
                break;
        }
    }
}

void StatementGenerator::generateTableExpression(RandomGenerator & rg, const bool use_global_agg, Expr * expr)
{
    const bool prev_allow_in_expression_alias = this->allow_in_expression_alias;
    const bool prev_allow_subqueries = this->allow_subqueries;

    this->levels[this->current_level].global_aggregate = use_global_agg && rg.nextSmallNumber() < 9;
    this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
    this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
    this->allow_in_expression_alias = rg.nextMediumNumber() < 11;
    this->allow_subqueries = rg.nextMediumNumber() < 11;
    generateExpression(rg, expr);
    this->allow_in_expression_alias = prev_allow_in_expression_alias;
    this->allow_subqueries = prev_allow_subqueries;
    this->levels.clear();
}

void StatementGenerator::generateTTLExpression(RandomGenerator & rg, const std::optional<SQLTable> & t, Expr * ttl_expr)
{
    chassert(filtered_entries.empty());
    for (const auto & entry : this->entries)
    {
        SQLType * tp = entry.getBottomType();

        if (!tp || (tp && (tp->getTypeClass() == SQLTypeClass::DATE || tp->getTypeClass() == SQLTypeClass::DATETIME)))
        {
            filtered_entries.emplace_back(std::ref<const ColumnPathChain>(entry));
        }
    }
    if (!filtered_entries.empty() && rg.nextMediumNumber() < 96)
    {
        BinaryExpr * bexpr = ttl_expr->mutable_comp_expr()->mutable_binary_expr();
        IntervalExpr * ie = bexpr->mutable_rhs()->mutable_comp_expr()->mutable_interval();
        IntLiteral * il = ie->mutable_expr()->mutable_lit_val()->mutable_int_lit();
        std::uniform_int_distribution<int64_t> next_dist(-100, 100);

        bexpr->set_op(rg.nextBool() ? BinaryOperator::BINOP_PLUS : BinaryOperator::BINOP_MINUS);
        columnPathRef(rg.pickRandomly(filtered_entries).get(), bexpr->mutable_lhs());
        ie->set_interval(
            static_cast<IntervalExpr_Interval>((rg.nextRandomUInt32() % static_cast<uint32_t>(IntervalExpr_Interval_MINUTE)) + 1));
        il->set_int_lit(next_dist(rg.generator));
        filtered_entries.clear();
    }
    else
    {
        filtered_entries.clear();
        if (t.has_value() && !t.value().cols.empty())
        {
            addTableRelation(rg, true, "", t.value());
        }
        generateTableExpression(rg, false, ttl_expr);
    }
}

void StatementGenerator::generateNextTTL(
    RandomGenerator & rg, const std::optional<SQLTable> & t, const TableEngine * te, TTLExpr * ttl_expr)
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
                    const bool prev_allow_in_expression_alias = this->allow_in_expression_alias;
                    const bool prev_allow_subqueries = this->allow_subqueries;

                    if (t.has_value() && !t.value().cols.empty())
                    {
                        addTableRelation(rg, true, "", t.value());
                    }
                    this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
                    this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
                    this->allow_in_expression_alias = rg.nextMediumNumber() < 11;
                    this->allow_subqueries = rg.nextMediumNumber() < 11;
                    generateWherePredicate(rg, tdel->mutable_where()->mutable_expr()->mutable_expr());
                    this->allow_in_expression_alias = prev_allow_in_expression_alias;
                    this->allow_subqueries = prev_allow_subqueries;
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
            const size_t nset = (rg.nextLargeNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(3))) + 1;

            for (uint32_t j = 0; j < ttl_group_size; j++)
            {
                const TableKeyExpr & tke = tk.exprs(j);
                Expr * expr = j == 0 ? el->mutable_expr() : el->add_extra_exprs();

                expr->CopyFrom(tke.expr());
            }

            std::shuffle(entries.begin(), entries.end(), rg.generator);
            for (size_t j = 0; j < nset; j++)
            {
                TTLSet * tset = j == 0 ? gb->mutable_ttl_set() : gb->add_other_ttl_set();

                columnPathRef(entries[j], tset->mutable_col());
                if (t.has_value() && !t.value().cols.empty())
                {
                    addTableRelation(rg, true, "", t.value());
                }
                /// Use global aggregate most of the time
                generateTableExpression(rg, true, tset->mutable_expr());
            }
        }
    }
}

void StatementGenerator::pickUpNextCols(RandomGenerator & rg, const SQLTable & t, ColumnPathList * clist)
{
    flatTableColumnPath(flat_nested | skip_nested_node, t.cols, [](const SQLColumn &) { return true; });
    const uint32_t ocols = (rg.nextLargeNumber() % std::min<uint32_t>(static_cast<uint32_t>(this->entries.size()), UINT32_C(4))) + 1;
    std::shuffle(entries.begin(), entries.end(), rg.generator);
    for (uint32_t i = 0; i < ocols; i++)
    {
        columnPathRef(this->entries[i], i == 0 ? clist->mutable_col() : clist->add_other_cols());
    }
    entries.clear();
}

static const std::vector<SQLFunc> multicolHash
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

static const std::vector<SQLFunc> datesHash
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

static const std::vector<SQLFunc> arithmeticFuncs
    = {SQLFunc::FUNCplus,
       SQLFunc::FUNCminus,
       SQLFunc::FUNCmultiply,
       SQLFunc::FUNCdivide,
       SQLFunc::FUNCintDiv,
       SQLFunc::FUNCintDivOrZero,
       SQLFunc::FUNCifNotFinite,
       SQLFunc::FUNCmodulo,
       SQLFunc::FUNCmoduloOrZero,
       SQLFunc::FUNCpositiveModulo,
       SQLFunc::FUNCgcd,
       SQLFunc::FUNClcm,
       SQLFunc::FUNCmax2,
       SQLFunc::FUNCmin2,
       SQLFunc::FUNCicebergBucket,
       SQLFunc::FUNCicebergTruncate};

static const std::vector<SQLFunc> icebergFuncs = {SQLFunc::FUNCicebergBucket, SQLFunc::FUNCicebergTruncate};

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

void StatementGenerator::entryOrConstant(RandomGenerator & rg, const ColumnPathChain & entry, Expr * expr)
{
    if (rg.nextBool())
    {
        columnPathRef(entry, expr);
    }
    else
    {
        expr->mutable_lit_val()->mutable_int_lit()->set_uint_lit(rg.nextRandomUInt32() % (rg.nextBool() ? 1024 : 65536));
    }
}

void StatementGenerator::colRefOrExpression(
    RandomGenerator & rg, const SQLRelation & rel, const SQLBase & b, const ColumnPathChain & entry, Expr * expr)
{
    SQLType * tp = entry.getBottomType();
    const uint32_t datetime_func = 15
        * static_cast<uint32_t>(hasType<DateType>(false, true, false, tp) || hasType<TimeType>(false, true, false, tp)
                                || hasType<DateTimeType>(false, true, false, tp));
    const uint32_t modulo_func = 15 * static_cast<uint32_t>(hasType<IntType>(true, true, false, tp));
    const uint32_t one_arg_func = 5;
    const uint32_t hash_func = 10 * static_cast<uint32_t>(b.teng != SummingMergeTree);
    const uint32_t rand_expr = 15;
    const uint32_t rand_func = 5 * static_cast<uint32_t>(this->allow_not_deterministic);
    const uint32_t arithmetic_func = 5;
    const uint32_t col_ref = 40;
    const uint32_t prob_space = datetime_func + modulo_func + one_arg_func + hash_func + rand_expr + rand_func + arithmetic_func + col_ref;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (datetime_func && nopt < (datetime_func + 1))
    {
        /// Use date functions for partitioning/keys
        SQLFuncCall * func_call = expr->mutable_comp_expr()->mutable_func_call();

        func_call->mutable_func()->set_catalog_func(rg.pickRandomly(datesHash));
        columnPathRef(entry, func_call->add_args()->mutable_expr());
    }
    else if (modulo_func && nopt < (datetime_func + modulo_func + 1))
    {
        /// Use modulo function for partitioning/keys
        BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

        entryOrConstant(rg, entry, bexpr->mutable_lhs());
        bexpr->set_op(BinaryOperator::BINOP_PERCENT);
        entryOrConstant(rg, entry, bexpr->mutable_rhs());
    }
    else if (one_arg_func && nopt < (datetime_func + modulo_func + one_arg_func + 1))
    {
        /// Use any one arg function
        SQLFuncCall * func_call = expr->mutable_comp_expr()->mutable_func_call();

        func_call->mutable_func()->set_catalog_func(
            (b.isAnyIcebergEngine() && rg.nextBool()) ? SQLFunc::FUNCidentity
                                                      : static_cast<SQLFunc>(rg.pickRandomly(this->one_arg_funcs).fnum));
        columnPathRef(entry, func_call->add_args()->mutable_expr());
    }
    else if (hash_func && nopt < (datetime_func + modulo_func + one_arg_func + hash_func + 1))
    {
        /// Use hash
        SQLFuncCall * func_call = expr->mutable_comp_expr()->mutable_func_call();

        func_call->mutable_func()->set_catalog_func(rg.pickRandomly(multicolHash));
        columnPathRef(entry, func_call->add_args()->mutable_expr());
    }
    else if (rand_expr && nopt < (datetime_func + modulo_func + one_arg_func + hash_func + rand_expr + 1))
    {
        /// Use a random expression
        std::unordered_map<uint32_t, QueryLevel> levels_backup;
        std::unordered_map<uint32_t, std::unordered_map<String, SQLRelation>> ctes_backup;

        for (const auto & [key, val] : this->levels)
        {
            levels_backup[key] = val;
        }
        for (const auto & [key, val] : this->ctes)
        {
            ctes_backup[key] = val;
        }
        this->levels.clear();
        this->ctes.clear();
        this->levels[this->current_level] = QueryLevel(this->current_level);
        this->levels[this->current_level].rels.push_back(rel);
        generateTableExpression(rg, false, expr);
        this->levels.clear();
        this->ctes.clear();
        for (const auto & [key, val] : levels_backup)
        {
            this->levels[key] = val;
        }
        for (const auto & [key, val] : ctes_backup)
        {
            this->ctes[key] = val;
        }
    }
    else if (rand_func && nopt < (datetime_func + modulo_func + one_arg_func + hash_func + rand_expr + rand_func + 1))
    {
        /// Use random func
        expr->mutable_comp_expr()->mutable_func_call()->mutable_func()->set_catalog_func(SQLFunc::FUNCrand);
    }
    else if (
        arithmetic_func && nopt < (datetime_func + modulo_func + one_arg_func + hash_func + rand_expr + rand_func + arithmetic_func + 1))
    {
        /// Use arithmetic function
        SQLFuncCall * func_call = expr->mutable_comp_expr()->mutable_func_call();

        func_call->mutable_func()->set_catalog_func(
            rg.pickRandomly((b.isAnyIcebergEngine() && rg.nextBool()) ? icebergFuncs : arithmeticFuncs));
        entryOrConstant(rg, entry, func_call->add_args()->mutable_expr());
        entryOrConstant(rg, entry, func_call->add_args()->mutable_expr());
    }
    else if (
        col_ref && nopt < (datetime_func + modulo_func + one_arg_func + hash_func + rand_expr + rand_func + arithmetic_func + col_ref + 1))
    {
        /// Reference a column
        columnPathRef(entry, expr);
    }
    else
    {
        chassert(0);
    }
}

void StatementGenerator::generateTableKey(
    RandomGenerator & rg, const SQLRelation & rel, const SQLBase & b, const bool allow_asc_desc, TableKey * tkey)
{
    if (!entries.empty() && rg.nextSmallNumber() < 7)
    {
        if (rg.nextSmallNumber() < 3)
        {
            /// Generate a random key
            const uint32_t nkeys = (rg.nextMediumNumber() % UINT32_C(3)) + UINT32_C(1);

            for (uint32_t i = 0; i < nkeys; i++)
            {
                TableKeyExpr * tke = tkey->add_exprs();

                this->levels[this->current_level].rels.push_back(rel);
                generateTableExpression(rg, false, tke->mutable_expr());
                if (allow_asc_desc && rg.nextSmallNumber() < 3)
                {
                    tke->set_asc_desc(rg.nextBool() ? AscDesc::ASC : AscDesc::DESC);
                }
            }
        }
        else
        {
            const size_t ocols = (rg.nextLargeNumber() % std::min<size_t>(entries.size(), UINT32_C(3))) + 1;

            std::shuffle(entries.begin(), entries.end(), rg.generator);
            if (b.teng != SummingMergeTree && rg.nextSmallNumber() < 3)
            {
                /// Use a single expression for the entire table
                /// See https://github.com/ClickHouse/ClickHouse/issues/72043 for SummingMergeTree exception
                TableKeyExpr * tke = tkey->add_exprs();
                Expr * expr = tke->mutable_expr();
                SQLFuncCall * func_call = expr->mutable_comp_expr()->mutable_func_call();

                func_call->mutable_func()->set_catalog_func(rg.pickRandomly(multicolHash));
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

                    colRefOrExpression(rg, rel, b, this->entries[i], tke->mutable_expr());
                    if (allow_asc_desc && rg.nextSmallNumber() < 3)
                    {
                        tke->set_asc_desc(rg.nextBool() ? AscDesc::ASC : AscDesc::DESC);
                    }
                }
            }
        }
    }
}

template <typename T>
String StatementGenerator::setMergeTableParameter(RandomGenerator & rg, const String & initial)
{
    const uint32_t noption = rg.nextSmallNumber();

    if constexpr (std::is_same_v<T, std::shared_ptr<SQLDatabase>>)
    {
        if (collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && noption < 4)
        {
            const std::shared_ptr<SQLDatabase> & d = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));

            return initial + std::to_string(d->dname);
        }
    }
    else if constexpr (std::is_same_v<T, SQLTable>)
    {
        if (collectionHas<SQLTable>(attached_tables) && noption < 4)
        {
            const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(attached_tables));

            return initial + std::to_string(t.tname);
        }
    }
    else if constexpr (std::is_same_v<T, SQLView>)
    {
        if (collectionHas<SQLView>(attached_views) && noption < 4)
        {
            const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(attached_views));

            return initial + std::to_string(v.tname);
        }
    }
    else if constexpr (std::is_same_v<T, SQLDictionary>)
    {
        if (collectionHas<SQLDictionary>(attached_dictionaries) && noption < 4)
        {
            const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(attached_dictionaries));

            return initial + std::to_string(d.tname);
        }
    }
    else
    {
        chassert(0);
    }
    if (noption < 7)
    {
        return initial + std::to_string(rg.nextSmallNumber() - 1) + ".*";
    }
    else if (noption < 10)
    {
        const uint32_t first = rg.nextSmallNumber() - 1;
        const uint32_t second = std::max(rg.nextSmallNumber() - 1, first);

        return fmt::format("{}[{}-{}].*", rg.nextBool() ? initial : "", first, second);
    }
    else if constexpr (std::is_same_v<T, std::shared_ptr<SQLDatabase>>)
    {
        return "default";
    }
    else
    {
        return initial + "0";
    }
}

void StatementGenerator::generateMergeTreeEngineDetails(
    RandomGenerator & rg, const SQLRelation & rel, const SQLBase & b, const bool add_pkey, TableEngine * te)
{
    if (rg.nextSmallNumber() < 6)
    {
        generateTableKey(rg, rel, b, b.peer_table != PeerTableDatabase::ClickHouse, te->mutable_order());
    }
    if (te->has_order() && add_pkey && rg.nextSmallNumber() < 5)
    {
        /// Pkey is a subset of order by
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
        generateTableKey(rg, rel, b, false, te->mutable_primary_key());
    }
    if (rg.nextBool())
    {
        generateTableKey(rg, rel, b, false, te->mutable_partition_by());
    }

    const int npkey = te->primary_key().exprs_size();
    if (npkey && !b.is_deterministic && rg.nextSmallNumber() < 5)
    {
        /// Try to add sample key
        chassert(this->ids.empty());
        for (const auto & entry : this->entries)
        {
            IntType * itp = nullptr;
            SQLType * tp = entry.getBottomType();

            if ((itp = dynamic_cast<IntType *>(tp)) && itp->is_unsigned)
            {
                const TableKey & tpk = te->primary_key();

                /// Must be in pkey
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
                            const String & col = i == 0 ? oecol.path().col().column() : oecol.path().sub_cols(i - UINT32_C(1)).column();

                            ok &= col == entry.path[i].cname;
                        }
                        if (ok)
                        {
                            this->filtered_entries.emplace_back(std::ref<const ColumnPathChain>(entry));
                            break;
                        }
                    }
                }
            }
        }
        if (!this->filtered_entries.empty())
        {
            TableKey * tkey = te->mutable_sample_by();
            const size_t ncols = (rg.nextLargeNumber() % std::min<size_t>(this->filtered_entries.size(), UINT32_C(3))) + 1;

            std::shuffle(this->filtered_entries.begin(), this->filtered_entries.end(), rg.generator);
            for (size_t i = 0; i < ncols; i++)
            {
                columnPathRef(this->filtered_entries[i].get(), tkey->add_exprs()->mutable_expr());
            }
            this->filtered_entries.clear();
        }
    }
    if (te->has_engine() && b.isReplicatedOrSharedMergeTree() && rg.nextSmallNumber() < 8)
    {
        /// Replicated table params must come first when set
        std::vector<TableEngineParam> temp_params;

        for (const auto & item : te->params())
        {
            temp_params.emplace_back(item);
        }
        te->clear_params();
        te->add_params()->set_svalue("/clickhouse/tables/{shard}/{database}/{table}");
        te->add_params()->set_svalue("{replica}");
        for (const auto & item : temp_params)
        {
            *te->add_params() = item;
        }
    }
    if (te->has_engine() && (b.teng == SummingMergeTree || b.teng == CoalescingMergeTree) && rg.nextSmallNumber() < 4)
    {
        /// Optional list of columns to be summed
        ColumnPathList * clist = te->add_params()->mutable_col_list();
        const size_t ncols = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(entries.size()), UINT32_C(4))) + 1;

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        for (size_t i = 0; i < ncols; i++)
        {
            columnPathRef(entries[i], i == 0 ? clist->mutable_col() : clist->add_other_cols());
        }
    }
}

void StatementGenerator::setClusterInfo(RandomGenerator & rg, SQLBase & b) const
{
    /// Don't use on CLUSTER with ReplicatedMergeTrees or SharedMergeTrees
    if (!fc.clusters.empty() && !b.isSharedMergeTree() && (!b.db || !b.db->isSharedDatabase()) && (b.db || !supports_cloud_features)
        && rg.nextSmallNumber() < (b.toption.has_value() ? 9 : 5))
    {
        if (b.db && b.db->cluster.has_value() && rg.nextSmallNumber() < 9)
        {
            b.cluster = b.db->cluster;
        }
        else
        {
            b.cluster = rg.pickRandomly(fc.clusters);
        }
    }
}

void StatementGenerator::setRandomShardKey(RandomGenerator & rg, const std::optional<SQLTable> & t, Expr * expr)
{
    if (this->allow_not_deterministic && rg.nextMediumNumber() < 26)
    {
        /// Use random sharding key sometimes
        expr->mutable_comp_expr()->mutable_func_call()->mutable_func()->set_catalog_func(SQLFunc::FUNCrand);
    }
    else if (t.has_value())
    {
        const SQLTable & tt = t.value();

        flatTableColumnPath(
            to_remote_entries | flat_tuple | flat_nested | flat_json | collect_generated, tt.cols, [](const SQLColumn &) { return true; });
        const ColumnPathChain entry = rg.pickRandomly(this->remote_entries);
        this->remote_entries.clear();
        colRefOrExpression(rg, createTableRelation(rg, true, "", tt), tt, entry, expr);
    }
    else
    {
        expr->mutable_lit_val()->set_no_quote_str("c" + std::to_string(rg.randomInt<uint32_t>(0, (fc.max_columns - 1))));
    }
}

String StatementGenerator::getTableStructure(RandomGenerator & rg, const SQLTable & t, const bool escape)
{
    String buf;
    bool first = true;
    const bool allCols = this->allow_not_deterministic && rg.nextSmallNumber() < 4;

    flatTableColumnPath(to_remote_entries, t.cols, [&](const SQLColumn & c) { return allCols || c.canBeInserted(); });
    std::shuffle(this->remote_entries.begin(), this->remote_entries.end(), rg.generator);
    for (const auto & entry : this->remote_entries)
    {
        buf += fmt::format(
            "{}{} {}{}",
            first ? "" : ", ",
            entry.getBottomName(),
            entry.getBottomType()->typeName(escape, false),
            entry.nullable.has_value() ? (entry.nullable.value() ? " NULL" : " NOT NULL") : "");
        first = false;
    }
    this->remote_entries.clear();
    return buf;
}

void StatementGenerator::generateEngineDetails(
    RandomGenerator & rg, const SQLRelation & rel, SQLBase & b, const bool add_pkey, TableEngine * te)
{
    SettingValues * svs = nullptr;
    const bool has_tables = collectionHas<SQLTable>(hasTableOrView<SQLTable>(b));
    const bool has_views = collectionHas<SQLView>(hasTableOrView<SQLView>(b));
    const bool has_dictionaries = collectionHas<SQLDictionary>(hasTableOrView<SQLDictionary>(b));
    const bool allow_shared_tbl = supports_cloud_features && (fc.engine_mask & allow_shared) != 0;

    /// Set what the filename is going to be first
    b.setTablePath(rg, fc, connections.hasDolorConnection());
    if (b.isMergeTreeFamily())
    {
        if (te->has_engine() && (((fc.engine_mask & allow_replicated) != 0) || allow_shared_tbl) && rg.nextSmallNumber() < 4)
        {
            chassert(this->ids.empty());
            if ((fc.engine_mask & allow_replicated) != 0)
            {
                this->ids.emplace_back(TReplicated);
            }
            if (allow_shared_tbl)
            {
                this->ids.emplace_back(TShared);
            }
            b.toption = static_cast<TableEngineOption>(rg.pickRandomly(this->ids));
            te->set_toption(b.toption.value());
            this->ids.clear();
        }
        generateMergeTreeEngineDetails(rg, rel, b, add_pkey, te);
    }
    else if (te->has_engine() && b.isFileEngine())
    {
        te->add_params()->set_in_out(b.file_format.value());
        te->add_params()->set_svalue(b.getTablePath(rg, fc, true));
        if (b.file_comp.has_value())
        {
            te->add_params()->set_svalue(b.file_comp.value());
        }
    }
    else if (te->has_engine() && b.isJoinEngine())
    {
        const size_t ncols = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(entries.size()), UINT32_C(3))) + 1;
        std::uniform_int_distribution<uint32_t> join_type_range(1, static_cast<uint32_t>(J_FULL));
        const JoinType jt = static_cast<JoinType>(join_type_range(rg.generator));

        te->add_params()->set_join_const(rg.pickRandomly(StatementGenerator::joinMappings.at(jt)));
        te->add_params()->set_join_op(jt);

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        for (size_t i = 0; i < ncols; i++)
        {
            columnPathRef(entries[i], te->add_params()->mutable_cols());
        }
    }
    else if (
        te->has_engine()
        && (b.isMySQLEngine() || b.isPostgreSQLEngine() || b.isMaterializedPostgreSQLEngine() || b.isSQLiteEngine() || b.isMongoDBEngine()
            || b.isRedisEngine() || b.isExternalDistributedEngine()))
    {
        if (SQLTable * t = dynamic_cast<SQLTable *>(&b))
        {
            connections.createExternalDatabaseTable(rg, *t, entries, te);
        }
    }
    else if (te->has_engine() && b.isMergeEngine())
    {
        String mergeDesc;
        const uint32_t nopt2 = rg.nextSmallNumber();

        te->add_params()->set_regexp(setMergeTableParameter<std::shared_ptr<SQLDatabase>>(rg, "d"));
        if (nopt2 < 3)
        {
            mergeDesc = setMergeTableParameter<SQLTable>(rg, "t");
        }
        else if (nopt2 < 5)
        {
            mergeDesc = setMergeTableParameter<SQLView>(rg, "v");
        }
        else
        {
            mergeDesc = setMergeTableParameter<SQLDictionary>(rg, "d");
        }
        te->add_params()->set_svalue(std::move(mergeDesc));
    }
    else if (te->has_engine() && (b.isDistributedEngine() || b.isBufferEngine() || b.isAliasEngine()))
    {
        const uint32_t dist_table = 15 * static_cast<uint32_t>(has_tables);
        const uint32_t dist_view = 5 * static_cast<uint32_t>(has_views);
        const uint32_t dist_dictionary = 5 * static_cast<uint32_t>(has_dictionaries);
        const uint32_t prob_space = dist_table + dist_view + dist_dictionary;
        std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
        const uint32_t nopt = next_dist(rg.generator);

        if (b.isDistributedEngine())
        {
            te->add_params()->set_svalue(rg.pickRandomly(fc.clusters));
        }
        if (dist_table && nopt < (dist_table + 1))
        {
            const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(hasTableOrView<SQLTable>(b)));

            t.setName(te);
            /// For the sharding key
            b.sub = t.teng;
        }
        else if (dist_view && nopt < (dist_table + dist_view + 1))
        {
            const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(hasTableOrView<SQLView>(b)));

            v.setName(te);
            b.sub = v.teng;
        }
        else if (dist_dictionary && nopt < (dist_table + dist_view + dist_dictionary + 1))
        {
            const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(hasTableOrView<SQLDictionary>(b)));

            d.setName(te);
            b.sub = d.teng;
        }
        else
        {
            chassert(0);
        }

        if (b.isDistributedEngine() && rg.nextBool())
        {
            /// Optional sharding key
            SQLTable * t = dynamic_cast<SQLTable *>(&b);

            setRandomShardKey(rg, t ? std::make_optional<SQLTable>(*t) : std::nullopt, te->add_params()->mutable_expr());
            /// Optional policy name
            if (!fc.storage_policies.empty() && rg.nextBool())
            {
                te->add_params()->set_svalue(rg.pickRandomly(fc.storage_policies));
            }
        }
        else if (b.isBufferEngine())
        {
            /// num_layers
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 101));
            /// min_time, max_time, min_rows, max_rows, min_bytes, max_bytes
            for (int i = 0; i < 6; i++)
            {
                te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
            }
            if (rg.nextSmallNumber() < 7)
            {
                /// flush_time
                te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 61));
            }
            if (rg.nextSmallNumber() < 7)
            {
                /// flush_rows
                te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
            }
            if (rg.nextSmallNumber() < 7)
            {
                /// flush_bytes
                te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
            }
        }
    }
    else if (te->has_engine() && b.isDictionaryEngine())
    {
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(hasTableOrView<SQLDictionary>(b)));

        d.setName(te->add_params()->mutable_est(), false);
    }
    else if (te->has_engine() && b.isGenerateRandomEngine())
    {
        te->add_params()->set_num(rg.nextRandomUInt64());
        if (rg.nextBool())
        {
            std::uniform_int_distribution<uint32_t> string_length_dist(0, fc.max_string_length);
            std::uniform_int_distribution<uint64_t> nested_rows_dist(fc.min_nested_rows, fc.max_nested_rows);

            te->add_params()->set_num(string_length_dist(rg.generator));
            te->add_params()->set_num(nested_rows_dist(rg.generator));
        }
    }
    else if (te->has_engine() && b.isURLEngine())
    {
        if (SQLTable * t = dynamic_cast<SQLTable *>(&b))
        {
            connections.createExternalDatabaseTable(rg, *t, entries, te);
        }
        if (b.file_format.has_value())
        {
            te->add_params()->set_in_out(b.file_format.value());
        }
        if (b.file_comp.has_value())
        {
            te->add_params()->set_svalue(b.file_comp.value());
        }
    }
    else if (te->has_engine() && b.isKeeperMapEngine())
    {
        te->add_params()->set_svalue(b.getTablePath(rg, fc, true));
        if (rg.nextBool())
        {
            std::uniform_int_distribution<uint64_t> keys_limit_dist(0, 8192);

            te->add_params()->set_num(keys_limit_dist(rg.generator));
        }
    }
    else if (te->has_engine() && (b.isAnyIcebergEngine() || b.isAnyDeltaLakeEngine() || b.isAnyS3Engine() || b.isAnyAzureEngine()))
    {
        const bool prev_allow_not_deterministic = this->allow_not_deterministic;

        if (b.integration != IntegrationCall::None)
        {
            if (SQLTable * t = dynamic_cast<SQLTable *>(&b))
            {
                connections.createExternalDatabaseTable(rg, *t, entries, te);
            }
        }
        else
        {
            chassert(b.isOnLocal());
            te->add_params()->set_rvalue("local");
        }
        this->allow_not_deterministic = false;
        setObjectStoreParams<SQLBase, TableEngine>(rg, b, te);
        this->allow_not_deterministic = prev_allow_not_deterministic;
    }
    else if (te->has_engine() && b.isArrowFlightEngine())
    {
        /// Set arrow flight params
        b.host_params = rg.pickRandomly(fc.arrow_flight_servers);
        te->add_params()->set_svalue(b.host_params.value());
        te->add_params()->set_svalue(b.getTablePath(rg, fc, true));
    }

    if (te->has_engine() && (b.isJoinEngine() || b.isSetEngine()) && allow_shared_tbl && rg.nextSmallNumber() < 5)
    {
        b.toption = TShared;
        te->set_toption(b.toption.value());
    }
    if (te->has_engine() && (b.isRocksEngine() || b.isRedisEngine() || b.isKeeperMapEngine() || b.isMaterializedPostgreSQLEngine())
        && add_pkey && !entries.empty())
    {
        colRefOrExpression(rg, rel, b, rg.pickRandomly(entries), te->mutable_primary_key()->add_exprs()->mutable_expr());
    }
    if (te->has_engine() && b.has_partition_by)
    {
        /// Optional PARTITION BY
        generateTableKey(rg, rel, b, false, te->mutable_partition_by());
    }
    if (te->has_engine())
    {
        const auto & engineSettings = allTableSettings.at(b.teng);

        if (!engineSettings.empty() && rg.nextBool())
        {
            /// Add table engine settings
            svs = svs ? svs : te->mutable_setting_values();
            generateSettingValues(rg, engineSettings, svs);
        }
        if (rg.nextSmallNumber() < 4)
        {
            /// Add server settings
            svs = svs ? svs : te->mutable_setting_values();
            generateSettingValues(rg, serverSettings, svs);
        }
        if (b.isMergeTreeFamily() && rg.nextMediumNumber() < 26)
        {
            /// Use wide and vertical merge settings more often
            static const DB::Strings & behaviorSettings
                = {"min_rows_for_wide_part",
                   "min_bytes_for_wide_part",
                   "vertical_merge_algorithm_min_rows_to_activate",
                   "vertical_merge_algorithm_min_bytes_to_activate",
                   "vertical_merge_algorithm_min_columns_to_activate",
                   "min_bytes_for_full_part_storage",
                   "min_rows_for_full_part_storage"};
            const size_t nsets = (rg.nextLargeNumber() % behaviorSettings.size()) + 1;

            chassert(this->ids.empty());
            for (size_t i = 0; i < behaviorSettings.size(); i++)
            {
                this->ids.emplace_back(i);
            }
            std::shuffle(this->ids.begin(), this->ids.end(), rg.generator);
            svs = svs ? svs : te->mutable_setting_values();
            for (size_t i = 0; i < nsets; i++)
            {
                SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

                sv->set_property(behaviorSettings[this->ids[i]]);
                sv->set_value(rg.nextBool() ? "1" : "0");
            }
            this->ids.clear();
        }
        if (b.isAnyS3Engine() && rg.nextBool())
        {
            svs = svs ? svs : te->mutable_setting_values();
            SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

            sv->set_property("input_format_with_names_use_header");
            sv->set_value("0");
        }
        if (b.isS3QueueEngine() || b.isAzureQueueEngine())
        {
            /// The mode setting is mandatory
            svs = svs ? svs : te->mutable_setting_values();
            SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

            sv->set_property("mode");
            sv->set_value(fmt::format("'{}ordered'", rg.nextBool() ? "un" : ""));
        }
        if ((b.isMergeTreeFamily() || b.isLogFamily()) && (b.isSharedMergeTree() || rg.nextSmallNumber() < 3)
            && (!fc.storage_policies.empty() || !fc.keeper_disks.empty())
            && (!svs
                || (svs->set_value().property() != "storage_policy" && svs->set_value().property() != "disk"
                    && (!svs->other_values_size()
                        || std::find_if(
                               svs->other_values().begin(),
                               svs->other_values().end(),
                               [](const auto & val) { return val.property() == "storage_policy" || val.property() == "disk"; })
                            == svs->other_values().end()))))
        {
            svs = svs ? svs : te->mutable_setting_values();
            SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();
            const String & pick = (fc.keeper_disks.empty() || rg.nextSmallNumber() < 3) ? "storage_policy" : "disk";

            sv->set_property(pick);
            sv->set_value("'" + rg.pickRandomly(pick == "storage_policy" ? fc.storage_policies : fc.keeper_disks) + "'");
        }
    }
    setClusterInfo(rg, b);
}

void StatementGenerator::addTableColumnInternal(
    RandomGenerator & rg,
    SQLTable & t,
    const uint32_t cname,
    const bool modify,
    const bool is_pk,
    const ColumnSpecial special,
    SQLColumn & col,
    ColumnDef * cd)
{
    SQLType * tp = nullptr;

    col.cname = cname;
    cd->mutable_col()->mutable_col()->set_column("c" + std::to_string(cname));
    if (special == ColumnSpecial::SIGN || special == ColumnSpecial::IS_DELETED)
    {
        tp = new IntType(8, special == ColumnSpecial::IS_DELETED);
        cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_integers(
            special == ColumnSpecial::IS_DELETED ? Integers::UInt8 : Integers::Int8);
    }
    else if (special == ColumnSpecial::VERSION)
    {
        if (((this->next_type_mask & (allow_dates | allow_datetimes)) == 0) || rg.nextBool())
        {
            Integers nint;

            std::tie(tp, nint) = randomIntType(rg, this->next_type_mask);
            cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_integers(nint);
        }
        else if (((this->next_type_mask & allow_datetimes) == 0) || rg.nextBool())
        {
            Dates dd;

            std::tie(tp, dd) = randomDateType(rg, this->next_type_mask);
            cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_dates(dd);
        }
        else
        {
            tp = randomDateTimeType(
                rg, this->next_type_mask, cd->mutable_type()->mutable_type()->mutable_non_nullable()->mutable_datetimes());
        }
    }
    else
    {
        tp = randomNextType(rg, this->next_type_mask, t.col_counter, cd->mutable_type()->mutable_type());
    }
    delete col.tp;
    col.tp = tp;
    col.special = special;
    if (!modify && col.special == ColumnSpecial::NONE && tp->isNullable() && rg.nextSmallNumber() < 3)
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
        std::uniform_int_distribution<uint32_t> dmod_range(1, static_cast<uint32_t>(DModifier_MAX));
        DModifier dmod = static_cast<DModifier>(dmod_range(rg.generator));

        if (is_pk && dmod == DModifier::DEF_EPHEMERAL)
        {
            dmod = DModifier::DEF_DEFAULT;
        }
        def_value->set_dvalue(dmod);
        col.dmod = std::optional<DModifier>(dmod);
        if (dmod != DModifier::DEF_EPHEMERAL || rg.nextMediumNumber() < 21)
        {
            if (!t.cols.empty())
            {
                addTableRelation(rg, true, "", t);
            }
            generateTableExpression(rg, false, def_value->mutable_expr());
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
            generateSettingValues(rg, csettings, cd->mutable_setting_values());
        }
        if ((!col.dmod.has_value() || col.dmod.value() != DModifier::DEF_EPHEMERAL) && !t.is_deterministic && rg.nextMediumNumber() < 16)
        {
            flatTableColumnPath(0, t.cols, [](const SQLColumn & c) { return c.tp->getTypeClass() != SQLTypeClass::NESTED; });
            generateTTLExpression(rg, std::make_optional<SQLTable>(t), cd->mutable_ttl_expr());
            this->entries.clear();
        }
        cd->set_is_pkey(is_pk);
    }
    if (rg.nextSmallNumber() < 3)
    {
        cd->set_comment(nextComment(rg));
    }
}

void StatementGenerator::addTableColumn(
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
    auto & to_add = staged ? t.staged_cols : t.cols;
    const uint32_t type_mask_backup = this->next_type_mask;

    if ((t.isMySQLEngine() && (t.is_deterministic || rg.nextSmallNumber() < 4)) || t.hasMySQLPeer())
    {
        this->next_type_mask &= ~(
            allow_int128 | allow_dynamic | allow_JSON | allow_array | allow_map | allow_tuple | allow_variant | allow_nested | allow_geo
            | set_no_decimal_limit);
    }
    if ((t.isPostgreSQLEngine() && (t.is_deterministic || rg.nextSmallNumber() < 4)) || t.hasPostgreSQLPeer())
    {
        this->next_type_mask &= ~(
            allow_int128 | allow_unsigned_int | allow_dynamic | allow_JSON | allow_map | allow_tuple | allow_variant | allow_nested
            | allow_geo);
        if (t.hasPostgreSQLPeer())
        {
            /// Datetime must have 6 digits precision
            this->next_type_mask &= ~(set_any_datetime_precision);
        }
    }
    if ((t.isSQLiteEngine() && (t.is_deterministic || rg.nextSmallNumber() < 4)) || t.hasSQLitePeer())
    {
        this->next_type_mask &= ~(
            allow_int128 | allow_unsigned_int | allow_dynamic | allow_JSON | allow_array | allow_map | allow_tuple | allow_variant
            | allow_nested | allow_geo);
        if (t.hasSQLitePeer())
        {
            /// For bool it maps to int type, then it outputs 0 as default instead of false
            /// For decimal it prints as text
            this->next_type_mask &= ~(allow_bool | allow_decimals);
        }
    }
    if ((t.isMongoDBEngine() && (t.is_deterministic || rg.nextSmallNumber() < 4)))
    {
        this->next_type_mask &= ~(allow_dynamic | allow_map | allow_tuple | allow_variant | allow_nested);
    }
    if (t.hasDatabasePeer())
    {
        /// ClickHouse's UUID sorting order is different from other databases
        this->next_type_mask &= ~(allow_uuid);
    }
    addTableColumnInternal(rg, t, cname, modify, is_pk, special, col, cd);

    to_add[cname] = std::move(col);
    this->next_type_mask = type_mask_backup;
}

void StatementGenerator::addTableIndex(RandomGenerator & rg, SQLTable & t, const bool staged, IndexDef * idef)
{
    SQLIndex idx;
    const uint32_t iname = t.idx_counter++;
    Expr * expr = idef->mutable_expr();
    std::uniform_int_distribution<uint32_t> idx_range(1, static_cast<uint32_t>(IndexType_MAX));
    const IndexType itpe = static_cast<IndexType>(idx_range(rg.generator));
    auto & to_add = staged ? t.staged_idxs : t.idxs;

    chassert(!t.cols.empty());
    idx.iname = iname;
    idef->mutable_idx()->set_index("i" + std::to_string(iname));
    idef->set_type(itpe);
    if (itpe == IndexType::IDX_hypothesis && rg.nextSmallNumber() < 9)
    {
        flatTableColumnPath(
            flat_tuple | flat_nested | flat_json | skip_nested_node,
            t.cols,
            [&itpe](const SQLColumn & c)
            {
                return itpe < IndexType::IDX_vector_similarity
                    || (itpe == IndexType::IDX_vector_similarity && hasType<FloatType>(true, true, true, c.tp))
                    || (itpe > IndexType::IDX_vector_similarity && hasType<StringType>(true, true, true, c.tp));
            });
        if (entries.size() > 1)
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
            std::shuffle(entries.begin(), entries.end(), rg.generator);
            columnPathRef(this->entries[0], estc1->mutable_col()->mutable_path());
            columnPathRef(this->entries[1], estc2->mutable_col()->mutable_path());
        }
        this->entries.clear();
    }
    if (!expr->has_comp_expr())
    {
        flatTableColumnPath(flat_tuple | flat_nested | flat_json | skip_nested_node, t.cols, [](const SQLColumn &) { return true; });
        colRefOrExpression(rg, createTableRelation(rg, true, "", t), t, rg.pickRandomly(this->entries), expr);
        this->entries.clear();
    }
    switch (itpe)
    {
        case IndexType::IDX_set: {
            uint32_t param = 0;

            if (rg.nextSmallNumber() > 6)
            {
                std::uniform_int_distribution<uint32_t> next_dist(1, 8192);
                param = next_dist(rg.generator);
            }
            idef->add_params()->set_ival(param);
        }
        break;
        case IndexType::IDX_bloom_filter:
            if (rg.nextBool())
            {
                std::uniform_int_distribution<uint32_t> next_dist(1, 8192);
                idef->add_params()->set_dval(static_cast<double>(next_dist(rg.generator)) / static_cast<double>(8192));
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
        case IndexType::IDX_text: {
            static const DB::Strings & tokenizerVals = {"default", "ngram", "split", "no_op"};
            const String & next_tokenizer = rg.pickRandomly(tokenizerVals);

            idef->add_params()->set_unescaped_sval("tokenizer = '" + next_tokenizer + "'");
            if (next_tokenizer == "ngram" && rg.nextBool())
            {
                std::uniform_int_distribution<uint32_t> next_dist(2, 8);

                idef->add_params()->set_unescaped_sval("ngram_size = " + std::to_string(next_dist(rg.generator)));
            }
            if (next_tokenizer == "split" && rg.nextBool())
            {
                String buf;
                DB::Strings separators = {"叫", "😉", "a", "b", "c", ",", "\\\\", "\"", "\\'", "\\t", "\\n", " ", "1", "."};
                std::uniform_int_distribution<size_t> next_dist(UINT32_C(1), separators.size());

                std::shuffle(separators.begin(), separators.end(), rg.generator);
                const size_t nlen = next_dist(rg.generator);
                buf += "separators = [";
                for (size_t i = 0; i < nlen; i++)
                {
                    if (i != 0)
                    {
                        buf += ", ";
                    }
                    buf += "'";
                    buf += separators[i];
                    buf += "'";
                }
                buf += "]";
                idef->add_params()->set_unescaped_sval(std::move(buf));
            }
        }
        break;
        case IndexType::IDX_vector_similarity: {
            static const std::vector<uint32_t> & dimensionVals = {1, 1, 1, 2, 2, 2, 4, 8, 32, 64, 128};

            idef->add_params()->set_sval("hnsw");
            idef->add_params()->set_sval(rg.nextBool() ? "cosineDistance" : "L2Distance");
            idef->add_params()->set_ival(rg.pickRandomly(dimensionVals));
            if (rg.nextBool())
            {
                std::uniform_int_distribution<uint32_t> next_dist(0, 4194304);
                static const DB::Strings & QuantitizationVals = {"f64", "f32", "f16", "bf16", "i8", "b1"};

                idef->add_params()->set_sval(rg.pickRandomly(QuantitizationVals));
                idef->add_params()->set_ival(next_dist(rg.generator));
                idef->add_params()->set_ival(next_dist(rg.generator));
            }
        }
        break;
        case IndexType::IDX_minmax:
        case IndexType::IDX_hypothesis:
            break;
    }
    if (rg.nextSmallNumber() < 7)
    {
        uint32_t granularity = 1;
        const uint32_t next_opt = rg.nextSmallNumber();

        if (next_opt < 4)
        {
            std::uniform_int_distribution<uint32_t> next_dist(1, 4194304);
            granularity = next_dist(rg.generator);
        }
        else if (next_opt < 8)
        {
            granularity = UINT32_C(1) << (rg.nextLargeNumber() % 21);
        }
        idef->set_granularity(granularity);
    }
    to_add[iname] = std::move(idx);
}

void StatementGenerator::addTableProjection(RandomGenerator & rg, SQLTable & t, const bool staged, ProjectionDef * pdef)
{
    const uint32_t pname = t.proj_counter++;
    const uint32_t ncols = std::max(std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % UINT32_C(3)) + 1), UINT32_C(1));
    auto & to_add = staged ? t.staged_projs : t.projs;

    pdef->mutable_proj()->set_projection("p" + std::to_string(pname));
    this->inside_projection = true;
    if (!t.cols.empty())
    {
        addTableRelation(rg, true, "", t);
    }
    generateSelect(rg, true, false, ncols, allow_groupby | allow_orderby, std::nullopt, pdef->mutable_select());
    this->levels.clear();
    this->inside_projection = false;
    to_add.insert(pname);
}

void StatementGenerator::addTableConstraint(RandomGenerator & rg, SQLTable & t, const bool staged, ConstraintDef * cdef)
{
    const uint32_t crname = t.constr_counter++;
    auto & to_add = staged ? t.staged_constrs : t.constrs;
    const bool prev_allow_in_expression_alias = this->allow_in_expression_alias;
    std::uniform_int_distribution<uint32_t> constr_range(1, static_cast<uint32_t>(ConstraintDef::ConstraintType_MAX));

    cdef->set_ctype(static_cast<ConstraintDef_ConstraintType>(constr_range(rg.generator)));
    cdef->mutable_constr()->set_constraint("c" + std::to_string(crname));
    if (!t.cols.empty())
    {
        addTableRelation(rg, true, "", t);
    }
    this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
    this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
    this->allow_in_expression_alias = rg.nextMediumNumber() < 11;
    this->generateWherePredicate(rg, cdef->mutable_expr());
    this->allow_in_expression_alias = prev_allow_in_expression_alias;
    this->levels.clear();
    to_add.insert(crname);
}

void StatementGenerator::getNextPeerTableDatabase(RandomGenerator & rg, SQLBase & b)
{
    chassert(this->ids.empty());
    if (b.is_deterministic && b.teng != Set && b.teng != ExternalDistributed)
    {
        if (b.teng != MySQL && connections.hasMySQLConnection())
        {
            this->ids.emplace_back(static_cast<uint32_t>(PeerTableDatabase::MySQL));
        }
        if (b.teng != PostgreSQL && b.teng != MaterializedPostgreSQL && connections.hasPostgreSQLConnection())
        {
            this->ids.emplace_back(static_cast<uint32_t>(PeerTableDatabase::PostgreSQL));
        }
        if (b.teng != SQLite && connections.hasSQLiteConnection())
        {
            this->ids.emplace_back(static_cast<uint32_t>(PeerTableDatabase::SQLite));
        }
        if (b.teng >= MergeTree && b.teng <= VersionedCollapsingMergeTree && connections.hasClickHouseExtraServerConnection())
        {
            this->ids.emplace_back(static_cast<uint32_t>(PeerTableDatabase::ClickHouse));
            this->ids.emplace_back(static_cast<uint32_t>(PeerTableDatabase::ClickHouse)); // give more probability
        }
    }
    b.peer_table
        = (this->ids.empty() || rg.nextBool()) ? PeerTableDatabase::None : static_cast<PeerTableDatabase>(rg.pickRandomly(this->ids));
    this->ids.clear();
}

void StatementGenerator::getNextTableEngine(RandomGenerator & rg, bool use_external_integrations, SQLBase & b)
{
    /// Make sure `is_determistic is already set`
    const uint32_t noption = rg.nextSmallNumber();
    const LakeStorage storage = b.getPossibleLakeStorage();
    const LakeFormat format = b.getPossibleLakeFormat();

    if (noption < 3)
    {
        b.teng = MergeTree;
        return;
    }

    chassert(this->ids.empty());
    this->ids.emplace_back(MergeTree);
    if ((fc.engine_mask & allow_replacing_mergetree) != 0)
    {
        this->ids.emplace_back(ReplacingMergeTree);
    }
    if ((fc.engine_mask & allow_coalescing_mergetree) != 0)
    {
        this->ids.emplace_back(CoalescingMergeTree);
    }
    if ((fc.engine_mask & allow_summing_mergetree) != 0)
    {
        this->ids.emplace_back(SummingMergeTree);
    }
    if ((fc.engine_mask & allow_aggregating_mergetree) != 0)
    {
        this->ids.emplace_back(AggregatingMergeTree);
    }
    if ((fc.engine_mask & allow_collapsing_mergetree) != 0)
    {
        this->ids.emplace_back(CollapsingMergeTree);
    }
    if ((fc.engine_mask & allow_versioned_collapsing_mergetree) != 0)
    {
        this->ids.emplace_back(VersionedCollapsingMergeTree);
    }
    if (noption < 6)
    {
        b.teng = static_cast<TableEngineValues>(rg.pickRandomly(this->ids));
        this->ids.clear();
        return;
    }
    const bool has_tables = collectionHas<SQLTable>(hasTableOrView<SQLTable>(b));
    const bool has_views = collectionHas<SQLView>(hasTableOrView<SQLView>(b));
    const bool has_dictionaries = collectionHas<SQLDictionary>(hasTableOrView<SQLDictionary>(b));
    const bool allow_mysql_tbl = connections.hasMySQLConnection() && (fc.engine_mask & allow_mysql) != 0;
    const bool allow_postgresql_tbl = connections.hasPostgreSQLConnection() && (fc.engine_mask & allow_postgresql) != 0;

    if ((fc.engine_mask & allow_file) != 0)
    {
        this->ids.emplace_back(File);
    }
    if ((fc.engine_mask & allow_null) != 0)
    {
        this->ids.emplace_back(Null);
    }
    if ((fc.engine_mask & allow_setengine) != 0)
    {
        this->ids.emplace_back(Set);
    }
    if ((fc.engine_mask & allow_join) != 0)
    {
        this->ids.emplace_back(Join);
    }
    if ((fc.engine_mask & allow_stripelog) != 0)
    {
        this->ids.emplace_back(StripeLog);
    }
    if ((fc.engine_mask & allow_log) != 0)
    {
        this->ids.emplace_back(Log);
    }
    if ((fc.engine_mask & allow_tinylog) != 0)
    {
        this->ids.emplace_back(TinyLog);
    }
    if ((fc.engine_mask & allow_embedded_rocksdb) != 0)
    {
        this->ids.emplace_back(EmbeddedRocksDB);
    }
    if (storage == LakeStorage::All || storage == LakeStorage::Local)
    {
        if (format != LakeFormat::DeltaLake && (fc.engine_mask & allow_icebergLocal) != 0)
        {
            this->ids.emplace_back(IcebergLocal);
        }
        if (format != LakeFormat::Iceberg && (fc.engine_mask & allow_deltalakelocal) != 0)
        {
            this->ids.emplace_back(DeltaLakeLocal);
        }
    }
    if (fc.allow_memory_tables && (fc.engine_mask & allow_memory) != 0)
    {
        this->ids.emplace_back(Memory);
    }
    if (!fc.keeper_map_path_prefix.empty() && (fc.engine_mask & allow_keepermap) != 0)
    {
        this->ids.emplace_back(KeeperMap);
    }
    if (!fc.arrow_flight_servers.empty() && (fc.engine_mask & allow_arrowflight) != 0)
    {
        this->ids.emplace_back(ArrowFlight);
    }
    if (has_tables || has_views || has_dictionaries)
    {
        if ((fc.engine_mask & allow_buffer) != 0)
        {
            this->ids.emplace_back(Buffer);
        }
        if (!fc.clusters.empty() && (fc.engine_mask & allow_distributed) != 0)
        {
            this->ids.emplace_back(Distributed);
        }
        if ((fc.engine_mask & allow_alias) != 0)
        {
            this->ids.emplace_back(Alias);
        }
    }
    if ((fc.engine_mask & allow_dictionary) != 0 && has_dictionaries)
    {
        this->ids.emplace_back(Dictionary);
    }
    if (!b.is_deterministic)
    {
        if ((fc.engine_mask & allow_merge) != 0)
        {
            this->ids.emplace_back(Merge);
        }
        if (fc.allow_infinite_tables && (fc.engine_mask & allow_generaterandom) != 0)
        {
            this->ids.emplace_back(GenerateRandom);
        }
    }
    if (use_external_integrations)
    {
        if (allow_mysql_tbl)
        {
            this->ids.emplace_back(MySQL);
        }
        if (connections.hasPostgreSQLConnection())
        {
            if (allow_postgresql_tbl)
            {
                this->ids.emplace_back(PostgreSQL);
            }
            if ((fc.engine_mask & allow_materialized_postgresql) != 0)
            {
                this->ids.emplace_back(MaterializedPostgreSQL);
            }
        }
        if (connections.hasSQLiteConnection() && (fc.engine_mask & allow_sqlite) != 0)
        {
            this->ids.emplace_back(SQLite);
        }
        if (connections.hasMongoDBConnection() && (fc.engine_mask & allow_mongodb) != 0)
        {
            this->ids.emplace_back(MongoDB);
        }
        if (connections.hasRedisConnection() && (fc.engine_mask & allow_redis) != 0)
        {
            this->ids.emplace_back(Redis);
        }
        if (connections.hasMinIOConnection())
        {
            if ((fc.engine_mask & allow_S3) != 0)
            {
                this->ids.emplace_back(S3);
            }
            if ((fc.engine_mask & allow_S3queue) != 0)
            {
                this->ids.emplace_back(S3Queue);
            }
            if (storage == LakeStorage::All || storage == LakeStorage::S3)
            {
                if (format != LakeFormat::DeltaLake && (fc.engine_mask & allow_icebergS3) != 0)
                {
                    this->ids.emplace_back(IcebergS3);
                }
                if (format != LakeFormat::Iceberg && (fc.engine_mask & allow_deltalakeS3) != 0)
                {
                    this->ids.emplace_back(DeltaLakeS3);
                }
            }
        }
        if (connections.hasAzuriteConnection())
        {
            if ((fc.engine_mask & allow_AzureBlobStorage) != 0)
            {
                this->ids.emplace_back(AzureBlobStorage);
            }
            if ((fc.engine_mask & allow_AzureQueue) != 0)
            {
                this->ids.emplace_back(AzureQueue);
            }
            if (storage == LakeStorage::All || storage == LakeStorage::Azure)
            {
                if (format != LakeFormat::DeltaLake && (fc.engine_mask & allow_icebergAzure) != 0)
                {
                    this->ids.emplace_back(IcebergAzure);
                }
                if (format != LakeFormat::Iceberg && (fc.engine_mask & allow_deltalakeAzure) != 0)
                {
                    this->ids.emplace_back(DeltaLakeAzure);
                }
            }
        }
        if (connections.hasHTTPConnection() && (fc.engine_mask & allow_URL) != 0)
        {
            this->ids.emplace_back(URL);
        }
        if (allow_mysql_tbl || allow_postgresql_tbl)
        {
            this->ids.emplace_back(ExternalDistributed);
        }
    }

    b.teng = static_cast<TableEngineValues>(rg.pickRandomly(this->ids));
    this->ids.clear();
    if (b.isExternalDistributedEngine())
    {
        b.sub = (!allow_mysql_tbl || rg.nextBool()) ? PostgreSQL : MySQL;
    }
}

void StatementGenerator::generateNextCreateTable(RandomGenerator & rg, const bool in_parallel, CreateTable * ct)
{
    SQLTable next;
    uint32_t tname = 0;
    bool added_pkey = false;
    TableEngine * te = ct->mutable_engine();
    const bool prev_enforce_final = this->enforce_final;
    const bool prev_allow_not_deterministic = this->allow_not_deterministic;

    SQLBase::setDeterministic(rg, next);
    this->allow_not_deterministic = !next.is_deterministic;
    this->enforce_final = next.is_deterministic;
    next.is_temp = fc.allow_memory_tables && rg.nextMediumNumber() < 11;
    ct->set_is_temp(next.is_temp);

    const auto tableLikeLambda
        = [&next](const SQLTable & t) { return t.isAttached() && !t.is_temp && (t.is_deterministic || !next.is_deterministic); };
    const auto replaceTableLambda
        = [&next](const SQLTable & t) { return t.isAttached() && !t.hasDatabasePeer() && (t.is_deterministic || !next.is_deterministic); };
    const bool replace = collectionCount<SQLTable>(replaceTableLambda) > 3 && rg.nextMediumNumber() < 16;
    if (replace)
    {
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(replaceTableLambda));

        next.db = t.db;
        tname = next.tname = t.tname;
    }
    else
    {
        if (!next.is_temp && collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && rg.nextSmallNumber() < 9)
        {
            next.db = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    ct->set_create_opt(replace ? CreateReplaceOption::Replace : CreateReplaceOption::Create);
    next.setName(ct->mutable_est(), false);
    if (!collectionHas<SQLTable>(tableLikeLambda) || rg.nextSmallNumber() < 9)
    {
        /// Create table with definition
        TableDef * colsdef = ct->mutable_table_def();

        getNextTableEngine(rg, !in_parallel, next);
        te->set_engine(next.teng);
        if (!in_parallel)
        {
            getNextPeerTableDatabase(rg, next);
        }
        added_pkey
            |= (!next.isMergeTreeFamily() && !next.isRocksEngine() && !next.isKeeperMapEngine() && !next.isRedisEngine()
                && !next.isMaterializedPostgreSQLEngine());
        const bool add_version_to_replacing
            = next.teng == ReplacingMergeTree && !next.hasPostgreSQLPeer() && !next.hasSQLitePeer() && rg.nextSmallNumber() < 4;
        uint32_t added_cols = 0;
        uint32_t added_idxs = 0;
        uint32_t added_projs = 0;
        uint32_t added_consts = 0;
        uint32_t added_sign = 0;
        uint32_t added_is_deleted = 0;
        uint32_t added_version = 0;
        const uint32_t to_addcols = (rg.nextLargeNumber() % fc.max_columns) + UINT32_C(1);
        const uint32_t to_addidxs
            = ((rg.nextMediumNumber() % 4) + UINT32_C(1)) * static_cast<uint32_t>(next.isMergeTreeFamily() && rg.nextSmallNumber() < 4);
        const uint32_t to_addprojs
            = ((rg.nextMediumNumber() % 3) + UINT32_C(1)) * static_cast<uint32_t>(next.isMergeTreeFamily() && rg.nextSmallNumber() < 5);
        const uint32_t to_addconsts = ((rg.nextMediumNumber() % 3) + UINT32_C(1)) * static_cast<uint32_t>(rg.nextSmallNumber() < 3);
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
            TableDefItem * ndef = colsdef->add_table_defs();

            if (add_idx && nopt < (add_idx + 1))
            {
                addTableIndex(rg, next, false, ndef->mutable_idx_def());
                added_idxs++;
            }
            else if (add_proj && nopt < (add_idx + add_proj + 1))
            {
                addTableProjection(rg, next, false, ndef->mutable_proj_def());
                added_projs++;
            }
            else if (add_const && nopt < (add_idx + add_proj + add_const + 1))
            {
                addTableConstraint(rg, next, false, ndef->mutable_const_def());
                added_consts++;
            }
            else if (add_col && nopt < (add_idx + add_proj + add_const + add_col + 1))
            {
                const bool add_pkey = !added_pkey && rg.nextMediumNumber() < 4;

                addTableColumn(rg, next, next.col_counter++, false, false, add_pkey, ColumnSpecial::NONE, ndef->mutable_col_def());
                added_pkey |= add_pkey;
                added_cols++;
            }
            else
            {
                const uint32_t cname = next.col_counter++;
                const bool add_pkey = !added_pkey && rg.nextMediumNumber() < 4;
                const bool add_version_col = add_version && nopt < (add_idx + add_proj + add_const + add_col + add_version + 1);

                addTableColumn(
                    rg,
                    next,
                    cname,
                    false,
                    false,
                    add_pkey,
                    add_version_col ? ColumnSpecial::VERSION : (add_sign ? ColumnSpecial::SIGN : ColumnSpecial::IS_DELETED),
                    ndef->mutable_col_def());
                added_pkey |= add_pkey;
                te->add_params()->mutable_cols()->mutable_col()->set_column("c" + std::to_string(cname));
                if (add_version_col)
                {
                    added_version++;
                }
                else if (add_sign)
                {
                    chassert(!add_is_deleted);
                    added_sign++;
                }
                else
                {
                    chassert(add_is_deleted);
                    added_is_deleted++;
                }
            }
        }
        if (rg.nextSmallNumber() < 2)
        {
            CreateTableSelect * cts = ct->mutable_as_select_stmt();

            cts->set_paren(rg.nextSmallNumber() < 9);
            cts->set_empty(rg.nextSmallNumber() < 3);
            this->levels[this->current_level] = QueryLevel(this->current_level);
            generateSelect(
                rg,
                true,
                false,
                static_cast<uint32_t>(next.numberOfInsertableColumns()),
                std::numeric_limits<uint32_t>::max(),
                std::nullopt,
                cts->mutable_select());
            this->levels.clear();
        }
    }
    else
    {
        /// Create table as
        CreateTableAs * cta = ct->mutable_table_as();
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(tableLikeLambda));
        const auto & toPick
            = next.is_deterministic ? likeEngsDeterministic : (fc.allow_infinite_tables ? likeEngsInfinite : likeEngsNotDeterministic);
        std::uniform_int_distribution<size_t> table_engine(0, toPick.size() - UINT32_C(1));
        TableEngineValues val = toPick[table_engine(rg.generator)];

        next.teng = val;
        te->set_engine(val);
        cta->set_clone(next.isMergeTreeFamily() && t.isMergeTreeFamily() && rg.nextBool());
        t.setName(cta->mutable_est(), false);
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

    flatTableColumnPath(flat_tuple | flat_nested | flat_json | skip_nested_node, next.cols, [](const SQLColumn &) { return true; });
    chassert(!next.cols.empty());
    generateEngineDetails(rg, createTableRelation(rg, true, "", next), next, !added_pkey, te);
    this->entries.clear();

    if (next.cluster.has_value())
    {
        ct->mutable_cluster()->set_cluster(next.cluster.value());
    }
    if (((next.isAnyIcebergEngine() && next.integration == IntegrationCall::Dolor) || next.isAliasEngine()
         || (next.projs.empty() && next.idxs.empty() && next.constrs.empty() && rg.nextMediumNumber() < 11)))
    {
        /// For Iceberg tables created from Spark, don't give table schema
        ct->clear_table_def();
    }
    if (next.hasDatabasePeer())
    {
        flatTableColumnPath(0, next.cols, [](const SQLColumn & c) { return c.canBeInserted(); });
        connections.createPeerTable(rg, next.peer_table, next, ct, entries);
        entries.clear();
    }
    else if (!next.is_deterministic && next.isMergeTreeFamily() && rg.nextBool())
    {
        flatTableColumnPath(0, next.cols, [](const SQLColumn & c) { return c.tp->getTypeClass() != SQLTypeClass::NESTED; });
        generateNextTTL(rg, std::make_optional<SQLTable>(next), te, te->mutable_ttl_expr());
        entries.clear();
    }

    this->enforce_final = prev_enforce_final;
    this->allow_not_deterministic = prev_allow_not_deterministic;
    chassert(!next.toption.has_value() || next.isMergeTreeFamily() || next.isJoinEngine() || next.isSetEngine());
    this->staged_tables[tname] = std::move(next);
}

void StatementGenerator::generateNextCreateDictionary(RandomGenerator & rg, CreateDictionary * cd)
{
    SQLDictionary next;
    uint32_t tname = 0;
    uint32_t col_counter = 0;
    const DictionaryLayouts & dl = rg.pickRandomly(allDictionaryLayoutSettings);
    const bool isRange = dl == COMPLEX_KEY_RANGE_HASHED || dl == RANGE_HASHED;
    /// Range requires 2 cols for min and max
    const uint32_t dictionary_ncols = std::max((rg.nextLargeNumber() % fc.max_columns) + UINT32_C(1), isRange ? UINT32_C(2) : UINT32_C(1));
    SettingValues * svs = nullptr;
    DictionaryLayout * layout = cd->mutable_layout();
    const uint32_t type_mask_backup = this->next_type_mask;
    const bool prev_enforce_final = this->enforce_final;
    const bool prev_allow_not_deterministic = this->allow_not_deterministic;

    SQLBase::setDeterministic(rg, next);
    this->allow_not_deterministic = !next.is_deterministic;
    this->enforce_final = next.is_deterministic;
    const auto replaceDictionaryLambda
        = [&next](const SQLDictionary & d) { return d.isAttached() && (d.is_deterministic || !next.is_deterministic); };
    const bool replace = collectionCount<SQLDictionary>(replaceDictionaryLambda) > 3 && rg.nextMediumNumber() < 16;
    if (replace)
    {
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(replaceDictionaryLambda));

        next.db = d.db;
        tname = next.tname = d.tname;
    }
    else
    {
        if (collectionHas<std::shared_ptr<SQLDatabase>>(attached_databases) && rg.nextSmallNumber() < 9)
        {
            next.db = rg.pickRandomly(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    cd->set_create_opt(replace ? CreateReplaceOption::Replace : CreateReplaceOption::Create);
    next.setName(cd->mutable_est(), false);

    const auto & dictionary_table_lambda
        = [&next](const SQLTable & t) { return t.isAttached() && (t.is_deterministic || !next.is_deterministic); };
    const auto & dictionary_view_lambda
        = [&next](const SQLView & v) { return v.isAttached() && (v.is_deterministic || !next.is_deterministic); };
    const auto & dictionary_dictionary_lambda
        = [&next](const SQLDictionary & v) { return v.isAttached() && (v.is_deterministic || !next.is_deterministic); };
    const bool has_table = collectionHas<SQLTable>(dictionary_table_lambda);
    const bool has_view = collectionHas<SQLView>(dictionary_view_lambda);
    const bool has_dictionary = collectionHas<SQLDictionary>(dictionary_dictionary_lambda);

    const uint32_t dict_table = 10 * static_cast<uint32_t>(has_table);
    const uint32_t dict_system_table = 5 * static_cast<uint32_t>(!systemTables.empty() && !next.is_deterministic);
    const uint32_t dict_view = 5 * static_cast<uint32_t>(has_view);
    const uint32_t dict_dict = 5 * static_cast<uint32_t>(has_dictionary);
    const uint32_t null_src = 2;
    const uint32_t prob_space = dict_table + dict_system_table + dict_view + dict_dict + null_src;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (dict_table && nopt < (dict_table + 1))
    {
        DictionarySourceDetails * dsd = cd->mutable_source()->mutable_source();
        const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(dictionary_table_lambda));

        if (t.isPostgreSQLEngine() && rg.nextSmallNumber() < 8)
        {
            ExprSchemaTable * est = dsd->mutable_est();
            const ServerCredentials & sc = fc.postgresql_server.value();

            est->mutable_database()->set_database(sc.database);
            est->mutable_table()->set_table(t.getTableName());
            dsd->set_host(sc.server_hostname);
            dsd->set_port(std::to_string(sc.port));
            dsd->set_user(sc.user);
            dsd->set_password(sc.password);
            dsd->set_source(DictionarySourceDetails::POSTGRESQL);
        }
        else if (t.isMySQLEngine() && rg.nextSmallNumber() < 8)
        {
            ExprSchemaTable * est = dsd->mutable_est();
            const ServerCredentials & sc = fc.mysql_server.value();

            est->mutable_database()->set_database(sc.database);
            est->mutable_table()->set_table(t.getTableName());
            dsd->set_host(sc.server_hostname);
            dsd->set_port(std::to_string(sc.mysql_port ? sc.mysql_port : sc.port));
            dsd->set_user(sc.user);
            dsd->set_password(sc.password);
            dsd->set_source(DictionarySourceDetails::MYSQL);
        }
        else if (t.isMongoDBEngine() && rg.nextSmallNumber() < 8)
        {
            ExprSchemaTable * est = dsd->mutable_est();
            const ServerCredentials & sc = fc.mongodb_server.value();

            est->mutable_database()->set_database(sc.database);
            est->mutable_table()->set_table(t.getTableName());
            dsd->set_host(sc.server_hostname);
            dsd->set_port(std::to_string(sc.port));
            dsd->set_user(sc.user);
            dsd->set_password(sc.password);
            dsd->set_source(DictionarySourceDetails::MONGODB);
        }
        else if (t.isRedisEngine() && rg.nextSmallNumber() < 8)
        {
            const ServerCredentials & sc = fc.redis_server.value();

            dsd->set_host(sc.server_hostname);
            dsd->set_port(std::to_string(sc.port));
            dsd->set_user(sc.user);
            dsd->set_password(sc.password);
            if (rg.nextBool())
            {
                dsd->set_redis_storage(
                    static_cast<DictionarySourceDetails_RedisStorageType>(
                        (rg.nextRandomUInt32() % static_cast<uint32_t>(DictionarySourceDetails::RedisStorageType_MAX)) + 1));
            }
            dsd->set_source(DictionarySourceDetails::REDIS);
        }
        else
        {
            t.setName(dsd->mutable_est(), false);
            dsd->set_source(DictionarySourceDetails::CLICKHOUSE);
        }
    }
    else if (dict_system_table && nopt < (dict_table + dict_system_table + 1))
    {
        DictionarySourceDetails * dsd = cd->mutable_source()->mutable_source();
        ExprSchemaTable * est = dsd->mutable_est();
        const auto & ntable = rg.pickRandomly(systemTables);

        est->mutable_database()->set_database(ntable.schema_name);
        est->mutable_table()->set_table(ntable.table_name);
        dsd->set_source(DictionarySourceDetails::CLICKHOUSE);
    }
    else if (dict_view && nopt < (dict_table + dict_system_table + dict_view + 1))
    {
        DictionarySourceDetails * dsd = cd->mutable_source()->mutable_source();
        const SQLView & v = rg.pickRandomly(filterCollection<SQLView>(dictionary_view_lambda));

        v.setName(dsd->mutable_est(), false);
        dsd->set_source(DictionarySourceDetails::CLICKHOUSE);
    }
    else if (dict_dict && nopt < (dict_table + dict_system_table + dict_view + dict_dict + 1))
    {
        DictionarySourceDetails * dsd = cd->mutable_source()->mutable_source();
        const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(dictionary_dictionary_lambda));

        d.setName(dsd->mutable_est(), false);
        dsd->set_source(DictionarySourceDetails::CLICKHOUSE);
    }
    else if (null_src && nopt < (dict_table + dict_system_table + dict_view + dict_dict + null_src + 1))
    {
        cd->mutable_source()->set_null_src(true);
    }
    else
    {
        chassert(0);
    }

    /// Set columns
    for (uint32_t i = 0; i < dictionary_ncols; i++)
    {
        SQLColumn col;
        DictionaryColumn * dc = i == 0 ? cd->mutable_col() : cd->add_other_cols();
        const uint32_t ncname = col_counter++;
        const bool prev_allow_in_expression_alias = this->allow_in_expression_alias;
        const bool prev_allow_subqueries = this->allow_subqueries;

        col.cname = ncname;
        dc->mutable_col()->set_column("c" + std::to_string(ncname));
        /// Many types are not allowed in dictionaries
        this->next_type_mask = fc.type_mask
            & ~(allow_JSON | allow_variant | allow_dynamic | allow_tuple | allow_low_cardinality | allow_map | allow_enum | allow_geo
                | allow_fixed_strings | allow_time);
        col.tp = randomNextType(rg, this->next_type_mask, col_counter, dc->mutable_type()->mutable_type());
        this->next_type_mask = type_mask_backup;

        next.cols[ncname] = std::move(col);
        addDictionaryRelation("", next);
        this->levels[this->current_level].allow_aggregates = rg.nextMediumNumber() < 11;
        this->levels[this->current_level].allow_window_funcs = rg.nextMediumNumber() < 11;
        this->allow_in_expression_alias = rg.nextMediumNumber() < 11;
        this->allow_subqueries = rg.nextMediumNumber() < 11;
        generateLiteralValue(rg, false, dc->mutable_default_val());
        if (rg.nextMediumNumber() < 21)
        {
            generateExpression(rg, dc->mutable_expression());
        }
        this->allow_in_expression_alias = prev_allow_in_expression_alias;
        this->allow_subqueries = prev_allow_subqueries;
        this->levels.clear();
        if (rg.nextSmallNumber() < 9)
        {
            dc->set_hierarchical(rg.nextBool());
        }
        dc->set_is_object_id(rg.nextMediumNumber() < 3);
    }
    setClusterInfo(rg, next);
    if (next.cluster.has_value())
    {
        cd->mutable_cluster()->set_cluster(next.cluster.value());
    }

    /// Layout properties
    const auto & layoutSettings = allDictionaryLayoutSettings.at(dl);
    layout->set_layout(dl);
    if (!layoutSettings.empty() && rg.nextSmallNumber() < 5)
    {
        svs = svs ? svs : layout->mutable_setting_values();
        generateSettingValues(rg, layoutSettings, svs);
    }
    if (dl == COMPLEX_KEY_SSD_CACHE || dl == SSD_CACHE)
    {
        /// needs path
        svs = svs ? svs : layout->mutable_setting_values();
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();
        const String ncache = "cache" + std::to_string(this->cache_counter++);
        const std::filesystem::path & nfile = fc.server_file_path / ncache;

        sv->set_property("PATH");
        sv->set_value("'" + nfile.generic_string() + "'");
    }
    else if (dl == COMPLEX_KEY_CACHE || dl == CACHE)
    {
        /// needs size_in_cells
        svs = svs ? svs : layout->mutable_setting_values();
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        sv->set_property("SIZE_IN_CELLS");
        sv->set_value(
            std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))));
    }

    /// Add Primary Key
    flatTableColumnPath(flat_tuple | flat_nested | flat_json | skip_nested_node, next.cols, [](const SQLColumn &) { return true; });
    const size_t kcols = dl == IP_TRIE ? 1 : ((rg.nextLargeNumber() % std::min<size_t>(entries.size(), UINT32_C(3))) + 1);
    std::shuffle(entries.begin(), entries.end(), rg.generator);
    TableKey * tkey = cd->mutable_primary_key();
    for (size_t i = 0; i < kcols; i++)
    {
        columnPathRef(this->entries[i], tkey->add_exprs()->mutable_expr());
    }
    if (isRange)
    {
        /// Range properties
        DictionaryRange * dr = cd->mutable_range();

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        columnPathRef(this->entries[0], dr->mutable_min());
        columnPathRef(this->entries[1], dr->mutable_max());
    }
    this->entries.clear();

    if (dl != COMPLEX_KEY_DIRECT && dl != DIRECT)
    {
        /// Lifetime properties
        DictionaryLifetime * life = cd->mutable_lifetime();
        static const std::vector<uint32_t> & lifeValues = {0, 1, 2, 10, 30, 60, 120};

        life->set_min(rg.pickRandomly(lifeValues));
        if (rg.nextBool())
        {
            life->set_max(rg.pickRandomly(lifeValues));
        }
    }

    if (rg.nextSmallNumber() < 3)
    {
        generateSettingValues(rg, serverSettings, cd->mutable_setting_values());
    }
    this->enforce_final = prev_enforce_final;
    this->allow_not_deterministic = prev_allow_not_deterministic;
    if (rg.nextSmallNumber() < 3)
    {
        cd->set_comment(nextComment(rg));
    }
    this->staged_dictionaries[tname] = std::move(next);
}

DatabaseEngineValues StatementGenerator::getNextDatabaseEngine(RandomGenerator & rg)
{
    chassert(this->ids.empty());
    this->ids.emplace_back(DAtomic);
    if (fc.allow_memory_tables && (fc.engine_mask & allow_memory) != 0)
    {
        this->ids.emplace_back(DMemory);
    }
    if ((fc.engine_mask & allow_replicated) != 0)
    {
        this->ids.emplace_back(DReplicated);
    }
    if (supports_cloud_features && (fc.engine_mask & allow_shared) != 0)
    {
        this->ids.emplace_back(DShared);
    }
    if (connections.hasAnyCatalog() && (fc.engine_mask & allow_datalakecatalog) != 0)
    {
        this->ids.emplace_back(DDataLakeCatalog);
    }
    const auto res = static_cast<DatabaseEngineValues>(rg.pickRandomly(this->ids));
    this->ids.clear();
    return res;
}

void StatementGenerator::generateNextCreateDatabase(RandomGenerator & rg, CreateDatabase * cd)
{
    SQLDatabase next;
    SettingValues * svs = nullptr;
    const uint32_t dname = this->database_counter++;
    DatabaseEngine * deng = cd->mutable_dengine();

    next.deng = this->getNextDatabaseEngine(rg);
    deng->set_engine(next.deng);
    if (!next.isSharedDatabase() && !fc.clusters.empty() && rg.nextSmallNumber() < 4)
    {
        next.cluster = rg.pickRandomly(fc.clusters);
        cd->mutable_cluster()->set_cluster(next.cluster.value());
    }
    next.dname = dname;
    next.setDatabasePath(rg, fc);
    next.finishDatabaseSpecification(deng);
    next.setName(cd->mutable_database());
    if (rg.nextSmallNumber() < 3)
    {
        cd->set_comment(nextComment(rg));
    }
    if (!next.isReplicatedOrSharedDatabase() && !next.isDataLakeCatalogDatabase() && rg.nextSmallNumber() < 4)
    {
        /// Add server settings
        svs = svs ? svs : cd->mutable_setting_values();
        generateSettingValues(rg, serverSettings, svs);
    }
    if ((next.isAtomicDatabase() || next.isOrdinaryDatabase()) && !fc.disks.empty() && rg.nextSmallNumber() < 4)
    {
        svs = svs ? svs : cd->mutable_setting_values();
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        sv->set_property("disk");
        sv->set_value("'" + rg.pickRandomly(fc.disks) + "'");
    }
    else if (next.isDataLakeCatalogDatabase())
    {
        svs = svs ? svs : cd->mutable_setting_values();
        connections.createExternalDatabase(rg, next, deng, svs);
    }
    this->staged_databases[dname] = std::make_shared<SQLDatabase>(std::move(next));
}

}
