#include <Client/BuzzHouse/Generator/QueryOracle.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>

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
    = {{OutFormat::OUT_CSV}, {OutFormat::OUT_TabSeparated}, {OutFormat::OUT_Values}};

static void finishSettings(SettingValues * svs)
{
    /// Wait for mutations to finish
    static const std::unordered_map<String, String> toSet
        = {{"alter_sync", "2"},
           {"apply_deleted_mask", "1"},
           {"apply_patch_parts", "1"},
           {"lightweight_deletes_sync", "2"},
           {"mutations_sync", "2"}};
    for (const auto & [key, val] : toSet)
    {
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        sv->set_property(key);
        sv->set_value(val);
    }
}

/// Correctness query oracle
/// SELECT COUNT(*) FROM <FROM_CLAUSE> WHERE <PRED>;
/// (GROUP BY / HAVING variant is TODO — see `combination` below)
void QueryOracle::generateCorrectnessTestFirstQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq1)
{
    TopSelect * ts = sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    Select * sel = ts->mutable_sel();
    SelectStatementCore * ssc = sel->mutable_select_core();
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
        gen.generateGroupBy(rg, 1, true, true, ssc);
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

    finishSettings(sel->mutable_setting_values());
    ts->set_format(OutFormat::OUT_CSV);
    /// If the file fails to be removed due to a legitimate way, the oracle will fail anyway
    const auto err = std::filesystem::remove(qcfile);
    UNUSED(err);
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

/// SELECT ifNull(SUM(PRED),0) FROM <FROM_CLAUSE>;
/// (or ifNull(SUM(PRED2),0) FROM <FROM_CLAUSE> WHERE <PRED1> GROUP BY … when sq1 has a GROUP BY,
///  but that path is currently unreachable because combination=0 in generateCorrectnessTestFirstQuery)
void QueryOracle::generateCorrectnessTestSecondQuery(SQLQuery & sq1, SQLQuery & sq2)
{
    TopSelect * ts = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    Select & sel1 = *sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select()->mutable_sel();
    SelectStatementCore & ssc1 = *sel1.mutable_select_core();
    Select * sel2 = ts->mutable_sel();
    SelectStatementCore * ssc2 = sel2->mutable_select_core();
    SQLFuncCall * sfc1 = ssc2->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call();
    SQLFuncCall * sfc2 = sfc1->add_args()->mutable_expr()->mutable_comp_expr()->mutable_func_call();

    sfc1->mutable_func()->set_catalog_func(FUNCifNull);
    sfc1->add_args()->mutable_expr()->mutable_lit_val()->mutable_special_val()->set_val(
        SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO);
    sfc2->mutable_func()->set_catalog_func(FUNCsum);

    ssc2->set_allocated_from(ssc1.release_from());
    if (ssc1.has_groupby())
    {
        ExprComparisonHighProbability & expr = *ssc1.mutable_groupby()->mutable_having_expr()->mutable_expr();

        sfc2->add_args()->set_allocated_expr(expr.release_expr());
        ssc2->set_allocated_groupby(ssc1.release_groupby());
        ssc2->set_allocated_where(ssc1.release_where());
        ssc2->set_allocated_pre_where(ssc1.release_pre_where());
    }
    else
    {
        ExprComparisonHighProbability & expr = *ssc1.mutable_where()->mutable_expr();

        sfc2->add_args()->set_allocated_expr(expr.release_expr());
    }
    sel2->set_allocated_setting_values(sel1.release_setting_values());
    ts->set_format(sq1.single_query().explain().inner_query().select().format());
    const auto err = std::filesystem::remove(qcfile);
    UNUSED(err);
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

/// Roundtrip oracle
/// Query 1: SELECT count() FROM <from_clause> WHERE col IS NOT NULL  (baseline: non-null rows)
/// Query 2: SELECT count() FROM <from_clause> WHERE roundtrip(col) = col  (must equal query 1)
///
/// Detects bugs where an encoding/encryption function fails to preserve data through a
/// round-trip: if roundtrip(col) != col for any non-null row the counts diverge.
///
/// Roundtrip predicates evaluate to NULL when col IS NULL (NULL = x is NULL, not TRUE),
/// so sq2's WHERE naturally excludes NULL rows just like sq1's IS NOT NULL filter.
/// This makes the oracle correct for Nullable columns without special-casing.
///
/// For non-String types the predicate wraps the value in `toString` so that hex/base64
/// functions always receive a String argument. When the column type is unknown (subquery
/// columns have tp == nullptr), `toString` is applied unconditionally.
void QueryOracle::generateRoundtripOracleQueries(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq1, SQLQuery & sq2)
{
    can_test_oracle_result = fc.compare_success_results;
    can_test_success = false; /// Don't compare query success, queries are different

    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.resetAliasCounter();
    gen.levels[gen.current_level] = QueryLevel(gen.current_level);

    TopSelect * ts1 = sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif1 = ts1->mutable_intofile();
    Select * sel1 = ts1->mutable_sel();
    SelectStatementCore * ssc1 = sel1->mutable_select_core();

    const auto u = gen.generateFromStatement(rg, std::numeric_limits<uint32_t>::max(), ssc1->mutable_from());
    UNUSED(u);

    /// Collect all columns from all available relations and pick one for the predicate
    String val;
    String col_ref;
    std::vector<const SQLRelationCol *> all_cols;

    for (const auto & rel : gen.levels[gen.current_level].rels)
        for (const auto & c : rel.cols)
            all_cols.push_back(&c);
    if (!all_cols.empty())
    {
        const SQLRelationCol & rel_col = *rg.pickRandomly(all_cols);

        /// Build a backtick-quoted SQL column reference from the SQLRelationCol
        if (!rel_col.rel_name.empty())
            col_ref = fmt::format("`{}`.", escapeSQLString(rel_col.rel_name, '`'));
        for (size_t i = 0; i < rel_col.path.size(); ++i)
        {
            if (i > 0)
                col_ref += ".";
            col_ref += "`" + escapeSQLString(rel_col.path[i], '`') + "`";
        }

        /// For String/FixedString: apply roundtrip directly.
        /// For all other types (or unknown type): wrap in `toString` so hex/base64 receive a String.
        const bool is_string = rel_col.tp != nullptr && rel_col.tp->getTypeClass() == SQLTypeClass::STRING;
        val = is_string ? col_ref : fmt::format("toString({})", col_ref);
    }
    else
    {
        col_ref = val = "1";
    }
    gen.levels.clear();
    gen.ctes.clear();

    /// Choose roundtrip function pair
    String roundtrip_pred;
    switch (rg.randomInt<uint32_t>(0, 3))
    {
        case 0:
            /// hex/unhex — exercises hex encoding path
            roundtrip_pred = fmt::format("unhex(hex({0})) = {0}", val);
            break;
        case 1:
            /// baseEncode/Decode
            roundtrip_pred = fmt::format("base{0}Decode(base{0}Encode({1})) = {1}", rg.nextBool() ? "58" : "64", val);
            break;
        case 2:
            /// reverse/reverseUTF8 — exercises byte and codepoint-aware string reversal (must use matching pair)
            {
                const String rev = rg.nextBool() ? "UTF8" : "";
                roundtrip_pred = fmt::format("reverse{0}(reverse{0}({1})) = {1}", rev, val);
            }
            break;
        default: {
            /// AES encrypt/decrypt — exercises all cipher modes, key sizes, and IV requirements
            struct CipherSpec
            {
                const char * name;
                uint32_t key_bytes; /// 16 = aes-128, 24 = aes-192, 32 = aes-256
                uint32_t iv_bytes; /// 0 = ECB (no IV), 16 = CBC/CFB128/OFB, 12 = GCM
            };
            static const std::vector<CipherSpec> ciphers = {
                {"aes-128-ecb", 16, 0},
                {"aes-192-ecb", 24, 0},
                {"aes-256-ecb", 32, 0},
                {"aes-128-cbc", 16, 16},
                {"aes-192-cbc", 24, 16},
                {"aes-256-cbc", 32, 16},
                {"aes-128-cfb128", 16, 16},
                {"aes-192-cfb128", 24, 16},
                {"aes-256-cfb128", 32, 16},
                {"aes-128-ofb", 16, 16},
                {"aes-192-ofb", 24, 16},
                {"aes-256-ofb", 32, 16},
                {"aes-128-gcm", 16, 12},
                {"aes-192-gcm", 24, 12},
                {"aes-256-gcm", 32, 12},
            };
            const CipherSpec & spec = rg.pickRandomly(ciphers);

            auto gen_hex = [&](uint32_t bytes) -> String
            {
                String hex;
                for (uint32_t i = 0; i < bytes; i++)
                    hex += fmt::format("{:02x}", rg.randomInt<uint8_t>(0, 255));
                return hex;
            };
            const String key_hex = gen_hex(spec.key_bytes);

            if (spec.iv_bytes == 0)
            {
                roundtrip_pred
                    = fmt::format("decrypt('{2}', encrypt('{2}', {0}, unhex('{1}')), unhex('{1}')) = {0}", val, key_hex, spec.name);
            }
            else
            {
                const String iv_hex = gen_hex(spec.iv_bytes);
                roundtrip_pred = fmt::format(
                    "decrypt('{3}', encrypt('{3}', {0}, unhex('{1}'), unhex('{2}')), unhex('{1}'), unhex('{2}')) = {0}",
                    val,
                    key_hex,
                    iv_hex,
                    spec.name);
            }
            break;
        }
    }

    /// Build sq1: SELECT count() FROM <from_clause> WHERE col IS NOT NULL  (baseline)
    ssc1->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call()->mutable_func()->set_catalog_func(
        FUNCcount);
    ssc1->mutable_where()->mutable_expr()->mutable_expr()->mutable_lit_val()->set_no_quote_str(fmt::format("{} IS NOT NULL", col_ref));
    finishSettings(sel1->mutable_setting_values());
    ts1->set_format(OutFormat::OUT_CSV);
    const auto err1 = std::filesystem::remove(qcfile);
    UNUSED(err1);
    sif1->set_path(qcfile.generic_string());
    sif1->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);

    /// Build sq2: SELECT count() FROM <from_clause> WHERE col IS NOT NULL AND roundtrip(col) = col
    /// The explicit IS NOT NULL guard is necessary because for Dynamic columns toString(NULL) returns ''
    /// rather than NULL, so the roundtrip predicate would evaluate to TRUE for NULL rows and diverge
    /// from sq1's IS NOT NULL baseline.  For Nullable columns this guard is redundant but harmless.
    /// CopyFrom clones the FROM clause, format, and output file from sq1.
    sq2.CopyFrom(sq1);
    {
        SelectStatementCore * ssc
            = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select()->mutable_sel()->mutable_select_core();
        ssc->mutable_where()->mutable_expr()->mutable_expr()->mutable_lit_val()->set_no_quote_str(
            fmt::format("{} IS NOT NULL AND {}", col_ref, roundtrip_pred));
    }
    gen.enforceFinal(false);
    gen.setAllowNotDetermistic(true);
}

/// Row policy correctness oracle.
///
/// Picks an existing catalog row policy (with a stored USING predicate) on a suitable table.
///
/// Query 1 (sq1):
///   SELECT count() FROM db.t [FINAL] INTO OUTFILE qcfile TRUNCATE FORMAT CSV
///   Run after "EXECUTE AS oracleUser" → row policy active → filtered count.
///
/// Query 2 (sq2):
///   SELECT count() FROM db.t [FINAL] WHERE pred INTO OUTFILE qcfile TRUNCATE FORMAT CSV
///   Run as admin (after reconnect) → explicit WHERE equivalent to policy filter.
///
/// The two counts must be equal: the policy USING pred must be semantically identical to
/// an explicit WHERE pred.  No setup or teardown — the policy already exists in the catalog.
void QueryOracle::generateRowPolicyOracleQueries(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq1, SQLQuery & sq2)
{
    can_test_oracle_result = fc.compare_success_results;
    /// Don't compare error codes: EXECUTE AS may introduce different failure modes
    can_test_success = false;

    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.resetAliasCounter();

    // ---- Build sq2: SELECT count() FROM db.t [FINAL] WHERE pred INTO OUTFILE ----
    // (run as admin with explicit WHERE predicate matching the policy's USING clause)
    TopSelect * ts2 = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    Select * sel2 = ts2->mutable_sel();
    SelectStatementCore * ssc2 = sel2->mutable_select_core();
    // FROM db.t [FINAL]
    JoinedTableOrFunction * jtf2 = ssc2->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();
    /// Pick an existing row policy with a USING predicate on a suitable table
    const SQLPolicy & policy = rg.pickRandomly(gen.filterCollection<SQLPolicy>(gen.row_policies_for_oracle));
    if (gen.hasTable(policy.table_key))
    {
        const SQLTable & t = gen.lookupTable(policy.table_key);

        t.setName(jtf2->mutable_tof()->mutable_est(), false);
        jtf2->set_final(t.supportsFinal());
    }
    else
    {
        /// The policy's table is gone — this can happen if the policy is detached but not dropped, or if the table was dropped without detaching the policy.
        /// In either case we can't generate a valid oracle query, so we build a dummy query that selects from system.one with a constant WHERE to produce a predictable result (1 row).
        jtf2->mutable_tof()->mutable_est()->mutable_database()->set_value("system");
        jtf2->mutable_tof()->mutable_est()->mutable_table()->set_value("one");
    }
    // count() result column
    ssc2->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call()->mutable_func()->set_catalog_func(
        FUNCcount);
    // WHERE pred — copied from the stored USING predicate of the catalog policy.
    // In the fallback path (table gone, querying system.one) we cannot reuse column references
    // from the original table; use a constant TRUE predicate instead so the result is always 1.
    if (gen.hasTable(policy.table_key))
        ssc2->mutable_where()->CopyFrom(policy.where_expr.value());
    else
        ssc2->mutable_where()->mutable_expr()->mutable_expr()->mutable_lit_val()->mutable_special_val()->set_val(
            SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ONE);

    finishSettings(sel2->mutable_setting_values());
    ts2->set_format(OutFormat::OUT_CSV);
    const auto err2 = std::filesystem::remove(qcfile);
    UNUSED(err2);
    ts2->mutable_intofile()->set_path(qcfile.generic_string());
    ts2->mutable_intofile()->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);

    // ---- Build sq1: EXECUTE AS oracleUser; SELECT count() FROM db.t [FINAL] INTO OUTFILE ----
    // so the session switches to the oracle user before the SELECT runs (row policy applies).
    sq1.CopyFrom(sq2);
    sq1.mutable_single_query()
        ->mutable_explain()
        ->mutable_inner_query()
        ->mutable_select()
        ->mutable_sel()
        ->mutable_select_core()
        ->clear_where();

    gen.enforceFinal(false);
    gen.setAllowNotDetermistic(true);
}

/// ifNull(COUNT(DISTINCT expr), 0) consistency oracle — first query
/// SELECT ifNull(COUNT(DISTINCT expr), 0) FROM <from_clause>
void QueryOracle::generateCountDistinctFirstQuery(RandomGenerator & rg, StatementGenerator & gen, SQLQuery & sq)
{
    TopSelect * ts = sq.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    Select * sel = ts->mutable_sel();
    SelectStatementCore * ssc = sel->mutable_select_core();

    can_test_oracle_result = fc.compare_success_results && rg.nextBool();
    can_test_success = false;
    gen.setAllowEngineUDF(!can_test_oracle_result);
    gen.setAllowNotDetermistic(false);
    gen.enforceFinal(true);
    gen.resetAliasCounter();
    gen.levels[gen.current_level] = QueryLevel(gen.current_level);

    const auto u = gen.generateFromStatement(rg, std::numeric_limits<uint32_t>::max(), ssc->mutable_from());
    UNUSED(u);

    /// Disable aggregates to avoid nested aggregation inside ifNull(COUNT(DISTINCT <expr>), 0)
    const bool prev_allow_aggregates = gen.levels[gen.current_level].allow_aggregates;
    const bool prev_allow_window_funcs = gen.levels[gen.current_level].allow_window_funcs;
    gen.levels[gen.current_level].allow_aggregates = gen.levels[gen.current_level].allow_window_funcs = false;
    SQLFuncCall * sfc1 = ssc->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_func_call();
    SQLFuncCall * sfc2 = sfc1->add_args()->mutable_expr()->mutable_comp_expr()->mutable_func_call();
    sfc1->mutable_func()->set_catalog_func(FUNCifNull);
    sfc1->add_args()->mutable_expr()->mutable_lit_val()->mutable_special_val()->set_val(
        SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO);
    sfc2->mutable_func()->set_catalog_func(FUNCcount);
    sfc2->set_distinct(true);
    gen.generateExpression(rg, sfc2->add_args()->mutable_expr());
    gen.levels[gen.current_level].allow_aggregates = prev_allow_aggregates;
    gen.levels[gen.current_level].allow_window_funcs = prev_allow_window_funcs;

    gen.levels.clear();
    gen.ctes.clear();
    gen.setAllowNotDetermistic(true);
    gen.enforceFinal(false);
    gen.setAllowEngineUDF(true);

    SettingValues * svs = sel->mutable_setting_values();
    /// Use exact count distinct implementation to avoid discrepancies between different implementations (e.g. HyperLogLog gives an approximation)
    SetValue * sv = svs->mutable_set_value();
    sv->set_property("count_distinct_implementation");
    sv->set_value("'uniqExact'");
    finishSettings(svs);
    ts->set_format(OutFormat::OUT_CSV);
    const auto err = std::filesystem::remove(qcfile);
    UNUSED(err);
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
}

/// ifNull(COUNT(DISTINCT expr), 0) consistency oracle — second query
/// SELECT COUNT(*) FROM (SELECT DISTINCT expr FROM <from_clause> WHERE isNotNull(expr)) AS sub
///
/// COUNT(DISTINCT expr) skips NULLs, so the inner DISTINCT subquery filters them out
/// with WHERE isNotNull(expr) to keep the outer COUNT(*) equivalent.
/// Moves the FROM clause and expression out of sq1 to build sq2,
/// mirroring the pattern used by `generateCorrectnessTestSecondQuery`.
void QueryOracle::generateCountDistinctSecondQuery(SQLQuery & sq1, SQLQuery & sq2)
{
    SelectStatementCore & ssc1
        = *sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select()->mutable_sel()->mutable_select_core();
    TopSelect * ts2 = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif2 = ts2->mutable_intofile();
    Select * sel2 = ts2->mutable_sel();
    SelectStatementCore * outer_ssc = sel2->mutable_select_core();

    outer_ssc->add_result_columns()
        ->mutable_eca()
        ->mutable_expr()
        ->mutable_comp_expr()
        ->mutable_func_call()
        ->mutable_func()
        ->set_catalog_func(FUNCcount);

    JoinedTableOrFunction * outer_jtf
        = outer_ssc->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();

    ExplainQuery * inner_explain = outer_jtf->mutable_tof()->mutable_select();
    SelectStatementCore * inner_ssc = inner_explain->mutable_inner_query()->mutable_select()->mutable_sel()->mutable_select_core();
    inner_ssc->set_s_or_d(AllOrDistinct::DISTINCT);
    inner_ssc->set_allocated_from(ssc1.release_from());
    Expr * arg_expr = ssc1.mutable_result_columns(0)
                          ->mutable_eca()
                          ->mutable_expr()
                          ->mutable_comp_expr()
                          ->mutable_func_call()
                          ->mutable_args(0)
                          ->mutable_expr()
                          ->mutable_comp_expr()
                          ->mutable_func_call()
                          ->mutable_args(0)
                          ->release_expr();
    ExprColAlias * eca = inner_ssc->add_result_columns()->mutable_eca();
    eca->set_allocated_expr(arg_expr);
    eca->mutable_col_alias()->set_column("cx");

    /// ifNull(COUNT(DISTINCT expr), 0) skips NULLs; filter them from the inner DISTINCT subquery
    /// to keep COUNT(*) equivalent:  WHERE isNotNull(expr)
    ExprNullTests * null_test = inner_ssc->mutable_where()->mutable_expr()->mutable_expr()->mutable_comp_expr()->mutable_expr_null_tests();
    null_test->mutable_expr()->mutable_comp_expr()->mutable_expr_stc()->mutable_col()->mutable_path()->mutable_col()->set_column("cx");
    null_test->set_not_(true);

    outer_jtf->mutable_table_alias()->set_value("sub");
    finishSettings(sel2->mutable_setting_values());
    ts2->set_format(sq1.single_query().explain().inner_query().select().format());
    const auto err = std::filesystem::remove(qcfile);
    UNUSED(err);
    sif2->set_path(qcfile.generic_string());
    sif2->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
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
    RandomGenerator & rg,
    StatementGenerator & gen,
    const DumpOracleStrategy strategy,
    const bool test_content,
    const SQLTable & t,
    SQLQuery & sq1,
    SQLQuery & sq2)
{
    TopSelect * ts = sq1.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_select();
    SelectIntoFile * sif = ts->mutable_intofile();
    Select * sel = ts->mutable_sel();
    SelectStatementCore * ssc = sel->mutable_select_core();
    JoinedTableOrFunction * jtf = ssc->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();

    insertOnTableOrCluster(rg, gen, t, false, jtf->mutable_tof());
    jtf->set_final(t.supportsFinal());
    switch (strategy)
    {
        case DumpOracleStrategy::DUMP_TABLE:
        case DumpOracleStrategy::OPTIMIZE:
        case DumpOracleStrategy::REATTACH:
        case DumpOracleStrategy::BACKUP_RESTORE: {
            /// Dump entire table and compare contents
            bool first = true;
            OrderByList * obs = ssc->mutable_orderby()->mutable_olist();

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
        }
        break;
        case DumpOracleStrategy::ALTER_UPDATE:
            /// Just match the count
            ssc->add_result_columns()
                ->mutable_eca()
                ->mutable_expr()
                ->mutable_comp_expr()
                ->mutable_func_call()
                ->mutable_func()
                ->set_catalog_func(FUNCcount);
            break;
        case DumpOracleStrategy::INSERT_COUNT: {
            /// On the first step get the current count plus the rows to be inserted
            BinaryExpr * bexpr = ssc->add_result_columns()->mutable_eca()->mutable_expr()->mutable_comp_expr()->mutable_binary_expr();

            nrows = rows_dist(rg.generator);
            bexpr->set_op(BinaryOperator::BINOP_PLUS);
            bexpr->mutable_lhs()->mutable_comp_expr()->mutable_func_call()->mutable_func()->set_catalog_func(FUNCcount);
            bexpr->mutable_rhs()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(nrows);
        }
        break;
    }
    if (test_content)
    {
        finishSettings(sel->mutable_setting_values());
    }
    ts->set_format(rg.pickRandomly(rg.pickRandomly(QueryOracle::oracleFormats)));
    const auto err = std::filesystem::remove(qcfile);
    UNUSED(err);
    sif->set_path(qcfile.generic_string());
    sif->set_step(SelectIntoFile_SelectIntoFileStep::SelectIntoFile_SelectIntoFileStep_TRUNCATE);
    /// Prepare second query
    switch (strategy)
    {
        case DumpOracleStrategy::DUMP_TABLE:
        case DumpOracleStrategy::OPTIMIZE:
        case DumpOracleStrategy::REATTACH:
        case DumpOracleStrategy::BACKUP_RESTORE:
        case DumpOracleStrategy::ALTER_UPDATE:
            /// Second step equal as the first one
            sq2.CopyFrom(sq1);
            break;
        case DumpOracleStrategy::INSERT_COUNT: {
            /// In the second step, just get the total count
            sq2.CopyFrom(sq1);
            SelectStatementCore & scc = *sq2.mutable_single_query()
                                             ->mutable_explain()
                                             ->mutable_inner_query()
                                             ->mutable_select()
                                             ->mutable_sel()
                                             ->mutable_select_core();
            scc.clear_result_columns();
            scc.add_result_columns()
                ->mutable_eca()
                ->mutable_expr()
                ->mutable_comp_expr()
                ->mutable_func_call()
                ->mutable_func()
                ->set_catalog_func(FUNCcount);
        }
        break;
    }
}

void QueryOracle::generateExportQuery(
    RandomGenerator & rg, StatementGenerator & gen, const bool test_content, const SQLTable & t, SQLQuery & sq2)
{
    SettingValues * svs = nullptr;
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
        gen.addRandomRelation(rg, std::nullopt, static_cast<uint32_t>(gen.entries.size()), expr);
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
    ff->set_outformat(rg.pickRandomly(rg.pickRandomly(can_test_oracle_result ? QueryOracle::oracleFormats : outFormats)));
    if (rg.nextSmallNumber() < 4)
    {
        ff->set_fcomp(rg.pickRandomly(compressionMethods));
    }
    if (rg.nextSmallNumber() < 10)
    {
        const auto & settings = can_test_oracle_result ? serverSettings : formatSettings;

        svs = ins->mutable_setting_values();
        gen.generateSettingValues(rg, settings, svs);
        for (int i = 0; i < (svs->other_values_size() + 1) && can_test_oracle_result; i++)
        {
            const SetValue & osv = i == 0 ? svs->set_value() : svs->other_values(i - 1);
            const CHSetting & ochs = settings.at(osv.property());

            can_test_oracle_result &= !ochs.changes_behavior;
        }
    }
    if (can_test_oracle_result)
    {
        /// Ensure deleted mask and patch parts are applied
        svs = svs ? svs : ins->mutable_setting_values();
        finishSettings(svs);
    }
    /// Set the table on select
    JoinedTableOrFunction * jtf = sel->mutable_from()->mutable_tos()->mutable_join_clause()->mutable_tos()->mutable_joined_table();

    insertOnTableOrCluster(rg, gen, t, false, jtf->mutable_tof());
    jtf->set_final(t.supportsFinal());
}

void QueryOracle::dumpOracleIntermediateSteps(
    RandomGenerator & rg,
    StatementGenerator & gen,
    SQLTable & t,
    const DumpOracleStrategy strategy,
    const bool test_content,
    std::vector<SQLQuery> & intermediate_queries)
{
    SQLQuery next;
    const std::optional<String> & cluster = t.getCluster();

    intermediate_queries.clear();
    gen.setAllowNotDetermistic(false);
    switch (strategy)
    {
        case DumpOracleStrategy::DUMP_TABLE: {
            SQLQuery next2;
            SQLQuery next3;
            const auto & t2
                = test_content ? t : rg.pickRandomly(gen.filterCollection<BuzzHouse::SQLTable>(gen.attached_tables_to_test_format)).get();

            /// Export data
            generateExportQuery(rg, gen, test_content, t, next);
            /// Truncate table, then insert everything again
            Truncate * trunc = next2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_trunc();

            t.setName(trunc->mutable_est(), false);
            if (cluster.has_value())
            {
                trunc->mutable_cluster()->set_cluster(cluster.value());
            }
            /// Import data again
            generateImportQuery(rg, gen, t2, next, next3);

            intermediate_queries.emplace_back(next);
            intermediate_queries.emplace_back(next2);
            intermediate_queries.emplace_back(next3);
        }
        break;
        case DumpOracleStrategy::OPTIMIZE: {
            OptimizeTable * ot = next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_opt();

            gen.generateNextOptimizeTableInternal(rg, t, true, ot);
            if (rg.nextSmallNumber() < 3)
            {
                gen.generateSettingValues(rg, formatSettings, ot->mutable_setting_values());
            }
            intermediate_queries.emplace_back(next);
        }
        break;
        case DumpOracleStrategy::REATTACH: {
            SQLQuery next2;
            Detach * det = next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_detach();
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
                gen.generateSettingValues(rg, formatSettings, det->mutable_setting_values());
            }
            if (rg.nextSmallNumber() < 4)
            {
                gen.generateSettingValues(rg, formatSettings, att->mutable_setting_values());
            }
            intermediate_queries.emplace_back(next);
            intermediate_queries.emplace_back(next2);
        }
        break;
        case DumpOracleStrategy::BACKUP_RESTORE: {
            SQLQuery next3;
            std::optional<String> bcluster;
            BackupRestore * bac = next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_backup_restore();
            BackupRestore * res = next3.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_backup_restore();
            SettingValues * bsett = nullptr;
            SettingValues * rsett = nullptr;
            BackupRestoreObject * baco = bac->mutable_backup_element()->mutable_bobject();
            const String dname = t.getDatabaseName();
            const String tname = t.getBaseName();
            const bool table_has_partitions = t.isMergeTreeFamily() && fc.tableHasPartitions(false, dname, tname);

            bac->set_command(BackupRestore_BackupCommand_BACKUP);
            res->set_command(BackupRestore_BackupCommand_RESTORE);

            t.setName(baco->mutable_object()->mutable_est(), false);
            bcluster = gen.backupOrRestoreObject(baco, SQLObject::TABLE, t);
            if (bcluster.has_value())
            {
                bac->mutable_cluster()->set_cluster(bcluster.value());
                res->mutable_cluster()->set_cluster(bcluster.value());
            }
            if (table_has_partitions && rg.nextSmallNumber() < 4)
            {
                baco->add_partitions()->set_partition_id(fc.tableGetRandomPartitionOrPart(rg.nextInFullRange(), false, true, dname, tname));
            }

            gen.setBackupOut(rg, bac->mutable_out());
            res->mutable_out()->CopyFrom(bac->out());
            res->mutable_backup_element()->mutable_bobject()->CopyFrom(bac->backup_element().bobject());

            bac->set_sync(BackupRestore_SyncOrAsync_SYNC);
            res->set_sync(BackupRestore_SyncOrAsync_SYNC);
            if (rg.nextSmallNumber() < 4)
            {
                bsett = bac->mutable_setting_values();
                gen.generateSettingValues(rg, backupSettings, bsett);
                SetValue * sv = bsett->has_set_value() ? bsett->add_other_values() : bsett->mutable_set_value();

                /// Make sure to backup everything
                sv->set_property("structure_only");
                sv->set_value("0");
            }
            if (rg.nextSmallNumber() < 4)
            {
                bsett = bsett ? bsett : bac->mutable_setting_values();
                gen.generateSettingValues(rg, formatSettings, bsett);
            }
            if (rg.nextSmallNumber() < 4)
            {
                rsett = res->mutable_setting_values();
                gen.generateSettingValues(rg, restoreSettings, rsett);
                SetValue * sv = rsett->has_set_value() ? rsett->add_other_values() : rsett->mutable_set_value();

                /// Make sure to recover everything
                sv->set_property("structure_only");
                sv->set_value("0");
            }
            if (rg.nextSmallNumber() < 4)
            {
                rsett = rsett ? rsett : res->mutable_setting_values();
                gen.generateSettingValues(rg, formatSettings, rsett);
            }

            intermediate_queries.emplace_back(next);
            if (baco->partitions_size() == 0)
            {
                /// Truncate table, so it is restored into an empty one
                SQLQuery next2;
                Truncate * trunc = next2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_trunc();

                t.setName(trunc->mutable_est(), false);
                if (cluster.has_value())
                {
                    trunc->mutable_cluster()->set_cluster(cluster.value());
                }
                intermediate_queries.emplace_back(next2);
            }
            intermediate_queries.emplace_back(next3);
        }
        break;
        case DumpOracleStrategy::ALTER_UPDATE: {
            if (!t.areInsertsAppends() || rg.nextBool())
            {
                std::optional<String> acluster;
                Alter * at = next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_alter();

                acluster = gen.alterSingleTable(rg, t, 1, false, t.areInsertsAppends(), false, at);
                if (acluster.has_value())
                {
                    at->mutable_cluster()->set_cluster(acluster.value());
                }
                if (rg.nextSmallNumber() < 3)
                {
                    gen.generateSettingValues(rg, formatSettings, at->mutable_setting_values());
                }
            }
            else
            {
                LightUpdate * upt = next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_upt();

                gen.generateNextUpdateOrDeleteOnTable<LightUpdate>(rg, t, upt);
            }
            intermediate_queries.emplace_back(next);
        }
        break;
        case DumpOracleStrategy::INSERT_COUNT: {
            SQLQuery next2;
            Insert * ins = next.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();

            chassert(nrows);
            next2.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_system_cmd()->set_flush_async_insert_queue(
                true);
            gen.generateInsertToTable(rg, t, false, nrows, ins);
            intermediate_queries.emplace_back(next);
            intermediate_queries.emplace_back(next2);
        }
    }
    gen.setAllowNotDetermistic(true);
}

void QueryOracle::generateImportQuery(
    RandomGenerator & rg, StatementGenerator & gen, const SQLTable & t, const SQLQuery & sq2, SQLQuery & sq4) const
{
    Insert * nins = sq4.mutable_single_query()->mutable_explain()->mutable_inner_query()->mutable_insert();
    InsertFromFile * iff = nins->mutable_insert_file();
    const Insert & oins = sq2.single_query().explain().inner_query().insert();
    const FileFunc & ff = oins.tof().tfunc().file();
    const InFormat & inf
        = (!can_test_oracle_result && rg.nextSmallNumber() < 4) ? rg.pickValueRandomlyFromMap(outIn) : outIn.at(ff.outformat());

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
        gen.generateSettingValues(rg, formatSettings, nins->mutable_setting_values());
    }
    else if (oins.has_setting_values())
    {
        SettingValues * svs = nins->mutable_setting_values();

        svs->CopyFrom(oins.setting_values());
    }
    if (can_test_oracle_result && inf == InFormat::IN_CSV)
    {
        SettingValues * svs = nins->mutable_setting_values();
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        /// The oracle expects to read all the lines from the file
        sv->set_property("input_format_csv_detect_header");
        sv->set_value("0");
    }
    if (can_test_oracle_result)
    {
        /// Ensure deleted mask and patch parts are applied
        finishSettings(nins->mutable_setting_values());
    }
}

/// Run query with different settings oracle
bool QueryOracle::generateFirstSetting(RandomGenerator & rg, SQLQuery & sq1)
{
    const bool use_settings = rg.nextMediumNumber() < 86;

    /// Most of the times use SET command, other times SYSTEM
    if (use_settings)
    {
        std::uniform_int_distribution<uint32_t> settings_range(1, 20);
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
        jtf->mutable_table_alias()->set_value("ex");
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
    }
    SettingValues * svs = sel->mutable_setting_values();

    finishSettings(svs);
    if (measure_performance)
    {
        /// Add tag to find query later on
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();
        sv->set_property("log_comment");
        sv->set_value("'measure_performance'");
    }
    else if (indexes)
    {
        /// These settings are relevant to show index information
        SetValue * sv2 = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();
        SetValue * sv3 = svs->add_other_values();

        sv2->set_property("use_query_condition_cache");
        sv2->set_value("0");
        sv3->set_property("use_skip_indexes_on_data_read");
        sv3->set_value("0");
    }
}

void QueryOracle::iterateQuery(google::protobuf::Message & message, const std::vector<MatchHandler> & rules)
{
    bool handled = false;
    const google::protobuf::Descriptor * desc = message.GetDescriptor();
    const google::protobuf::Reflection * refl = message.GetReflection();

    checkStackSize();
    for (const auto & rh : rules)
    {
        if (rh.predicate(message))
        {
            /// If this message itself is the target type, mutate it.
            /// If the handler returns true, it consumed the node — skip recursion into children
            /// to avoid double-replacing nested sub-messages created by the handler itself.
            handled |= rh.handler(message);
        }
    }
    if (handled)
    {
        return;
    }
    const int field_count = desc->field_count();
    for (int i = 0; i < field_count; ++i)
    {
        const google::protobuf::FieldDescriptor * field = desc->field(i);

        if (field->is_repeated())
        {
            if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
            {
                const int n = refl->FieldSize(message, field);
                for (int idx = 0; idx < n; ++idx)
                {
                    google::protobuf::Message * sub = refl->MutableRepeatedMessage(&message, field, idx);
                    iterateQuery(*sub, rules);
                }
            }
        }
        else if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
        {
            if (!refl->HasField(message, field))
            {
                continue;
            }
            google::protobuf::Message * sub = refl->MutableMessage(&message, field);
            iterateQuery(*sub, rules);
        }
    }
}

void QueryOracle::maybeUpdateOracleSelectQuery(RandomGenerator & rg, StatementGenerator & gen, const SQLQuery & sq1, SQLQuery & sq2)
{
    sq2.CopyFrom(sq1);
    chassert(!compare_explain);
    if (rg.nextBool())
    {
        /// Swap query parts
        std::vector<MatchHandler> rules;
        SQLQueryInner * sq2inner = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query();
        Select & nsel
            = *(measure_performance ? sq2inner->mutable_select()->mutable_sel()
                                    : sq2inner->mutable_insert()->mutable_select()->mutable_select());

        rules.push_back(
            MatchHandler{
                .predicate
                = [](const google::protobuf::Message & m) { return m.GetDescriptor()->full_name() == "BuzzHouse.TableOrFunction"; },
                .handler = [&](google::protobuf::Message & message) -> bool
                {
                    TableOrFunction * tf = dynamic_cast<TableOrFunction *>(&message);

                    chassert(tf);
                    if (tf && tf->has_est() && !compare_explain)
                    {
                        const ExprSchemaTable & est = tf->est();

                        if (!est.has_database()
                            || (est.database().value() != "system" && est.database().value() != "INFORMATION_SCHEMA"
                                && est.database().value() != "information_schema"))
                        {
                            const String tkey = StatementGenerator::getNameFromProto(est.table().value());

                            if (gen.tables.contains(tkey))
                            {
                                /// Replace table with table function call
                                const SQLTable & t = gen.tables.at(tkey);

                                gen.setAllowNotDetermistic(false);
                                if (t.isEngineReplaceable() && rg.nextSmallNumber() < 5)
                                {
                                    gen.setTableFunction(rg, TableFunctionUsage::EngineReplace, t, tf->mutable_tfunc());
                                }
                                else if (rg.nextSmallNumber() < 3)
                                {
                                    gen.setTableFunction(rg, TableFunctionUsage::RemoteCall, t, tf->mutable_tfunc());
                                }
                                gen.setAllowNotDetermistic(true);
                                /// Stop recursion: the replacement created a new inner tof->est that
                                /// would otherwise be visited and replaced again (producing nested remote calls).
                                return true;
                            }
                        }
                    }
                    return false;
                }});
        iterateQuery(nsel, rules);
    }
}

void QueryOracle::truncatePeerTables(const StatementGenerator & gen)
{
    for (const auto & entry : found_tables)
    {
        /// First truncate tables
        other_steps_success &= gen.connections.truncatePeerTableOnRemote(gen.tables.at(entry));
    }
}

void QueryOracle::optimizePeerTables(const StatementGenerator & gen)
{
    for (const auto & entry : found_tables)
    {
        /// Lastly optimize tables
        const auto & ntable = gen.tables.at(entry);

        other_steps_success &= gen.connections.optimizeTableForOracle(PeerTableDatabase::ClickHouse, ntable);
        if (measure_performance)
        {
            other_steps_success &= gen.connections.optimizeTableForOracle(PeerTableDatabase::None, ntable);
        }
    }
}

void QueryOracle::replaceQueryWithTablePeers(
    RandomGenerator & rg, const SQLQuery & sq1, StatementGenerator & gen, std::vector<SQLQuery> & peer_queries, SQLQuery & sq2)
{
    std::vector<MatchHandler> rules;
    found_tables.clear();
    peer_queries.clear();

    sq2.CopyFrom(sq1);
    SQLQueryInner * sq2inner = sq2.mutable_single_query()->mutable_explain()->mutable_inner_query();
    Select & nsel = *(
        measure_performance ? sq2inner->mutable_select()->mutable_sel() : sq2inner->mutable_insert()->mutable_select()->mutable_select());

    /// Replace references
    rules.push_back(
        MatchHandler{
            .predicate = [](const google::protobuf::Message & m) { return m.GetDescriptor()->full_name() == "BuzzHouse.TableOrSubquery"; },
            .handler = [&](google::protobuf::Message & message) -> bool
            {
                TableOrSubquery * tos = dynamic_cast<TableOrSubquery *>(&message);

                chassert(tos);
                if (tos && tos->has_joined_table())
                {
                    bool res = false;
                    JoinedTableOrFunction & jtf = *tos->mutable_joined_table();
                    TableOrFunction & tf = *jtf.mutable_tof();

                    if (tf.has_est())
                    {
                        const ExprSchemaTable & est = tf.est();

                        if (!est.has_database()
                            || (est.database().value() != "system" && est.database().value() != "INFORMATION_SCHEMA"
                                && est.database().value() != "information_schema"))
                        {
                            const String tkey = StatementGenerator::getNameFromProto(est.table().value());

                            if (gen.tables.contains(tkey))
                            {
                                const SQLTable & t = gen.tables.at(tkey);

                                if (t.hasDatabasePeer())
                                {
                                    if (peer_query != PeerQuery::ClickHouseOnly)
                                    {
                                        insertOnTableOrCluster(rg, gen, t, true, &tf);
                                    }
                                    found_tables.insert(tkey);
                                    res = !t.hasClickHousePeer();
                                    can_test_oracle_result &= t.hasClickHousePeer();
                                }
                            }
                        }
                    }
                    /// Remove final for MySQL and PostgreSQL calls
                    jtf.set_final(jtf.final() && !res);
                }
                return false;
            }});
    iterateQuery(nsel, rules);

    if (peer_query == PeerQuery::ClickHouseOnly && !measure_performance)
    {
        /// Use a different file for the peer database
        FileFunc & ff = *sq2.mutable_single_query()
                             ->mutable_explain()
                             ->mutable_inner_query()
                             ->mutable_insert()
                             ->mutable_tof()
                             ->mutable_tfunc()
                             ->mutable_file();

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

        if (t.isMergeTreeFamily() && t.can_run_merges)
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
    other_steps_success = true;
    can_test_oracle_result = can_test_success = fc.compare_success_results;
    nrows = 0;
    res1 = PerformanceResult();
    res2 = PerformanceResult();
}

void QueryOracle::setIntermediateStepSuccess(const bool success)
{
    other_steps_success &= success;
}

void QueryOracle::processFirstOracleQueryResult(const int errcode, ExternalIntegrations & ei)
{
    if (can_test_oracle_result)
    {
        if (!errcode)
        {
            if (measure_performance)
            {
                other_steps_success &= ei.getPerformanceMetricsForLastQuery(PeerTableDatabase::None, this->res1);
            }
            else
            {
                md5_hash1.hashFile(qcfile.generic_string(), first_digest);
            }
        }
        first_errcode = errcode;
    }
}

void QueryOracle::processSecondOracleQueryResult(const int errcode, ExternalIntegrations & ei, const String & oracle_name)
{
    if (other_steps_success && can_test_oracle_result)
    {
        if (can_test_success && ((first_errcode && !errcode) || (!first_errcode && errcode))
            && !fc.oracle_ignore_error_codes.contains(static_cast<uint32_t>(first_errcode ? first_errcode : errcode)))
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
