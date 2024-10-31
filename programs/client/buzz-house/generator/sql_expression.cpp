#include <cstddef>
#include <cstdint>
#include "sql_funcs.h"
#include "sql_types.h"
#include "statement_generator.h"

#include <sys/types.h>

namespace buzzhouse
{

int StatementGenerator::AddFieldAccess(RandomGenerator & rg, sql_query_grammar::Expr * expr, const uint32_t nested_prob)
{
    if (rg.NextMediumNumber() < nested_prob)
    {
        const uint32_t noption = rg.NextMediumNumber();
        sql_query_grammar::FieldAccess * fa = expr->mutable_field();

        this->depth++;
        if (noption < 41)
        {
            fa->set_array_index(rg.NextRandomUInt32() % 5);
        }
        else if (noption < 71)
        {
            fa->set_tuple_index(rg.NextRandomUInt32() % 5);
        }
        else if (this->depth >= this->fc.max_depth || noption < 81)
        {
            buf.resize(0);
            rg.NextJsonCol(buf);
            fa->mutable_array_key()->set_column(buf);
        }
        else
        {
            this->GenerateExpression(rg, fa->mutable_array_expr());
        }
        this->depth--;
    }
    return 0;
}

int StatementGenerator::AddColNestedAccess(RandomGenerator & rg, sql_query_grammar::ExprColumn * expr, const uint32_t nested_prob)
{
    const uint32_t nsuboption = rg.NextLargeNumber();

    this->depth++;
    if (rg.NextMediumNumber() < nested_prob)
    {
        sql_query_grammar::TypeName * tpn = nullptr;
        sql_query_grammar::JSONColumns * subcols = expr->mutable_subcols();
        const uint32_t noption = rg.NextMediumNumber(),
                       nvalues = std::max(std::min(this->fc.max_width - this->width, rg.NextSmallNumber() % 5), UINT32_C(1));

        for (uint32_t i = 0; i < nvalues; i++)
        {
            const uint32_t noption2 = rg.NextMediumNumber();
            sql_query_grammar::JSONColumn * jcol = i == 0 ? subcols->mutable_jcol() : subcols->add_other_jcols();

            this->width++;
            if (noption2 < 31)
            {
                jcol->set_json_col(true);
            }
            else if (noption2 < 61)
            {
                jcol->set_json_array(0);
            }
            buf.resize(0);
            rg.NextJsonCol(buf);
            jcol->mutable_col()->set_column(buf);
        }
        if (noption < 4)
        {
            tpn = subcols->mutable_json_cast();
        }
        else if (noption < 8)
        {
            tpn = subcols->mutable_json_reinterpret();
        }
        this->width -= nvalues;
        if (tpn)
        {
            uint32_t col_counter = 0;
            const SQLType * tp = RandomNextType(rg, ~(allow_nested), col_counter, tpn->mutable_type());
            delete tp;
        }
    }
    if (rg.NextMediumNumber() < nested_prob)
    {
        uint32_t col_counter = 0;
        const SQLType * tp = RandomNextType(rg, ~(allow_nested), col_counter, expr->mutable_dynamic_subtype()->mutable_type());
        delete tp;
    }
    if (nsuboption < 15)
    {
        expr->set_null(true);
    }
    else if (nsuboption < 31)
    {
        expr->set_keys(true);
    }
    else if (nsuboption < 46)
    {
        expr->set_values(true);
    }
    else if (nsuboption < 61)
    {
        expr->set_array_size(rg.NextMediumNumber() % 3);
    }
    this->depth--;
    return 0;
}

int StatementGenerator::RefColumn(RandomGenerator & rg, const GroupCol & gcol, sql_query_grammar::Expr * expr)
{
    sql_query_grammar::ExprSchemaTableColumn * estc = expr->mutable_comp_expr()->mutable_expr_stc();
    sql_query_grammar::ExprColumn * ecol = estc->mutable_col();

    if (gcol.col.rel_name != "")
    {
        estc->mutable_table()->set_table(gcol.col.rel_name);
    }
    ecol->mutable_col()->set_column(gcol.col.name);
    if (gcol.col.name2.has_value())
    {
        ecol->mutable_subcol()->set_column(gcol.col.name2.value());
    }
    if (gcol.gexpr == nullptr)
    {
        AddFieldAccess(rg, expr, 16);
        AddColNestedAccess(rg, ecol, 31);
    }
    else
    {
        const sql_query_grammar::ExprColumn & gecol = gcol.gexpr->comp_expr().expr_stc().col();

        if (gcol.gexpr->has_field())
        {
            expr->mutable_field()->CopyFrom(gcol.gexpr->field());
        }
        if (gecol.has_subcols())
        {
            ecol->mutable_subcols()->CopyFrom(gecol.subcols());
        }
        if (gecol.has_null())
        {
            ecol->set_null(gecol.null());
        }
        else if (gecol.has_keys())
        {
            ecol->set_keys(gecol.keys());
        }
        else if (gecol.has_values())
        {
            ecol->set_values(gecol.values());
        }
        else if (gecol.has_array_size())
        {
            ecol->set_array_size(gecol.array_size());
        }
    }
    return 0;
}

int StatementGenerator::GenerateLiteralValue(RandomGenerator & rg, sql_query_grammar::Expr * expr)
{
    const uint32_t noption = rg.NextLargeNumber();
    sql_query_grammar::LiteralValue * lv = expr->mutable_lit_val();
    uint32_t nested_prob = 0;

    if (noption < 201)
    {
        sql_query_grammar::IntLiteral * il = lv->mutable_int_lit();

        if (noption < 21)
        {
            //hugeint
            sql_query_grammar::HugeInt * huge = il->mutable_hugeint();

            huge->set_lower(rg.NextRandomInt64());
            huge->set_upper(rg.NextRandomUInt64());
            if (rg.NextSmallNumber() < 9)
            {
                il->set_integers(rg.NextBool() ? sql_query_grammar::Integers::Int128 : sql_query_grammar::Integers::Int256);
            }
        }
        else if (noption < 41)
        {
            //uhugeint
            sql_query_grammar::UHugeInt * uhuge = il->mutable_uhugeint();

            uhuge->set_lower(rg.NextRandomUInt64());
            uhuge->set_upper(rg.NextRandomUInt64());
            if (rg.NextSmallNumber() < 9)
            {
                il->set_integers(rg.NextBool() ? sql_query_grammar::Integers::UInt128 : sql_query_grammar::Integers::UInt256);
            }
        }
        else if (noption < 121)
        {
            il->set_int_lit(rg.NextRandomInt64());
            if (rg.NextSmallNumber() < 9)
            {
                il->set_integers(static_cast<sql_query_grammar::Integers>(
                    (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::Integers::Int - sql_query_grammar::Integers::UInt256))
                    + static_cast<uint32_t>(sql_query_grammar::Integers::Int8)));
            }
        }
        else
        {
            il->set_uint_lit(rg.NextRandomUInt64());
            if (rg.NextSmallNumber() < 9)
            {
                il->set_integers(static_cast<sql_query_grammar::Integers>(
                    (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::Integers_MAX)) + 1));
            }
        }
    }
    else if (noption < 401)
    {
        buf.resize(0);
        buf += "'";
        if (noption < 251)
        {
            rg.NextDate(buf);
            buf += "'::Date";
        }
        else if (noption < 301)
        {
            rg.NextDate32(buf);
            buf += "'::Date32";
        }
        else if (noption < 351)
        {
            rg.NextDateTime(buf);
            buf += "'::DateTime";
        }
        else
        {
            rg.NextDateTime64(buf);
            buf += "'::DateTime64";
        }
        lv->set_no_quote_str(buf);
    }
    else if (noption < 501)
    {
        std::uniform_int_distribution<uint32_t> next_dist(0, 30);
        const uint32_t left = next_dist(rg.gen), right = next_dist(rg.gen);

        buf.resize(0);
        buf += "(";
        AppendDecimal(rg, buf, left, right);
        buf += ")";
        lv->set_no_quote_str(buf);
    }
    else if (this->allow_not_deterministic && noption < 551)
    {
        const uint32_t nlen = rg.NextLargeNumber(), noption2 = rg.NextSmallNumber();

        buf.resize(0);
        if (noption2 < 3)
        {
            buf += "randomString";
        }
        else if (noption2 < 5)
        {
            buf += "randomFixedString";
        }
        else if (noption2 < 7)
        {
            buf += "randomPrintableASCII";
        }
        else
        {
            buf += "randomStringUTF8";
        }
        buf += "(";
        buf += std::to_string(nlen);
        buf += ")";
        lv->set_no_quote_str(buf);
    }
    else if (noption < 601)
    {
        buf.resize(0);
        if (rg.NextMediumNumber() < 6)
        {
            buf += "'";
            rg.NextUUID(buf);
            buf += "'";
        }
        else
        {
            rg.NextString(buf, "'", true, (rg.NextRandomUInt32() % 10000) + 1);
        }
        lv->set_no_quote_str(buf);
    }
    else if (noption < 701)
    {
        lv->set_special_val(static_cast<sql_query_grammar::SpecialVal>(
            (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::SpecialVal_MAX)) + 1));
        nested_prob = 3;
    }
    else if (noption < 801)
    {
        lv->set_special_val(rg.NextBool() ? sql_query_grammar::SpecialVal::VAL_ONE : sql_query_grammar::SpecialVal::VAL_ZERO);
    }
    else if (noption < 951)
    {
        std::uniform_int_distribution<int> dopt(1, 3), wopt(1, 3);

        buf.resize(0);
        buf += "'";
        StrBuildJSON(rg, dopt(rg.gen), wopt(rg.gen), buf);
        buf += "'::JSON";
        lv->set_no_quote_str(buf);
    }
    else
    {
        lv->set_special_val(sql_query_grammar::SpecialVal::VAL_NULL);
    }
    AddFieldAccess(rg, expr, nested_prob);
    return 0;
}

int StatementGenerator::GenerateColRef(RandomGenerator & rg, sql_query_grammar::Expr * expr)
{
    std::vector<GroupCol> available_cols;

    if ((this->levels[this->current_level].gcols.empty() && !this->levels[this->current_level].global_aggregate)
        || this->levels[this->current_level].inside_aggregate)
    {
        for (const auto & entry : this->levels[this->current_level].rels)
        {
            for (const auto & col : entry.cols)
            {
                available_cols.push_back(GroupCol(col, nullptr));
            }
        }
    }
    else if (!this->levels[this->current_level].gcols.empty())
    {
        for (const auto & entry : this->levels[this->current_level].gcols)
        {
            available_cols.push_back(entry);
        }
    }

    if (available_cols.empty())
    {
        return this->GenerateLiteralValue(rg, expr);
    }
    return RefColumn(rg, rg.PickRandomlyFromVector(available_cols), expr);
}

int StatementGenerator::GenerateSubquery(RandomGenerator & rg, sql_query_grammar::Select * sel)
{
    const bool prev_inside_aggregate = this->levels[this->current_level].inside_aggregate,
               prev_allow_aggregates = this->levels[this->current_level].allow_aggregates,
               prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

    this->levels[this->current_level].inside_aggregate = false;
    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;
    this->current_level++;
    this->levels[this->current_level] = QueryLevel(this->current_level);
    this->GenerateSelect(rg, true, 1, std::numeric_limits<uint32_t>::max(), sel);
    this->current_level--;
    this->levels[this->current_level].inside_aggregate = prev_inside_aggregate;
    this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
    this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
    return 0;
}

int StatementGenerator::GeneratePredicate(RandomGenerator & rg, sql_query_grammar::Expr * expr)
{
    if (this->depth < this->fc.max_depth)
    {
        const uint32_t noption = rg.NextLargeNumber();

        if (noption < 101)
        {
            sql_query_grammar::ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            sql_query_grammar::UnaryExpr * unexp = cexpr->mutable_unary_expr();

            unexp->set_unary_op(sql_query_grammar::UnaryOperator::UNOP_NOT);
            this->depth++;
            if (rg.NextSmallNumber() < 5)
            {
                this->GeneratePredicate(rg, unexp->mutable_expr());
            }
            else
            {
                this->GenerateExpression(rg, unexp->mutable_expr());
            }
            this->depth--;
        }
        else if (this->fc.max_width > this->width + 1 && noption < 301)
        {
            sql_query_grammar::ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            sql_query_grammar::BinaryExpr * bexpr = cexpr->mutable_binary_expr();

            this->depth++;
            if (rg.NextSmallNumber() < 5)
            {
                bexpr->set_op(rg.NextBool() ? sql_query_grammar::BinaryOperator::BINOP_AND : sql_query_grammar::BinaryOperator::BINOP_OR);

                this->GeneratePredicate(rg, bexpr->mutable_lhs());
                this->width++;
                this->GeneratePredicate(rg, bexpr->mutable_rhs());
            }
            else
            {
                bexpr->set_op(static_cast<sql_query_grammar::BinaryOperator>(
                    (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::BinaryOperator_MAX)) + 1));
                this->GenerateExpression(rg, bexpr->mutable_lhs());
                this->width++;
                this->GenerateExpression(rg, bexpr->mutable_rhs());
            }
            this->width--;
            this->depth--;
        }
        else if (this->fc.max_width > this->width + 2 && noption < 401)
        {
            sql_query_grammar::ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            sql_query_grammar::ExprBetween * bexpr = cexpr->mutable_expr_between();

            bexpr->set_not_(rg.NextBool());
            this->depth++;
            this->GenerateExpression(rg, bexpr->mutable_expr1());
            this->width++;
            this->GenerateExpression(rg, bexpr->mutable_expr2());
            this->width++;
            this->GenerateExpression(rg, bexpr->mutable_expr3());
            this->width -= 2;
            this->depth--;
        }
        else if (this->width < this->fc.max_width && noption < 501)
        {
            const uint32_t nclauses = std::min(this->fc.max_width - this->width, (rg.NextSmallNumber() % 4) + 1);
            sql_query_grammar::ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            sql_query_grammar::ExprIn * ein = cexpr->mutable_expr_in();
            sql_query_grammar::ExprList * elist = ein->mutable_expr();

            ein->set_not_(rg.NextBool());
            ein->set_global(rg.NextBool());

            this->depth++;
            for (uint32_t i = 0; i < nclauses; i++)
            {
                this->GenerateExpression(rg, i == 0 ? elist->mutable_expr() : elist->add_extra_exprs());
            }
            if (rg.NextBool())
            {
                this->GenerateSubquery(rg, ein->mutable_sel());
            }
            else
            {
                sql_query_grammar::ExprList * elist2 = ein->mutable_exprs();

                for (uint32_t i = 0; i < nclauses; i++)
                {
                    this->GenerateExpression(rg, i == 0 ? elist2->mutable_expr() : elist2->add_extra_exprs());
                }
            }
            this->depth--;
        }
        else if (this->fc.max_width > this->width + 1 && noption < 601)
        {
            sql_query_grammar::ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            sql_query_grammar::ExprAny * eany = cexpr->mutable_expr_any();

            eany->set_op(static_cast<sql_query_grammar::BinaryOperator>(
                (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::BinaryOperator::BINOP_LEGR)) + 1));
            eany->set_anyall(rg.NextBool());
            this->depth++;
            this->GenerateExpression(rg, eany->mutable_expr());
            this->width++;
            this->GenerateSubquery(rg, eany->mutable_sel());
            this->width--;
            this->depth--;
        }
        else if (noption < 701)
        {
            sql_query_grammar::ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            sql_query_grammar::ExprNullTests * enull = cexpr->mutable_expr_null_tests();

            enull->set_not_(rg.NextBool());
            this->depth++;
            this->GenerateExpression(rg, enull->mutable_expr());
            this->depth--;
        }
        else if (noption < 801)
        {
            sql_query_grammar::ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            sql_query_grammar::ExprExists * exists = cexpr->mutable_expr_exists();

            exists->set_not_(rg.NextBool());
            this->depth++;
            this->GenerateSubquery(rg, exists->mutable_select());
            this->depth--;
        }
        else if (this->fc.max_width > this->width + 1 && noption < 901)
        {
            sql_query_grammar::ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            sql_query_grammar::ExprLike * elike = cexpr->mutable_expr_like();

            elike->set_not_(rg.NextBool());
            elike->set_keyword(static_cast<sql_query_grammar::ExprLike_PossibleKeywords>(
                (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::ExprLike::PossibleKeywords_MAX)) + 1));
            this->depth++;
            this->GenerateExpression(rg, elike->mutable_expr1());
            this->width++;
            this->GenerateExpression(rg, elike->mutable_expr2());
            this->width--;
            this->depth--;
        }
        else
        {
            this->depth++;
            this->GenerateExpression(rg, expr);
            this->depth--;
        }
        AddFieldAccess(rg, expr, 0);
    }
    else
    {
        return this->GenerateLiteralValue(rg, expr);
    }
    return 0;
}

int StatementGenerator::GenerateLambdaCall(RandomGenerator & rg, const uint32_t nparams, sql_query_grammar::LambdaExpr * lexpr)
{
    SQLRelation rel("");
    std::map<uint32_t, QueryLevel> levels_backup;
    const bool prev_inside_aggregate = this->levels[this->current_level].inside_aggregate,
               prev_allow_aggregates = this->levels[this->current_level].allow_aggregates,
               prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

    for (const auto & entry : this->levels)
    {
        levels_backup[entry.first] = std::move(entry.second);
    }
    this->levels.clear();
    this->levels[this->current_level].inside_aggregate = false;
    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;

    for (uint32_t i = 0; i < nparams; i++)
    {
        buf.resize(0);
        buf += ('x' + i);
        lexpr->add_args()->set_column(buf);
        rel.cols.push_back(SQLRelationCol("", buf, std::nullopt));
    }
    this->levels[this->current_level].rels.push_back(std::move(rel));
    this->GenerateExpression(rg, lexpr->mutable_expr());

    this->levels.clear();
    for (const auto & entry : levels_backup)
    {
        this->levels[entry.first] = std::move(entry.second);
    }
    this->levels[this->current_level].inside_aggregate = prev_inside_aggregate;
    this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
    this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
    return 0;
}

int StatementGenerator::GenerateFuncCall(
    RandomGenerator & rg, const bool allow_funcs, const bool allow_aggr, sql_query_grammar::SQLFuncCall * func_call)
{
    const size_t funcs_size = this->allow_not_deterministic ? CHFuncs.size() : (CHFuncs.size() - 60);
    const uint32_t nfuncs = static_cast<uint32_t>((allow_funcs ? funcs_size : 0) + (allow_aggr ? CHAggrs.size() : 0));
    std::uniform_int_distribution<uint32_t> next_dist(0, nfuncs - 1);
    uint32_t generated_params = 0;

    assert(allow_funcs || allow_aggr);
    const uint32_t nopt = next_dist(rg.gen);
    if (!allow_funcs || nopt >= funcs_size)
    {
        //aggregate
        const CHAggregate & agg = CHAggrs[nopt - static_cast<uint32_t>(allow_funcs ? funcs_size : 0)];
        const uint32_t max_params = std::min(this->fc.max_width - this->width, std::min(agg.max_params, UINT32_C(5))),
                       max_args = std::min(this->fc.max_width - this->width, std::min(agg.max_args, UINT32_C(5))),
                       ncombinators
            = rg.NextSmallNumber() < 4 ? std::min(this->fc.max_width - this->width, (rg.NextSmallNumber() % 3) + 1) : 0;
        const bool prev_inside_aggregate = this->levels[this->current_level].inside_aggregate,
                   prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

        this->levels[this->current_level].inside_aggregate = true;
        this->levels[this->current_level].allow_window_funcs = false;
        if (max_params > 0 && max_params >= agg.min_params)
        {
            std::uniform_int_distribution<uint32_t> nparams(agg.min_params, max_params);
            const uint32_t nagg_params = nparams(rg.gen);

            for (uint32_t i = 0; i < nagg_params; i++)
            {
                this->GenerateExpression(rg, func_call->add_params());
                this->width++;
                generated_params++;
            }
        }
        else if (agg.min_params > 0)
        {
            for (uint32_t i = 0; i < agg.min_params; i++)
            {
                GenerateLiteralValue(rg, func_call->add_params());
            }
        }

        if (max_args > 0 && max_args >= agg.min_args)
        {
            std::uniform_int_distribution<uint32_t> nparams(agg.min_args, max_args);
            const uint32_t nagg_args = nparams(rg.gen);

            for (uint32_t i = 0; i < nagg_args; i++)
            {
                this->GenerateExpression(rg, func_call->add_args()->mutable_expr());
                this->width++;
                generated_params++;
            }
        }
        else if (agg.min_args > 0)
        {
            for (uint32_t i = 0; i < agg.min_args; i++)
            {
                GenerateLiteralValue(rg, func_call->add_args()->mutable_expr());
            }
        }

        for (uint32_t i = 0; i < ncombinators; i++)
        {
            sql_query_grammar::SQLFuncCall_AggregateCombinator comb = static_cast<sql_query_grammar::SQLFuncCall_AggregateCombinator>(
                (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::SQLFuncCall::AggregateCombinator_MAX)) + 1);

            switch (comb)
            {
                case sql_query_grammar::SQLFuncCall_AggregateCombinator::SQLFuncCall_AggregateCombinator_If:
                    if (rg.NextSmallNumber() < 9)
                    {
                        this->GeneratePredicate(rg, func_call->add_args()->mutable_expr());
                    }
                    else
                    {
                        this->GenerateExpression(rg, func_call->add_args()->mutable_expr());
                    }
                    this->width++;
                    generated_params++;
                    break;
                case sql_query_grammar::SQLFuncCall_AggregateCombinator::SQLFuncCall_AggregateCombinator_ArgMin:
                case sql_query_grammar::SQLFuncCall_AggregateCombinator::SQLFuncCall_AggregateCombinator_ArgMax:
                    this->GenerateExpression(rg, func_call->add_args()->mutable_expr());
                    this->width++;
                    generated_params++;
                    break;
                default:
                    break;
            }
            func_call->add_combinators(comb);
        }
        this->levels[this->current_level].inside_aggregate = prev_inside_aggregate;
        this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;

        func_call->set_distinct(agg.support_distinct && func_call->args_size() == 1 && rg.NextBool());
        if (agg.support_nulls_clause && rg.NextSmallNumber() < 7)
        {
            func_call->set_fnulls(rg.NextBool() ? sql_query_grammar::FuncNulls::NRESPECT : sql_query_grammar::FuncNulls::NIGNORE);
        }
        func_call->mutable_func()->set_catalog_func(static_cast<sql_query_grammar::SQLFunc>(agg.fnum));
    }
    else
    {
        //function
        uint32_t n_lambda = 0, min_args = 0, max_args = 0;
        sql_query_grammar::SQLFuncName * sfn = func_call->mutable_func();

        if (!this->functions.empty()
            && (this->allow_not_deterministic || CollectionHas<SQLFunction>([](const SQLFunction & f) { return !f.not_deterministic; }))
            && rg.NextSmallNumber() < 3)
        {
            //use a function from the user
            const std::reference_wrapper<const SQLFunction> & func = this->allow_not_deterministic
                ? std::ref<const SQLFunction>(rg.PickValueRandomlyFromMap(this->functions))
                : rg.PickRandomlyFromVector(FilterCollection<SQLFunction>([](const SQLFunction & f) { return !f.not_deterministic; }));

            min_args = max_args = func.get().nargs;
            sfn->mutable_function()->set_function("f" + std::to_string(func.get().fname));
        }
        else
        {
            //use a default catalog function
            const CHFunction & func = CHFuncs[nopt];

            n_lambda = std::max(func.min_lambda_param, func.max_lambda_param > 0 ? (rg.NextSmallNumber() % func.max_lambda_param) : 0);
            min_args = func.min_args;
            max_args = std::min(this->fc.max_width - this->width, std::min(func.max_args, UINT32_C(5)));
            sfn->set_catalog_func(static_cast<sql_query_grammar::SQLFunc>(func.fnum));
        }

        if (n_lambda > 0)
        {
            assert(n_lambda == 1);
            GenerateLambdaCall(rg, (rg.NextSmallNumber() % 3) + 1, func_call->add_args()->mutable_lambda());
            this->width++;
            generated_params++;
        }
        if (max_args > 0 && max_args >= min_args)
        {
            std::uniform_int_distribution<uint32_t> nparams(min_args, max_args);
            const uint32_t nfunc_args = nparams(rg.gen);

            for (uint32_t i = 0; i < nfunc_args; i++)
            {
                this->GenerateExpression(rg, func_call->add_args()->mutable_expr());
                this->width++;
                generated_params++;
            }
        }
        else if (min_args > 0)
        {
            for (uint32_t i = 0; i < min_args; i++)
            {
                GenerateLiteralValue(rg, func_call->add_args()->mutable_expr());
            }
        }
    }
    this->width -= generated_params;
    return 0;
}

int StatementGenerator::GenerateFrameBound(RandomGenerator & rg, sql_query_grammar::Expr * expr)
{
    if (rg.NextBool())
    {
        expr->mutable_lit_val()->mutable_int_lit()->set_int_lit(rg.NextRandomInt64());
    }
    else
    {
        std::map<uint32_t, QueryLevel> levels_backup;

        for (const auto & entry : this->levels)
        {
            levels_backup[entry.first] = std::move(entry.second);
        }
        this->levels.clear();
        this->GenerateExpression(rg, expr);
        for (const auto & entry : levels_backup)
        {
            this->levels[entry.first] = std::move(entry.second);
        }
    }
    return 0;
}

int StatementGenerator::GenerateExpression(RandomGenerator & rg, sql_query_grammar::Expr * expr)
{
    const uint32_t noption = rg.NextLargeNumber();
    sql_query_grammar::ExprColAlias * eca = nullptr;

    if (rg.NextSmallNumber() < 3)
    {
        sql_query_grammar::ParenthesesExpr * paren = expr->mutable_comp_expr()->mutable_par_expr();

        eca = paren->mutable_expr();
        expr = eca->mutable_expr();
    }

    if (noption < (this->inside_projection ? 76 : 151))
    {
        (void)this->GenerateLiteralValue(rg, expr);
    }
    else if (this->depth >= this->fc.max_depth || noption < 401)
    {
        (void)this->GenerateColRef(rg, expr);
    }
    else if (noption < 451)
    {
        (void)this->GeneratePredicate(rg, expr);
    }
    else if (noption < 501)
    {
        uint32_t col_counter = 0;
        sql_query_grammar::CastExpr * casexpr = expr->mutable_comp_expr()->mutable_cast_expr();

        this->depth++;
        const SQLType * tp = RandomNextType(rg, ~(allow_nested), col_counter, casexpr->mutable_type_name()->mutable_type());
        delete tp;
        this->GenerateExpression(rg, casexpr->mutable_expr());
        this->depth--;
    }
    else if (noption < 526)
    {
        sql_query_grammar::UnaryExpr * uexpr = expr->mutable_comp_expr()->mutable_unary_expr();

        this->depth++;
        uexpr->set_unary_op(static_cast<sql_query_grammar::UnaryOperator>(
            (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::UnaryOperator::UNOP_PLUS)) + 1));
        this->GenerateExpression(rg, uexpr->mutable_expr());
        this->depth--;
    }
    else if (noption < 551)
    {
        sql_query_grammar::IntervalExpr * inter = expr->mutable_comp_expr()->mutable_interval();

        this->depth++;
        inter->set_interval(static_cast<sql_query_grammar::IntervalExpr_Interval>(
            (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::IntervalExpr::Interval_MAX)) + 1));
        this->GenerateExpression(rg, inter->mutable_expr());
        this->depth--;
    }
    else if (this->fc.max_width > this->width && noption < 576)
    {
        sql_query_grammar::CondExpr * conexpr = expr->mutable_comp_expr()->mutable_expr_cond();

        this->depth++;
        this->GenerateExpression(rg, conexpr->mutable_expr1());
        this->width++;
        this->GenerateExpression(rg, conexpr->mutable_expr2());
        this->width++;
        this->GenerateExpression(rg, conexpr->mutable_expr3());
        this->width -= 2;
        this->depth--;
    }
    else if (this->fc.max_width > this->width + 1 && noption < 601)
    {
        sql_query_grammar::ExprCase * caseexp = expr->mutable_comp_expr()->mutable_expr_case();
        const uint32_t nwhen = std::min(this->fc.max_width - this->width, rg.NextSmallNumber() % 4);

        this->depth++;
        if (rg.NextSmallNumber() < 5)
        {
            this->GenerateExpression(rg, caseexp->mutable_expr());
        }
        for (uint32_t i = 0; i < nwhen; i++)
        {
            sql_query_grammar::ExprWhenThen * wt = i == 0 ? caseexp->mutable_when_then() : caseexp->add_extra_when_thens();

            this->GenerateExpression(rg, wt->mutable_when_expr());
            this->GenerateExpression(rg, wt->mutable_then_expr());
            this->width++;
        }
        this->width -= nwhen;
        if (rg.NextSmallNumber() < 5)
        {
            this->GenerateExpression(rg, caseexp->mutable_else_expr());
        }
        this->depth--;
    }
    else if (noption < 651)
    {
        this->depth++;
        this->GenerateSubquery(rg, expr->mutable_comp_expr()->mutable_subquery());
        this->depth--;
    }
    else if (this->fc.max_width > this->width + 1 && noption < 701)
    {
        sql_query_grammar::BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

        this->depth++;
        bexpr->set_op(static_cast<sql_query_grammar::BinaryOperator>((rg.NextRandomUInt32() % 7) + 10));
        this->GenerateExpression(rg, bexpr->mutable_lhs());
        this->width++;
        this->GenerateExpression(rg, bexpr->mutable_rhs());
        this->width--;
        this->depth--;
    }
    else if (this->width < this->fc.max_width && noption < 751)
    {
        sql_query_grammar::ArraySequence * arr = expr->mutable_comp_expr()->mutable_array();
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.NextSmallNumber() % 8);

        this->depth++;
        for (uint32_t i = 0; i < nvalues; i++)
        {
            this->GenerateExpression(rg, arr->add_values());
            this->width++;
        }
        this->depth--;
        this->width -= nvalues;
    }
    else if (this->width < this->fc.max_width && noption < 801)
    {
        sql_query_grammar::TupleSequence * tupl = expr->mutable_comp_expr()->mutable_tuple();
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.NextSmallNumber() % 8),
                       ncols = std::min(this->fc.max_width - this->width, (rg.NextSmallNumber() % 4) + 1);

        this->depth++;
        for (uint32_t i = 0; i < ncols; i++)
        {
            sql_query_grammar::ExprList * elist = tupl->add_values();

            for (uint32_t j = 0; j < nvalues; j++)
            {
                sql_query_grammar::Expr * el = j == 0 ? elist->mutable_expr() : elist->add_extra_exprs();

                this->GenerateExpression(rg, el);
                this->width++;
            }
            this->width -= nvalues;
            this->width++;
        }
        this->width -= ncols;
        this->depth--;
    }
    else if (!this->levels[this->current_level].allow_window_funcs || this->levels[this->current_level].inside_aggregate || noption < 951)
    {
        //func
        const bool allow_aggr = !this->levels[this->current_level].inside_aggregate && this->levels[this->current_level].allow_aggregates
            && (!this->levels[this->current_level].gcols.empty() || this->levels[this->current_level].global_aggregate);

        this->depth++;
        GenerateFuncCall(rg, true, allow_aggr, expr->mutable_comp_expr()->mutable_func_call());
        this->depth--;
    }
    else
    {
        //window func
        sql_query_grammar::WindowFuncCall * sfc = expr->mutable_comp_expr()->mutable_window_call();
        sql_query_grammar::WindowDefn * wdf = sfc->mutable_win_defn();
        const bool prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

        this->depth++;
        this->levels[this->current_level].allow_window_funcs = false;
        if (rg.NextSmallNumber() < 7)
        {
            GenerateFuncCall(rg, false, true, sfc->mutable_agg_func());
        }
        else
        {
            uint32_t nargs = 0;
            sql_query_grammar::SQLWindowCall * wc = sfc->mutable_win_func();

            assert(this->ids.empty());
            if (this->fc.max_width - this->width > 1)
            {
                this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINnth_value));
            }
            if (this->fc.max_width > this->width)
            {
                this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINfirst_value));
                this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINlast_value));
                this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINntile));
                this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINlagInFrame));
                this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINleadInFrame));
            }
            this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINdense_rank));
            this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINnth_value));
            this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINpercent_rank));
            this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINrank));
            this->ids.push_back(static_cast<uint32_t>(sql_query_grammar::WINrow_number));
            const sql_query_grammar::WindowFuncs wfs = static_cast<sql_query_grammar::WindowFuncs>(rg.PickRandomlyFromVector(this->ids));

            this->ids.clear();
            switch (wfs)
            {
                case sql_query_grammar::WINfirst_value:
                case sql_query_grammar::WINlast_value:
                    if (rg.NextSmallNumber() < 7)
                    {
                        wc->set_fnulls(rg.NextBool() ? sql_query_grammar::FuncNulls::NRESPECT : sql_query_grammar::FuncNulls::NIGNORE);
                    }
                    nargs = 1;
                    break;
                case sql_query_grammar::WINntile:
                    nargs = 1;
                    break;
                case sql_query_grammar::WINnth_value:
                    nargs = 2;
                    break;
                case sql_query_grammar::WINlagInFrame:
                case sql_query_grammar::WINleadInFrame:
                    nargs = std::min(this->fc.max_width - this->width, (rg.NextMediumNumber() % 3) + 1);
                    break;
                default:
                    break;
            }
            wc->set_func(wfs);
            for (uint32_t i = 0; i < nargs; i++)
            {
                this->GenerateExpression(rg, wc->add_args());
                this->width++;
            }
            this->width -= nargs;
        }
        if (this->width < this->fc.max_width && rg.NextSmallNumber() < 4)
        {
            const uint32_t nclauses = std::min(this->fc.max_width - this->width, (rg.NextSmallNumber() % 4) + 1);

            for (uint32_t i = 0; i < nclauses; i++)
            {
                this->GenerateExpression(rg, wdf->add_partition_exprs());
                this->width++;
            }
            this->width -= nclauses;
        }
        if (this->width < this->fc.max_width && rg.NextSmallNumber() < 4)
        {
            GenerateOrderBy(rg, 0, true, wdf->mutable_order_by());
        }
        if (this->width < this->fc.max_width && rg.NextSmallNumber() < 4)
        {
            sql_query_grammar::ExprFrameSpec * efs = wdf->mutable_frame_spec();
            sql_query_grammar::FrameSpecSubLeftExpr * fssle = efs->mutable_left_expr();
            sql_query_grammar::FrameSpecSubLeftExpr_Which fspec = static_cast<sql_query_grammar::FrameSpecSubLeftExpr_Which>(
                (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::FrameSpecSubLeftExpr_Which_Which_MAX)) + 1);

            efs->set_range_rows(
                rg.NextBool() ? sql_query_grammar::ExprFrameSpec_RangeRows_RANGE : sql_query_grammar::ExprFrameSpec_RangeRows_ROWS);
            fssle->set_which(fspec);
            if (fspec > sql_query_grammar::FrameSpecSubLeftExpr_Which_UNBOUNDED_PRECEDING)
            {
                this->GenerateFrameBound(rg, fssle->mutable_expr());
            }
            if (rg.NextBool())
            {
                sql_query_grammar::FrameSpecSubRightExpr * fsslr = efs->mutable_right_expr();
                sql_query_grammar::FrameSpecSubRightExpr_Which fspec2 = static_cast<sql_query_grammar::FrameSpecSubRightExpr_Which>(
                    (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::FrameSpecSubRightExpr_Which_Which_MAX)) + 1);

                fsslr->set_which(fspec2);
                if (fspec2 > sql_query_grammar::FrameSpecSubRightExpr_Which_UNBOUNDED_FOLLOWING)
                {
                    this->GenerateFrameBound(rg, fsslr->mutable_expr());
                }
            }
        }
        this->depth--;
        this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
    }
    AddFieldAccess(rg, expr, 16);
    if (eca && rg.NextSmallNumber() < 4)
    {
        SQLRelation rel("");
        const uint32_t cname = this->levels[this->current_level].aliases_counter++;
        const std::string cname_str = "c" + std::to_string(cname);

        rel.cols.push_back(SQLRelationCol("", cname_str, std::nullopt));
        this->levels[this->current_level].rels.push_back(std::move(rel));
        eca->mutable_col_alias()->set_column(cname_str);
        this->levels[this->current_level].projections.push_back(cname);
    }
    return 0;
}

}
