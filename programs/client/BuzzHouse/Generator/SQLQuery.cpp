#include "SQLCatalog.h"
#include "StatementGenerator.h"

namespace BuzzHouse
{

int StatementGenerator::generateArrayJoin(RandomGenerator & rg, ArrayJoin * aj)
{
    SQLRelation rel("");
    std::string cname = "c" + std::to_string(this->levels[this->current_level].aliases_counter++);
    ExprColAlias * eca = aj->mutable_constraint();
    Expr * expr = eca->mutable_expr();

    aj->set_left(rg.nextBool());
    if (rg.nextSmallNumber() < 8)
    {
        const SQLRelation & rel1 = rg.pickRandomlyFromVector(this->levels[this->current_level].rels);
        const SQLRelationCol & col1 = rg.pickRandomlyFromVector(rel1.cols);
        ExprSchemaTableColumn * estc = expr->mutable_comp_expr()->mutable_expr_stc();
        ExprColumn * ecol = estc->mutable_col();

        if (!rel1.name.empty())
        {
            estc->mutable_table()->set_table(rel1.name);
        }
        ecol->mutable_col()->set_column(col1.name);
        if (col1.name2.has_value())
        {
            ecol->mutable_subcol()->set_column(col1.name2.value());
        }
        addFieldAccess(rg, expr, 16);
        addColNestedAccess(rg, ecol, 31);
    }
    else
    {
        generateExpression(rg, expr);
    }
    rel.cols.push_back(SQLRelationCol("", cname, std::nullopt));
    this->levels[this->current_level].rels.push_back(std::move(rel));
    eca->mutable_col_alias()->set_column(cname);
    return 0;
}

int StatementGenerator::generateDerivedTable(RandomGenerator & rg, SQLRelation & rel, const uint32_t allowed_clauses, Select * sel)
{
    std::map<uint32_t, QueryLevel> levels_backup;
    uint32_t ncols = std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % UINT32_C(5)) + 1);

    for (const auto & entry : this->levels)
    {
        levels_backup[entry.first] = std::move(entry.second);
    }
    this->levels.clear();

    this->current_level++;
    this->levels[this->current_level] = QueryLevel(this->current_level);
    generateSelect(rg, false, ncols, allowed_clauses, sel);
    this->current_level--;

    for (const auto & entry : levels_backup)
    {
        this->levels[entry.first] = std::move(entry.second);
    }

    if (sel->has_select_core())
    {
        const SelectStatementCore & scc = sel->select_core();

        for (int i = 0; i < scc.result_columns_size(); i++)
        {
            rel.cols.push_back(SQLRelationCol(rel.name, scc.result_columns(i).eca().col_alias().column(), std::nullopt));
        }
    }
    else if (sel->has_set_query())
    {
        const Select * aux = &sel->set_query().sel1();

        while (aux->has_set_query())
        {
            aux = &aux->set_query().sel1();
        }
        if (aux->has_select_core())
        {
            const SelectStatementCore & scc = aux->select_core();
            for (int i = 0; i < scc.result_columns_size(); i++)
            {
                rel.cols.push_back(SQLRelationCol(rel.name, scc.result_columns(i).eca().col_alias().column(), std::nullopt));
            }
        }
    }
    if (rel.cols.empty())
    {
        rel.cols.push_back(SQLRelationCol(rel.name, "c0", std::nullopt));
    }
    return 0;
}

int StatementGenerator::generateFromElement(RandomGenerator & rg, const uint32_t allowed_clauses, TableOrSubquery * tos)
{
    std::string name;
    const uint32_t derived_table = 30 * static_cast<uint32_t>(this->depth < this->fc.max_depth && this->width < this->fc.max_width),
                   cte = 10 * static_cast<uint32_t>(!this->ctes.empty()),
                   table = 40 * static_cast<uint32_t>(collectionHas<SQLTable>(attached_tables)),
                   view = 20
        * static_cast<uint32_t>(collectionHas<SQLView>(
            [&](const SQLView & vv)
            {
                return (!vv.db || vv.db->attached == DetachStatus::ATTACHED) && vv.attached == DetachStatus::ATTACHED
                    && (vv.is_deterministic || this->allow_not_deterministic);
            })),
                   tudf = 5, prob_space = derived_table + cte + table + view + tudf;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    name += "t";
    name += std::to_string(this->levels[this->current_level].rels.size());
    name += "d";
    name += std::to_string(this->current_level);

    if (derived_table && nopt < (derived_table + 1))
    {
        SQLRelation rel(name);
        JoinedDerivedQuery * jdq = tos->mutable_joined_derived_query();

        generateDerivedTable(rg, rel, allowed_clauses, jdq->mutable_select());
        jdq->mutable_table_alias()->set_table(name);
        this->levels[this->current_level].rels.push_back(std::move(rel));
    }
    else if (cte && nopt < (derived_table + cte + 1))
    {
        SQLRelation rel(name);
        JoinedTable * jt = tos->mutable_joined_table();
        const auto & next_cte = rg.pickValueRandomlyFromMap(rg.pickValueRandomlyFromMap(this->ctes));

        jt->mutable_est()->mutable_table()->set_table(next_cte.name);
        for (const auto & entry : next_cte.cols)
        {
            rel.cols.push_back(entry);
        }
        jt->mutable_table_alias()->set_table(name);
        this->levels[this->current_level].rels.push_back(std::move(rel));
    }
    else if (table && nopt < (derived_table + cte + table + 1))
    {
        JoinedTable * jt = tos->mutable_joined_table();
        ExprSchemaTable * est = jt->mutable_est();
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));

        if (t.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        est->mutable_table()->set_table("t" + std::to_string(t.tname));
        jt->mutable_table_alias()->set_table(name);
        jt->set_final(t.supportsFinal() && (this->enforce_final || rg.nextSmallNumber() < 3));
        addTableRelation(rg, true, name, t);
    }
    else if (view && nopt < (derived_table + cte + table + view + 1))
    {
        SQLRelation rel(name);
        JoinedTable * jt = tos->mutable_joined_table();
        ExprSchemaTable * est = jt->mutable_est();
        const SQLView & v = rg.pickRandomlyFromVector(filterCollection<SQLView>(
            [&](const SQLView & vv)
            {
                return (!vv.db || vv.db->attached == DetachStatus::ATTACHED) && vv.attached == DetachStatus::ATTACHED
                    && (vv.is_deterministic || this->allow_not_deterministic);
            }));

        if (v.db)
        {
            est->mutable_database()->set_database("d" + std::to_string(v.db->dname));
        }
        est->mutable_table()->set_table("v" + std::to_string(v.tname));
        jt->mutable_table_alias()->set_table(name);
        jt->set_final(!v.is_materialized && (this->enforce_final || rg.nextSmallNumber() < 3));
        for (uint32_t i = 0; i < v.ncols; i++)
        {
            rel.cols.push_back(SQLRelationCol(name, "c" + std::to_string(i), std::nullopt));
        }
        this->levels[this->current_level].rels.push_back(std::move(rel));
    }
    else if (tudf)
    {
        SQLRelation rel(name);
        std::map<uint32_t, QueryLevel> levels_backup;
        const uint32_t noption = rg.nextSmallNumber();
        Expr * limit = nullptr;
        JoinedTableFunction * jtf = tos->mutable_joined_table_function();
        TableFunction * tf = jtf->mutable_tfunc();
        GenerateSeriesFunc * gsf = tf->mutable_gseries();
        GenerateSeriesFunc_GSName val = static_cast<GenerateSeriesFunc_GSName>(
            (rg.nextRandomUInt32() % static_cast<uint32_t>(GenerateSeriesFunc_GSName_GSName_MAX)) + 1);
        const std::string & cname = val == GenerateSeriesFunc_GSName::GenerateSeriesFunc_GSName_numbers ? "number" : "generate_series";

        gsf->set_fname(val);
        for (const auto & entry : this->levels)
        {
            levels_backup[entry.first] = std::move(entry.second);
        }
        this->levels.clear();
        if (val == GenerateSeriesFunc_GSName::GenerateSeriesFunc_GSName_numbers)
        {
            if (noption < 4)
            {
                //1 arg
                limit = gsf->mutable_expr1();
            }
            else
            {
                //2 args
                if (rg.nextBool())
                {
                    gsf->mutable_expr1()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(rg.nextRandomUInt64() % 10000);
                }
                else
                {
                    generateExpression(rg, gsf->mutable_expr1());
                }
                limit = gsf->mutable_expr2();
                if (noption >= 8)
                {
                    //3 args
                    if (rg.nextBool())
                    {
                        gsf->mutable_expr3()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(rg.nextRandomUInt64() % 10000);
                    }
                    else
                    {
                        generateExpression(rg, gsf->mutable_expr3());
                    }
                }
            }
        }
        else
        {
            //2 args
            if (rg.nextBool())
            {
                gsf->mutable_expr1()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(rg.nextRandomUInt64() % 10000);
            }
            else
            {
                generateExpression(rg, gsf->mutable_expr1());
            }
            limit = gsf->mutable_expr2();
            if (noption >= 6)
            {
                //3 args
                if (rg.nextBool())
                {
                    gsf->mutable_expr3()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(rg.nextRandomUInt64() % 10000);
                }
                else
                {
                    generateExpression(rg, gsf->mutable_expr3());
                }
            }
        }
        for (const auto & entry : levels_backup)
        {
            this->levels[entry.first] = std::move(entry.second);
        }

        limit->mutable_lit_val()->mutable_int_lit()->set_uint_lit(rg.nextRandomUInt64() % 10000);
        rel.cols.push_back(SQLRelationCol(name, cname, std::nullopt));

        jtf->mutable_table_alias()->set_table(name);
        this->levels[this->current_level].rels.push_back(std::move(rel));
    }
    else
    {
        assert(0);
    }
    return 0;
}

int StatementGenerator::addJoinClause(RandomGenerator & rg, BinaryExpr * bexpr)
{
    const SQLRelation *rel1 = &rg.pickRandomlyFromVector(this->levels[this->current_level].rels),
                      *rel2 = &this->levels[this->current_level].rels.back();

    if (rel1->name == rel2->name)
    {
        rel1 = &this->levels[this->current_level].rels[this->levels[this->current_level].rels.size() - 2];
    }
    if (rg.nextSmallNumber() < 4)
    {
        //swap
        const SQLRelation * rel3 = rel1;
        rel1 = rel2;
        rel2 = rel3;
    }
    bexpr->set_op(
        rg.nextSmallNumber() < 9
            ? BinaryOperator::BINOP_EQ
            : static_cast<BinaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(BinaryOperator::BINOP_LEGR)) + 1));
    const SQLRelationCol &col1 = rg.pickRandomlyFromVector(rel1->cols), &col2 = rg.pickRandomlyFromVector(rel2->cols);
    Expr *expr1 = bexpr->mutable_lhs(), *expr2 = bexpr->mutable_rhs();
    ExprSchemaTableColumn *estc1 = expr1->mutable_comp_expr()->mutable_expr_stc(), *estc2 = expr2->mutable_comp_expr()->mutable_expr_stc();
    ExprColumn *ecol1 = estc1->mutable_col(), *ecol2 = estc2->mutable_col();

    if (!rel1->name.empty())
    {
        estc1->mutable_table()->set_table(rel1->name);
    }
    if (!rel2->name.empty())
    {
        estc2->mutable_table()->set_table(rel2->name);
    }
    ecol1->mutable_col()->set_column(col1.name);
    if (col1.name2.has_value())
    {
        ecol1->mutable_subcol()->set_column(col1.name2.value());
    }
    ecol2->mutable_col()->set_column(col2.name);
    if (col2.name2.has_value())
    {
        ecol2->mutable_subcol()->set_column(col2.name2.value());
    }
    addFieldAccess(rg, expr1, 16);
    addFieldAccess(rg, expr2, 16);
    addColNestedAccess(rg, ecol1, 31);
    addColNestedAccess(rg, ecol2, 31);
    return 0;
}

int StatementGenerator::generateJoinConstraint(RandomGenerator & rg, const bool allow_using, JoinConstraint * jc)
{
    if (rg.nextSmallNumber() < 9)
    {
        bool generated = false;

        if (allow_using && rg.nextSmallNumber() < 3)
        {
            //using clause
            const SQLRelation &rel1 = rg.pickRandomlyFromVector(this->levels[this->current_level].rels),
                              &rel2 = this->levels[this->current_level].rels.back();
            std::vector<std::string> cols1, cols2, intersect;

            for (const auto & entry : rel1.cols)
            {
                cols1.push_back(entry.name);
            }
            for (const auto & entry : rel2.cols)
            {
                cols2.push_back(entry.name);
            }
            std::set_intersection(cols1.begin(), cols1.end(), cols2.begin(), cols2.end(), std::back_inserter(intersect));

            if (!intersect.empty())
            {
                ExprColumnList * ecl = jc->mutable_using_expr()->mutable_col_list();
                const uint32_t nclauses
                    = std::min<uint32_t>(UINT32_C(3), (rg.nextRandomUInt32() % static_cast<uint32_t>(intersect.size())) + 1);

                std::shuffle(intersect.begin(), intersect.end(), rg.generator);
                for (uint32_t i = 0; i < nclauses; i++)
                {
                    ExprColumn * ec = i == 0 ? ecl->mutable_col() : ecl->add_extra_cols();

                    ec->mutable_col()->set_column(intersect[i]);
                }
                generated = true;
            }
        }
        if (!generated)
        {
            //joining clause
            const uint32_t nclauses = std::min(this->fc.max_width - this->width, rg.nextSmallNumber() % 3) + UINT32_C(1);
            BinaryExpr * bexpr = jc->mutable_on_expr()->mutable_comp_expr()->mutable_binary_expr();

            for (uint32_t i = 0; i < nclauses; i++)
            {
                if (i == nclauses - 1)
                {
                    addJoinClause(rg, bexpr);
                }
                else
                {
                    addJoinClause(rg, bexpr->mutable_lhs()->mutable_comp_expr()->mutable_binary_expr());
                    bexpr->set_op(rg.nextSmallNumber() < 9 ? BinaryOperator::BINOP_AND : BinaryOperator::BINOP_OR);
                    bexpr = bexpr->mutable_rhs()->mutable_comp_expr()->mutable_binary_expr();
                }
            }
        }
    }
    else
    {
        //random clause
        const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates,
                   prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
        generateExpression(rg, jc->mutable_on_expr());
        this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
        this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
    }
    return 0;
}

int StatementGenerator::addWhereSide(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr)
{
    if (rg.nextSmallNumber() < 3)
    {
        refColumn(rg, rg.pickRandomlyFromVector(available_cols), expr);
    }
    else
    {
        generateLiteralValue(rg, expr);
    }
    return 0;
}

int StatementGenerator::addWhereFilter(RandomGenerator & rg, const std::vector<GroupCol> & available_cols, Expr * expr)
{
    const GroupCol & gcol = rg.pickRandomlyFromVector(available_cols);
    const uint32_t noption = rg.nextLargeNumber();

    if (noption < 761)
    {
        //binary expr
        BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();
        Expr *lexpr = bexpr->mutable_lhs(), *rexpr = bexpr->mutable_rhs();

        if (rg.nextSmallNumber() < 9)
        {
            refColumn(rg, gcol, lexpr);
            addWhereSide(rg, available_cols, rexpr);
        }
        else
        {
            addWhereSide(rg, available_cols, lexpr);
            refColumn(rg, gcol, rexpr);
        }
        bexpr->set_op(
            rg.nextSmallNumber() < 7
                ? BinaryOperator::BINOP_EQ
                : static_cast<BinaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(BinaryOperator::BINOP_LEGR)) + 1));
    }
    else if (noption < 901)
    {
        //between expr
        const uint32_t noption2 = rg.nextMediumNumber();
        ExprBetween * bexpr = expr->mutable_comp_expr()->mutable_expr_between();
        Expr *expr1 = bexpr->mutable_expr1(), *expr2 = bexpr->mutable_expr2(), *expr3 = bexpr->mutable_expr3();

        bexpr->set_not_(rg.nextBool());
        if (noption2 < 34)
        {
            refColumn(rg, gcol, expr1);
            addWhereSide(rg, available_cols, expr2);
            addWhereSide(rg, available_cols, expr3);
        }
        else if (noption2 < 68)
        {
            addWhereSide(rg, available_cols, expr1);
            refColumn(rg, gcol, expr2);
            addWhereSide(rg, available_cols, expr3);
        }
        else
        {
            addWhereSide(rg, available_cols, expr1);
            addWhereSide(rg, available_cols, expr2);
            refColumn(rg, gcol, expr3);
        }
    }
    else if (noption < 971)
    {
        //is null expr
        ExprNullTests * enull = expr->mutable_comp_expr()->mutable_expr_null_tests();

        enull->set_not_(rg.nextBool());
        refColumn(rg, gcol, enull->mutable_expr());
    }
    else if (noption < 981)
    {
        //like expr
        ExprLike * elike = expr->mutable_comp_expr()->mutable_expr_like();
        Expr * expr2 = elike->mutable_expr2();

        elike->set_not_(rg.nextBool());
        elike->set_keyword(
            static_cast<ExprLike_PossibleKeywords>((rg.nextRandomUInt32() % static_cast<uint32_t>(ExprLike::PossibleKeywords_MAX)) + 1));
        refColumn(rg, gcol, elike->mutable_expr1());
        if (rg.nextSmallNumber() < 5)
        {
            buf.resize(0);
            rg.nextString(buf, "'", true, (rg.nextRandomUInt32() % 10000) + 1);
            expr2->mutable_lit_val()->set_no_quote_str(buf);
        }
        else
        {
            addWhereSide(rg, available_cols, expr2);
        }
    }
    else if (noption < 991)
    {
        //in expr
        const uint32_t nclauses = rg.nextSmallNumber();
        ExprIn * ein = expr->mutable_comp_expr()->mutable_expr_in();
        ExprList * elist = ein->mutable_exprs();

        ein->set_not_(rg.nextBool());
        ein->set_global(rg.nextBool());
        refColumn(rg, gcol, ein->mutable_expr()->mutable_expr());
        for (uint32_t i = 0; i < nclauses; i++)
        {
            addWhereSide(rg, available_cols, elist->mutable_expr());
        }
    }
    else
    {
        //any predicate
        generatePredicate(rg, expr);
    }
    return 0;
}

int StatementGenerator::generateWherePredicate(RandomGenerator & rg, Expr * expr)
{
    std::vector<GroupCol> available_cols;
    const uint32_t noption = rg.nextSmallNumber();

    if (this->levels[this->current_level].gcols.empty() && !this->levels[this->current_level].global_aggregate)
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

    this->depth++;
    if (!available_cols.empty() && noption < 8)
    {
        const uint32_t nclauses = std::max(std::min(this->fc.max_width - this->width, (rg.nextSmallNumber() % 4) + 1), UINT32_C(1));

        for (uint32_t i = 0; i < nclauses; i++)
        {
            this->width++;
            if (i == nclauses - 1)
            {
                addWhereFilter(rg, available_cols, expr);
            }
            else
            {
                BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

                addWhereFilter(rg, available_cols, bexpr->mutable_lhs());
                bexpr->set_op(rg.nextSmallNumber() < 8 ? BinaryOperator::BINOP_AND : BinaryOperator::BINOP_OR);
                expr = bexpr->mutable_rhs();
            }
        }
        this->width -= nclauses;
    }
    else if (noption < 10)
    {
        //predicate
        generatePredicate(rg, expr);
    }
    else
    {
        //random clause
        generateExpression(rg, expr);
    }
    this->depth--;
    return 0;
}

int StatementGenerator::generateFromStatement(RandomGenerator & rg, const uint32_t allowed_clauses, FromStatement * ft)
{
    JoinClause * jc = ft->mutable_tos()->mutable_join_clause();
    const uint32_t njoined = std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % UINT32_C(4)) + 1);

    this->depth++;
    this->width++;
    generateFromElement(rg, allowed_clauses, jc->mutable_tos());
    for (uint32_t i = 1; i < njoined; i++)
    {
        JoinClauseCore * jcc = jc->add_clauses();

        this->depth++;
        this->width++;
        if (rg.nextSmallNumber() < 3)
        {
            generateArrayJoin(rg, jcc->mutable_arr());
        }
        else
        {
            JoinCore * core = jcc->mutable_core();
            JoinType jt = static_cast<JoinType>((rg.nextRandomUInt32() % static_cast<uint32_t>(JoinType_MAX)) + 1);

            if (!this->allow_not_deterministic && jt == JoinType::J_PASTE)
            {
                jt = JoinType::J_INNER;
            }
            core->set_global(rg.nextSmallNumber() < 3);
            core->set_join_op(jt);
            if (rg.nextSmallNumber() < 4)
            {
                switch (jt)
                {
                    case JoinType::J_LEFT:
                    case JoinType::J_INNER:
                        core->set_join_const(static_cast<JoinConst>((rg.nextRandomUInt32() % static_cast<uint32_t>(JoinConst_MAX)) + 1));
                        break;
                    case JoinType::J_RIGHT:
                        core->set_join_const(
                            static_cast<JoinConst>((rg.nextRandomUInt32() % static_cast<uint32_t>(JoinConst::J_ANTI)) + 1));
                        break;
                    case JoinType::J_FULL:
                        core->set_join_const(JoinConst::J_ALL);
                        break;
                    default:
                        break;
                }
            }
            generateFromElement(rg, allowed_clauses, core->mutable_tos());
            generateJoinConstraint(rg, njoined == 2, core->mutable_join_constraint());
        }
    }
    this->width -= njoined;
    this->depth -= njoined;
    return 0;
}

int StatementGenerator::generateGroupByExpr(
    RandomGenerator & rg,
    const bool enforce_having,
    const uint32_t offset,
    const uint32_t ncols,
    const std::vector<SQLRelationCol> & available_cols,
    std::vector<GroupCol> & gcols,
    Expr * expr)
{
    const uint32_t next_option = rg.nextSmallNumber();

    if (!available_cols.empty() && (enforce_having || next_option < 9))
    {
        const SQLRelationCol & rel_col = available_cols[offset];
        ExprSchemaTableColumn * estc = expr->mutable_comp_expr()->mutable_expr_stc();
        ExprColumn * ecol = estc->mutable_col();

        if (!rel_col.rel_name.empty())
        {
            estc->mutable_table()->set_table(rel_col.rel_name);
        }
        ecol->mutable_col()->set_column(rel_col.name);
        if (rel_col.name2.has_value())
        {
            ecol->mutable_subcol()->set_column(rel_col.name2.value());
        }
        addFieldAccess(rg, expr, 16);
        addColNestedAccess(rg, ecol, 31);
        gcols.push_back(GroupCol(rel_col, expr));
    }
    else if (next_option < 10)
    {
        LiteralValue * lv = expr->mutable_lit_val();

        lv->mutable_int_lit()->set_uint_lit((rg.nextRandomUInt64() % ncols) + 1);
    }
    else
    {
        generateExpression(rg, expr);
    }
    return 0;
}

int StatementGenerator::generateGroupBy(
    RandomGenerator & rg, const uint32_t ncols, const bool enforce_having, const bool allow_settings, GroupByStatement * gbs)
{
    std::vector<SQLRelationCol> available_cols;

    if (!this->levels[this->current_level].rels.empty())
    {
        for (const auto & entry : this->levels[this->current_level].rels)
        {
            available_cols.insert(available_cols.end(), entry.cols.begin(), entry.cols.end());
        }
        std::shuffle(available_cols.begin(), available_cols.end(), rg.generator);
    }
    if (enforce_having && available_cols.empty())
    {
        return 0;
    }
    this->depth++;
    if (enforce_having || rg.nextSmallNumber() < (available_cols.empty() ? 3 : 9))
    {
        std::vector<GroupCol> gcols;
        const uint32_t next_opt = rg.nextMediumNumber();
        GroupByList * gbl = gbs->mutable_glist();
        const uint32_t nclauses = std::min<uint32_t>(
            this->fc.max_width - this->width,
            std::min<uint32_t>(
                UINT32_C(5), (rg.nextRandomUInt32() % (available_cols.empty() ? 5 : static_cast<uint32_t>(available_cols.size()))) + 1));
        const bool no_grouping_sets = next_opt < 91 || !allow_settings,
                   has_gsm = !enforce_having && next_opt < 51 && allow_settings && rg.nextSmallNumber() < 4,
                   has_totals = !enforce_having && no_grouping_sets && allow_settings && rg.nextSmallNumber() < 4;

        if (no_grouping_sets)
        {
            //group list
            ExprList * elist = (!allow_settings || next_opt < 51) ? gbl->mutable_exprs()
                                                                  : ((next_opt < 71) ? gbl->mutable_rollup() : gbl->mutable_cube());

            for (uint32_t i = 0; i < nclauses; i++)
            {
                this->width++;
                generateGroupByExpr(
                    rg, enforce_having, i, ncols, available_cols, gcols, i == 0 ? elist->mutable_expr() : elist->add_extra_exprs());
            }
        }
        else
        {
            //grouping sets
            bool has_global = false;
            GroupingSets * gsets = gbl->mutable_sets();

            for (uint32_t i = 0; i < nclauses; i++)
            {
                const uint32_t nelems = rg.nextRandomUInt32() % (available_cols.empty() ? 3 : static_cast<uint32_t>(available_cols.size()));
                OptionalExprList * oel = i == 0 ? gsets->mutable_exprs() : gsets->add_other_exprs();

                has_global |= nelems == 0;
                this->width++;
                for (uint32_t j = 0; j < nelems; j++)
                {
                    this->width++;
                    generateGroupByExpr(rg, enforce_having, j, ncols, available_cols, gcols, oel->add_exprs());
                }
                this->width -= nelems;
                std::shuffle(available_cols.begin(), available_cols.end(), rg.generator);
            }
            this->levels[this->current_level].global_aggregate |= gcols.empty() && has_global;
        }
        this->width -= nclauses;
        this->levels[this->current_level].gcols = std::move(gcols);

        if (has_gsm)
        {
            gbl->set_gsm(static_cast<GroupByList_GroupingSetsModifier>(
                (rg.nextRandomUInt32() % static_cast<uint32_t>(GroupByList::GroupingSetsModifier_MAX)) + 1));
        }
        gbl->set_with_totals(has_totals);

        if (!has_gsm && !has_totals && allow_settings && (enforce_having || rg.nextSmallNumber() < 5))
        {
            const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates;

            this->levels[this->current_level].allow_aggregates = true;
            generateWherePredicate(rg, gbs->mutable_having_expr());
            this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
        }
    }
    else
    {
        gbs->set_gall(true);
        this->levels[this->current_level].group_by_all = true;
    }
    this->depth--;
    return 1;
}

int StatementGenerator::generateOrderBy(RandomGenerator & rg, const uint32_t ncols, const bool allow_settings, OrderByStatement * ob)
{
    if (rg.nextSmallNumber() < 3)
    {
        ob->set_oall(true);
    }
    else
    {
        bool has_fill = false;
        std::vector<GroupCol> available_cols;
        OrderByList * olist = ob->mutable_olist();

        if (this->levels[this->current_level].group_by_all)
        {
            for (const auto & entry : this->levels[this->current_level].projections)
            {
                const std::string cname = "c" + std::to_string(entry);
                available_cols.push_back(GroupCol(SQLRelationCol("", std::move(cname), std::nullopt), nullptr));
            }
        }
        else if (this->levels[this->current_level].gcols.empty() && !this->levels[this->current_level].global_aggregate)
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
        if (!available_cols.empty())
        {
            std::shuffle(available_cols.begin(), available_cols.end(), rg.generator);
        }
        const uint32_t nclauses = std::min<uint32_t>(
            this->fc.max_width - this->width,
            std::min<uint32_t>(
                UINT32_C(5), (rg.nextRandomUInt32() % (available_cols.empty() ? 5 : static_cast<uint32_t>(available_cols.size()))) + 1));

        for (uint32_t i = 0; i < nclauses; i++)
        {
            ExprOrderingTerm * eot = i == 0 ? olist->mutable_ord_term() : olist->add_extra_ord_terms();
            Expr * expr = eot->mutable_expr();
            const uint32_t next_option = rg.nextSmallNumber();

            this->width++;
            if (!available_cols.empty() && next_option < 9)
            {
                refColumn(rg, available_cols[i], expr);
            }
            else if (ncols && next_option < 10)
            {
                LiteralValue * lv = expr->mutable_lit_val();

                lv->mutable_int_lit()->set_uint_lit((rg.nextRandomUInt64() % ncols) + 1);
            }
            else
            {
                generateExpression(rg, expr);
            }
            if (allow_settings)
            {
                if (rg.nextSmallNumber() < 7)
                {
                    eot->set_asc_desc(
                        rg.nextBool() ? ExprOrderingTerm_AscDesc::ExprOrderingTerm_AscDesc_ASC
                                      : ExprOrderingTerm_AscDesc::ExprOrderingTerm_AscDesc_DESC);
                }
                if (rg.nextSmallNumber() < 7)
                {
                    eot->set_nulls_order(
                        rg.nextBool() ? ExprOrderingTerm_NullsOrder::ExprOrderingTerm_NullsOrder_FIRST
                                      : ExprOrderingTerm_NullsOrder::ExprOrderingTerm_NullsOrder_LAST);
                }
                if (!this->fc.collations.empty() && rg.nextSmallNumber() < 3)
                {
                    eot->set_collation(rg.pickRandomlyFromVector(this->fc.collations));
                }
                if (rg.nextSmallNumber() < 2)
                {
                    ExprOrderingWithFill * eowf = eot->mutable_fill();

                    has_fill = true;
                    if (rg.nextSmallNumber() < 4)
                    {
                        generateExpression(rg, eowf->mutable_from_expr());
                    }
                    if (rg.nextSmallNumber() < 4)
                    {
                        generateExpression(rg, eowf->mutable_to_expr());
                    }
                    if (rg.nextSmallNumber() < 4)
                    {
                        generateExpression(rg, eowf->mutable_step_expr());
                    }
                }
            }
        }
        this->width -= nclauses;
        if (has_fill && !this->levels[this->current_level].projections.empty()
            && (this->levels[this->current_level].group_by_all
                || (this->levels[this->current_level].gcols.empty() && !this->levels[this->current_level].global_aggregate))
            && rg.nextSmallNumber() < 4)
        {
            std::vector<uint32_t> nids;
            const uint32_t iclauses = std::min<uint32_t>(
                this->fc.max_width - this->width,
                std::min<uint32_t>(
                    UINT32_C(3),
                    (rg.nextRandomUInt32() % static_cast<uint32_t>(this->levels[this->current_level].projections.size())) + 1));

            nids.insert(
                nids.end(), this->levels[this->current_level].projections.begin(), this->levels[this->current_level].projections.end());
            std::shuffle(nids.begin(), nids.end(), rg.generator);
            for (uint32_t i = 0; i < iclauses; i++)
            {
                InterpolateExpr * ie = olist->add_interpolate();

                ie->mutable_col()->set_column("c" + std::to_string(nids[i]));
                generateExpression(rg, ie->mutable_expr());
                this->width++;
            }
            this->width -= iclauses;
        }
    }
    return 0;
}

int StatementGenerator::generateLimitExpr(RandomGenerator & rg, Expr * expr)
{
    if (this->depth >= this->fc.max_depth || rg.nextSmallNumber() < 8)
    {
        uint32_t nlimit = 0;
        const int next_option = rg.nextSmallNumber();

        if (next_option < 3)
        {
            nlimit = 0;
        }
        else if (next_option < 5)
        {
            nlimit = 1;
        }
        else if (next_option < 7)
        {
            nlimit = 2;
        }
        else if (next_option < 9)
        {
            nlimit = 10;
        }
        else
        {
            nlimit = rg.nextRandomUInt32();
        }
        expr->mutable_lit_val()->mutable_int_lit()->set_uint_lit(nlimit);
    }
    else
    {
        this->depth++;
        generateExpression(rg, expr);
        this->depth--;
    }
    return 0;
}

int StatementGenerator::generateLimit(RandomGenerator & rg, const bool has_order_by, const uint32_t ncols, LimitStatement * ls)
{
    generateLimitExpr(rg, ls->mutable_limit());
    if (rg.nextBool())
    {
        generateLimitExpr(rg, ls->mutable_offset());
    }
    ls->set_with_ties(has_order_by && (!this->allow_not_deterministic || rg.nextSmallNumber() < 7));
    if (rg.nextSmallNumber() < 4)
    {
        Expr * expr = ls->mutable_limit_by();

        if (this->depth >= this->fc.max_depth || rg.nextSmallNumber() < 8)
        {
            LiteralValue * lv = expr->mutable_lit_val();

            lv->mutable_int_lit()->set_uint_lit((rg.nextRandomUInt64() % ncols) + 1);
        }
        else
        {
            this->depth++;
            generateExpression(rg, expr);
            this->depth--;
        }
    }
    return 0;
}

int StatementGenerator::generateOffset(RandomGenerator & rg, OffsetStatement * off)
{
    generateLimitExpr(rg, off->mutable_row_count());
    off->set_rows(rg.nextBool());
    if (!this->allow_not_deterministic || rg.nextBool())
    {
        FetchStatement * fst = off->mutable_fetch();

        generateLimitExpr(rg, fst->mutable_row_count());
        fst->set_rows(rg.nextBool());
        fst->set_first(rg.nextBool());
        fst->set_only(!this->allow_not_deterministic || rg.nextBool());
    }
    return 0;
}

int StatementGenerator::addCTEs(RandomGenerator & rg, const uint32_t allowed_clauses, CTEs * qctes)
{
    const uint32_t nclauses = std::min<uint32_t>(this->fc.max_width - this->width, (rg.nextRandomUInt32() % 3) + 1);

    this->depth++;
    for (uint32_t i = 0; i < nclauses; i++)
    {
        CTEquery * cte = i == 0 ? qctes->mutable_cte() : qctes->add_other_ctes();
        std::string name;

        name += "cte";
        name += std::to_string(i);
        name += "d";
        name += std::to_string(this->current_level);
        SQLRelation rel(name);

        cte->mutable_table()->set_table(name);
        generateDerivedTable(rg, rel, allowed_clauses, cte->mutable_query());
        this->ctes[this->current_level][name] = std::move(rel);
        this->width++;
    }
    this->width -= nclauses;
    this->depth--;
    return 0;
}

int StatementGenerator::generateSelect(
    RandomGenerator & rg, const bool top, const uint32_t ncols, const uint32_t allowed_clauses, Select * sel)
{
    int res = 0;

    if ((allowed_clauses & allow_cte) && this->depth < this->fc.max_depth && this->width < this->fc.max_width && rg.nextMediumNumber() < 13)
    {
        this->addCTEs(rg, allowed_clauses, sel->mutable_ctes());
    }
    if ((allowed_clauses & allow_set) && this->depth<this->fc.max_depth && this->fc.max_width> this->width + 1 && rg.nextSmallNumber() < 3)
    {
        SetQuery * setq = sel->mutable_set_query();

        setq->set_set_op(static_cast<SetQuery_SetOp>((rg.nextRandomUInt32() % static_cast<uint32_t>(SetQuery::SetOp_MAX)) + 1));
        setq->set_s_or_d(rg.nextBool() ? AllOrDistinct::ALL : AllOrDistinct::DISTINCT);

        this->depth++;
        this->current_level++;
        this->levels[this->current_level] = QueryLevel(this->current_level);
        res = std::max<int>(res, generateSelect(rg, false, ncols, allowed_clauses, setq->mutable_sel1()));
        this->width++;
        this->levels[this->current_level] = QueryLevel(this->current_level);
        res = std::max<int>(res, generateSelect(rg, false, ncols, allowed_clauses, setq->mutable_sel2()));
        this->current_level--;
        this->depth--;
        this->width--;
    }
    else
    {
        SelectStatementCore * ssc = sel->mutable_select_core();

        if ((allowed_clauses & allow_distinct) && rg.nextSmallNumber() < 3)
        {
            ssc->set_s_or_d(rg.nextBool() ? AllOrDistinct::ALL : AllOrDistinct::DISTINCT);
        }
        if ((allowed_clauses & allow_from) && this->depth < this->fc.max_depth && this->width < this->fc.max_width
            && rg.nextSmallNumber() < 10)
        {
            res = std::max<int>(res, generateFromStatement(rg, allowed_clauses, ssc->mutable_from()));
        }
        const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates,
                   prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
        if ((allowed_clauses & allow_prewhere) && this->depth < this->fc.max_depth && ssc->has_from() && rg.nextSmallNumber() < 2)
        {
            generateWherePredicate(rg, ssc->mutable_pre_where()->mutable_expr()->mutable_expr());
        }
        if ((allowed_clauses & allow_where) && this->depth < this->fc.max_depth && rg.nextSmallNumber() < 5)
        {
            generateWherePredicate(rg, ssc->mutable_where()->mutable_expr()->mutable_expr());
        }

        if ((allowed_clauses & allow_groupby) && this->depth < this->fc.max_depth && this->width < this->fc.max_width
            && rg.nextSmallNumber() < 4)
        {
            generateGroupBy(rg, ncols, false, (allowed_clauses & allow_groupby_settings), ssc->mutable_groupby());
        }
        else
        {
            this->levels[this->current_level].global_aggregate = rg.nextSmallNumber() < 4;
        }
        this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
        this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;

        this->depth++;
        for (uint32_t i = 0; i < ncols; i++)
        {
            ExprColAlias * eca = ssc->add_result_columns()->mutable_eca();

            this->width++;
            generateExpression(rg, eca->mutable_expr());
            if (!top)
            {
                const uint32_t cname = this->levels[this->current_level].aliases_counter++;
                const std::string cname_str = "c" + std::to_string(cname);

                SQLRelation rel("");
                rel.cols.push_back(SQLRelationCol("", cname_str, std::nullopt));
                this->levels[this->current_level].rels.push_back(std::move(rel));
                eca->mutable_col_alias()->set_column(cname_str);
                this->levels[this->current_level].projections.push_back(cname);
            }
        }
        this->depth--;
        this->width -= ncols;

        if ((allowed_clauses & allow_orderby) && this->depth < this->fc.max_depth && this->width < this->fc.max_width
            && (!this->allow_not_deterministic || rg.nextSmallNumber() < 4))
        {
            this->depth++;
            generateOrderBy(rg, ncols, (allowed_clauses & allow_orderby_settings), ssc->mutable_orderby());
            this->depth--;
        }
        if ((allowed_clauses & allow_limit) && (this->allow_not_deterministic || ssc->has_orderby()) && rg.nextSmallNumber() < 4)
        {
            if (rg.nextBool())
            {
                generateLimit(rg, ssc->has_orderby(), ncols, ssc->mutable_limit());
            }
            else
            {
                generateOffset(rg, ssc->mutable_offset());
            }
        }
    }
    this->levels.erase(this->current_level);
    this->ctes.erase(this->current_level);
    return res;
}

int StatementGenerator::generateTopSelect(RandomGenerator & rg, const uint32_t allowed_clauses, TopSelect * ts)
{
    int res = 0;
    const uint32_t ncols = std::max(std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % UINT32_C(5)) + 1), UINT32_C(1));

    assert(this->levels.empty());
    this->levels[this->current_level] = QueryLevel(this->current_level);
    if ((res = generateSelect(rg, true, ncols, allowed_clauses, ts->mutable_sel())))
    {
        return res;
    }
    if (rg.nextSmallNumber() < 3)
    {
        ts->set_format(static_cast<OutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(OutFormat_MAX)) + 1));
    }
    return res;
}

}
